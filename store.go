package gkvlite

// TODO: Concurrent eviction might have a race, where the fix is that
// read()'s should return the nodes that they just read.

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"sync/atomic"
	"unsafe"
)

// A persistable store holding collections of ordered keys & values.
type Store struct {
	coll      unsafe.Pointer // Immutable, read-only map[string]*Collection.
	file      StoreFile
	size      int64
	readOnly  bool
	callbacks StoreCallbacks
}

// The StoreFile interface is implemented by os.File.  Application
// specific implementations may add concurrency, caching, stats, etc.
type StoreFile interface {
	io.ReaderAt
	io.WriterAt
	Stat() (os.FileInfo, error)
}

// Allows applications to interpose before/after certain events.
type StoreCallbacks struct {
	BeforeItemWrite, AfterItemRead ItemCallback
}

type ItemCallback func(*Item) (*Item, error)

const VERSION = uint32(4)

var MAGIC_BEG []byte = []byte("0g1t2r")
var MAGIC_END []byte = []byte("3e4a5p")

// Provide a nil StoreFile for in-memory-only (non-persistent) usage.
func NewStore(file StoreFile) (*Store, error) {
	return NewStoreEx(file, StoreCallbacks{})
}
func NewStoreEx(file StoreFile,
	callbacks StoreCallbacks) (*Store, error) {
	coll := make(map[string]*Collection)
	res := &Store{coll: unsafe.Pointer(&coll), callbacks: callbacks}
	if file == nil || !reflect.ValueOf(file).Elem().IsValid() {
		return res, nil // Memory-only Store.
	}
	res.file = file
	if err := res.readRoots(); err != nil {
		return nil, err
	}
	return res, nil
}

// SetCollection() is used to create a named Collection, or to modify
// the KeyCompare function on an existing, named Collection.  The new
// Collection and any operations on it won't be reflected into
// persistence until you do a Flush().
func (s *Store) SetCollection(name string, compare KeyCompare) *Collection {
	if compare == nil {
		compare = bytes.Compare
	}
	for {
		orig := atomic.LoadPointer(&s.coll)
		coll := copyColl(*(*map[string]*Collection)(orig))
		if coll[name] == nil {
			coll[name] = s.MakePrivateCollection(compare)
		}
		coll[name].compare = compare
		if atomic.CompareAndSwapPointer(&s.coll, orig, unsafe.Pointer(&coll)) {
			return coll[name]
		}
	}
	return nil // Never reached.
}

// Returns a new, unregistered (non-named) collection.  This allows
// advanced users to manage collections of private collections.
func (s *Store) MakePrivateCollection(compare KeyCompare) *Collection {
	if compare == nil {
		compare = bytes.Compare
	}
	return &Collection{
		store:   s,
		compare: compare,
		root:    unsafe.Pointer(&nodeLoc{}),
	}
}

// Retrieves a named Collection.
func (s *Store) GetCollection(name string) *Collection {
	coll := *(*map[string]*Collection)(atomic.LoadPointer(&s.coll))
	return coll[name]
}

func (s *Store) GetCollectionNames() []string {
	return collNames(*(*map[string]*Collection)(atomic.LoadPointer(&s.coll)))
}

func collNames(coll map[string]*Collection) []string {
	res := make([]string, 0, len(coll))
	for name, _ := range coll {
		res = append(res, name)
	}
	sort.Strings(res) // Sorting because common callers need stability.
	return res
}

// The Collection removal won't be reflected into persistence until
// you do a Flush().  Invoking RemoveCollection(x) and then
// SetCollection(x) is a fast way to empty a Collection.
func (s *Store) RemoveCollection(name string) {
	for {
		orig := atomic.LoadPointer(&s.coll)
		coll := copyColl(*(*map[string]*Collection)(orig))
		delete(coll, name)
		if atomic.CompareAndSwapPointer(&s.coll, orig, unsafe.Pointer(&coll)) {
			return
		}
	}
}

func copyColl(orig map[string]*Collection) map[string]*Collection {
	res := make(map[string]*Collection)
	for name, c := range orig {
		res[name] = c
	}
	return res
}

// Writes (appends) any unpersisted data to file.  As a
// greater-window-of-data-loss versus higher-performance tradeoff,
// consider having many mutations (Set()'s & Delete()'s) and then
// have a less occasional Flush() instead of Flush()'ing after every
// mutation.  Users may also wish to file.Sync() after a Flush() for
// extra data-loss protection.
func (s *Store) Flush() error {
	if s.readOnly {
		return errors.New("readonly, so cannot Flush()")
	}
	if s.file == nil {
		return errors.New("no file / in-memory only, so cannot Flush()")
	}
	coll := *(*map[string]*Collection)(atomic.LoadPointer(&s.coll))
	for _, name := range collNames(coll) {
		if err := coll[name].Write(); err != nil {
			return err
		}
	}
	return s.writeRoots()
}

// Returns a non-persistable snapshot, including any mutations that
// have not been Flush()'ed to disk yet.  The snapshot has its Flush()
// disabled because the original store "owns" writes to the StoreFile.
// Caller should ensure that the returned snapshot store and the
// original store are either used in "single-threaded" manner or that
// the underlying StoreFile supports concurrent operations.  On
// isolation: if you make updates to a snapshot store, those updates
// will not be seen by the original store; and vice-versa for
// mutations on the original store.  To persist the snapshot (and any
// updates on it) to a new file, use snapshot.CopyTo().
func (s *Store) Snapshot() (snapshot *Store) {
	coll := copyColl(*(*map[string]*Collection)(atomic.LoadPointer(&s.coll)))
	res := &Store{
		coll:      unsafe.Pointer(&coll),
		file:      s.file,
		size:      atomic.LoadInt64(&s.size),
		readOnly:  true,
		callbacks: s.callbacks,
	}
	for _, name := range collNames(coll) {
		coll[name] = &Collection{
			store:   res,
			compare: coll[name].compare,
			root:    unsafe.Pointer(atomic.LoadPointer(&coll[name].root)),
		}
	}
	return res
}

// Copy all active collections and their items to a different file.
// If flushEvery > 0, then during the item copying, Flush() will be
// invoked at every flushEvery'th item and at the end of the item
// copying.  The copy will not include any old items or nodes so the
// copy should be more compact if flushEvery is relatively large.
func (s *Store) CopyTo(dstFile StoreFile, flushEvery int) (res *Store, err error) {
	dstStore, err := NewStore(dstFile)
	if err != nil {
		return nil, err
	}
	coll := *(*map[string]*Collection)(atomic.LoadPointer(&s.coll))
	for _, name := range collNames(coll) {
		srcColl := coll[name]
		dstColl := dstStore.SetCollection(name, srcColl.compare)
		minItem, err := srcColl.MinItem(true)
		if err != nil {
			return nil, err
		}
		if minItem == nil {
			continue
		}
		numItems := 0
		var errCopyItem error = nil
		err = srcColl.VisitItemsAscend(minItem.Key, true, func(i *Item) bool {
			if errCopyItem = dstColl.SetItem(i); errCopyItem != nil {
				return false
			}
			numItems++
			if flushEvery > 0 && numItems%flushEvery == 0 {
				if errCopyItem = dstStore.Flush(); errCopyItem != nil {
					return false
				}
			}
			return true
		})
		if err != nil {
			return nil, err
		}
		if errCopyItem != nil {
			return nil, errCopyItem
		}
	}
	if flushEvery > 0 {
		if err = dstStore.Flush(); err != nil {
			return nil, err
		}
	}
	return dstStore, nil
}

// User-supplied key comparison func should return 0 if a == b,
// -1 if a < b, and +1 if a > b.  For example: bytes.Compare()
type KeyCompare func(a, b []byte) int

// A persistable collection of ordered key-values (Item's).
type Collection struct {
	store   *Store
	compare KeyCompare
	root    unsafe.Pointer // Value is *nodeLoc type.
}

// A persistable node.
type node struct {
	item               itemLoc
	left, right        nodeLoc
	numNodes, numBytes uint64
}

// A persistable node and its persistence location.
type nodeLoc struct {
	loc  unsafe.Pointer // *ploc - can be nil if node is dirty (not yet persisted).
	node unsafe.Pointer // *node - can be nil if node is not fetched into memory yet.
}

var empty = &nodeLoc{} // Sentinel.

func (nloc *nodeLoc) Loc() *ploc {
	return (*ploc)(atomic.LoadPointer(&nloc.loc))
}

func (nloc *nodeLoc) Node() *node {
	return (*node)(atomic.LoadPointer(&nloc.node))
}

func (nloc *nodeLoc) isEmpty() bool {
	return nloc == nil || (nloc.Loc().isEmpty() && nloc.Node() == nil)
}

func (nloc *nodeLoc) write(o *Store) error {
	if nloc != nil && nloc.Loc().isEmpty() {
		node := nloc.Node()
		if node == nil {
			return nil
		}
		offset := atomic.LoadInt64(&o.size)
		length := ploc_length + ploc_length + ploc_length + 8 + 8
		b := bytes.NewBuffer(make([]byte, 0, length))
		if err := node.item.Loc().write(b); err != nil {
			return err
		}
		if err := node.left.Loc().write(b); err != nil {
			return err
		}
		if err := node.right.Loc().write(b); err != nil {
			return err
		}
		if err := binary.Write(b, binary.BigEndian, node.numNodes); err != nil {
			return err
		}
		if err := binary.Write(b, binary.BigEndian, node.numBytes); err != nil {
			return err
		}
		if _, err := o.file.WriteAt(b.Bytes()[:length], offset); err != nil {
			return err
		}
		atomic.StoreInt64(&o.size, offset+int64(length))
		atomic.StorePointer(&nloc.loc,
			unsafe.Pointer(&ploc{Offset: offset, Length: uint32(length)}))
	}
	return nil
}

func (nloc *nodeLoc) read(o *Store) (err error) {
	if nloc != nil && nloc.Node() == nil {
		loc := nloc.Loc()
		if loc.isEmpty() {
			return nil
		}
		b := make([]byte, loc.Length)
		if _, err := o.file.ReadAt(b, loc.Offset); err != nil {
			return err
		}
		buf := bytes.NewBuffer(b)
		n := &node{}
		p := &ploc{}
		if p, err = p.read(buf); err != nil {
			return err
		}
		atomic.StorePointer(&n.item.loc, unsafe.Pointer(p))
		p = &ploc{}
		if p, err = p.read(buf); err != nil {
			return err
		}
		atomic.StorePointer(&n.left.loc, unsafe.Pointer(p))
		p = &ploc{}
		if p, err = p.read(buf); err != nil {
			return err
		}
		atomic.StorePointer(&n.right.loc, unsafe.Pointer(p))
		var num uint64
		if err = binary.Read(buf, binary.BigEndian, &num); err != nil {
			return err
		}
		n.numNodes = num
		if err = binary.Read(buf, binary.BigEndian, &num); err != nil {
			return err
		}
		n.numBytes = num
		atomic.StorePointer(&nloc.node, unsafe.Pointer(n))
	}
	return nil
}

func numInfo(o *Store, left *nodeLoc, right *nodeLoc) (
	leftNum uint64, leftBytes uint64, rightNum uint64, rightBytes uint64, err error) {
	if err = left.read(o); err != nil {
		return 0, 0, 0, 0, err
	}
	if err = right.read(o); err != nil {
		return 0, 0, 0, 0, err
	}
	if !left.isEmpty() {
		leftNum = left.Node().numNodes
		leftBytes = left.Node().numBytes
	}
	if !right.isEmpty() {
		rightNum = right.Node().numNodes
		rightBytes = right.Node().numBytes
	}
	return leftNum, leftBytes, rightNum, rightBytes, nil
}

// A persistable item.
type Item struct {
	Key, Val  []byte         // Val may be nil if not fetched into memory yet.
	Priority  int32          // Use rand.Int() for probabilistic balancing.
	Transient unsafe.Pointer // For any ephemeral data; atomic CAS recommended.
}

// A persistable item and its persistence location.
type itemLoc struct {
	loc  unsafe.Pointer // *ploc - can be nil if item is dirty (not yet persisted).
	item unsafe.Pointer // *Item - can be nil if item is not fetched into memory yet.
}

func (i *itemLoc) Loc() *ploc {
	return (*ploc)(atomic.LoadPointer(&i.loc))
}

func (i *itemLoc) Item() *Item {
	return (*Item)(atomic.LoadPointer(&i.item))
}

func (i *itemLoc) write(o *Store) (err error) {
	if i.Loc().isEmpty() {
		iItem := i.Item()
		if iItem == nil {
			return errors.New("itemLoc.write with nil item")
		}
		if o.callbacks.BeforeItemWrite != nil {
			iItem, err = o.callbacks.BeforeItemWrite(iItem)
			if err != nil {
				return err
			}
		}
		offset := atomic.LoadInt64(&o.size)
		length := 4 + 2 + 4 + 4 + len(iItem.Key) + len(iItem.Val)
		b := bytes.NewBuffer(make([]byte, length)[:0])
		binary.Write(b, binary.BigEndian, uint32(length))
		binary.Write(b, binary.BigEndian, uint16(len(iItem.Key)))
		binary.Write(b, binary.BigEndian, uint32(len(iItem.Val)))
		binary.Write(b, binary.BigEndian, int32(iItem.Priority))
		b.Write(iItem.Key)
		b.Write(iItem.Val) // TODO: handle large values more efficiently.
		if _, err := o.file.WriteAt(b.Bytes()[:length], offset); err != nil {
			return err
		}
		atomic.StoreInt64(&o.size, offset+int64(length))
		atomic.StorePointer(&i.loc,
			unsafe.Pointer(&ploc{Offset: offset, Length: uint32(length)}))
	}
	return nil
}

func (i *itemLoc) read(o *Store, withValue bool) (err error) {
	if i == nil {
		return nil
	}
	iItem := i.Item()
	if iItem == nil || (iItem.Val == nil && withValue) {
		loc := i.Loc()
		if loc.isEmpty() {
			return nil
		}
		b := make([]byte, loc.Length) // TODO: Read less when not withValue.
		if _, err := o.file.ReadAt(b, loc.Offset); err != nil {
			return err
		}
		buf := bytes.NewBuffer(b)
		item := &Item{}
		var keyLength uint16
		var length, valLength uint32
		if err = binary.Read(buf, binary.BigEndian, &length); err != nil {
			return err
		}
		if err = binary.Read(buf, binary.BigEndian, &keyLength); err != nil {
			return err
		}
		if err = binary.Read(buf, binary.BigEndian, &valLength); err != nil {
			return err
		}
		if err = binary.Read(buf, binary.BigEndian, &item.Priority); err != nil {
			return err
		}
		hdrLength := 4 + 2 + 4 + 4
		if length != uint32(hdrLength)+uint32(keyLength)+valLength {
			return errors.New("mismatched itemLoc lengths")
		}
		item.Key = b[hdrLength : hdrLength+int(keyLength)]
		if withValue {
			item.Val = b[hdrLength+int(keyLength) : hdrLength+int(keyLength)+int(valLength)]
		}
		if o.callbacks.AfterItemRead != nil {
			item, err = o.callbacks.AfterItemRead(item)
			if err != nil {
				return err
			}
		}
		atomic.StorePointer(&i.item, unsafe.Pointer(item))
	}
	return nil
}

func (i *itemLoc) numBytes() uint64 {
	// TODO: Potential race here if a evictor concurrently has evicted the item.
	return uint64(len(i.Item().Key)) + uint64(len(i.Item().Val))
}

// Offset/location of a persisted range of bytes.
type ploc struct {
	Offset int64  `json:"o"` // Usable for os.ReadAt/WriteAt() at file offset 0.
	Length uint32 `json:"l"` // Number of bytes.
}

const ploc_length int = 8 + 4

var ploc_empty *ploc = &ploc{} // Sentinel.

func (p *ploc) isEmpty() bool {
	return p == nil || (p.Offset == int64(0) && p.Length == uint32(0))
}

func (p *ploc) write(b *bytes.Buffer) (err error) {
	if p != nil {
		if err := binary.Write(b, binary.BigEndian, p.Offset); err != nil {
			return err
		}
		if err := binary.Write(b, binary.BigEndian, p.Length); err != nil {
			return err
		}
		return nil
	}
	return ploc_empty.write(b)
}

func (p *ploc) read(b *bytes.Buffer) (res *ploc, err error) {
	if err := binary.Read(b, binary.BigEndian, &p.Offset); err != nil {
		return nil, err
	}
	if err := binary.Read(b, binary.BigEndian, &p.Length); err != nil {
		return nil, err
	}
	if p.isEmpty() {
		return nil, nil
	}
	return p, nil
}

// Retrieve an item by its key.  Use withValue of false if you don't
// need the item's value (Item.Val may be nil), which might be able
// to save on I/O and memory resources, especially for large values.
// The returned Item should be treated as immutable.
func (t *Collection) GetItem(key []byte, withValue bool) (i *Item, err error) {
	n := (*nodeLoc)(atomic.LoadPointer(&t.root))
	for {
		if err = n.read(t.store); err != nil || n.isEmpty() {
			break
		}
		i := &n.Node().item
		if err = i.read(t.store, false); err != nil {
			return nil, err
		}
		iItem := i.Item()
		if iItem == nil || iItem.Key == nil {
			return nil, errors.New("missing item after item.read() in GetItem()")
		}
		c := t.compare(key, iItem.Key)
		if c < 0 {
			n = &n.Node().left
		} else if c > 0 {
			n = &n.Node().right
		} else {
			if withValue {
				if err = i.read(t.store, withValue); err != nil {
					return nil, err
				}
			}
			return i.Item(), nil // Need to use Item() due to 2nd read().
		}
	}
	return nil, err
}

// Retrieve a value by its key.  Returns nil if the item is not in the
// collection.  The returned value should be treated as immutable.
func (t *Collection) Get(key []byte) (val []byte, err error) {
	i, err := t.GetItem(key, true)
	if err != nil {
		return nil, err
	}
	if i != nil {
		return i.Val, nil
	}
	return nil, nil
}

// Replace or insert an item of a given key.
// A random item Priority (e.g., rand.Int()) will usually work well,
// but advanced users may consider using non-random item priorities
// at the risk of unbalancing the lookup tree.
func (t *Collection) SetItem(item *Item) (err error) {
	if item.Key == nil || len(item.Key) > 0xffff || len(item.Key) == 0 ||
		item.Val == nil {
		return errors.New("Item.Key/Val missing or too long")
	}
	root := atomic.LoadPointer(&t.root)
	r, err := t.store.union(t, (*nodeLoc)(root),
		&nodeLoc{node: unsafe.Pointer(&node{item: itemLoc{item: unsafe.Pointer(&Item{
			Key:      item.Key,
			Val:      item.Val,
			Priority: item.Priority,
		})},
			numNodes: 1,
			numBytes: uint64(len(item.Key)) + uint64(len(item.Val)),
		})})
	if err != nil {
		return err
	}
	if !atomic.CompareAndSwapPointer(&t.root, root, unsafe.Pointer(r)) {
		return errors.New("concurrent mutation attempted")
	}
	return nil
}

// Replace or insert an item of a given key.
func (t *Collection) Set(key []byte, val []byte) error {
	return t.SetItem(&Item{Key: key, Val: val, Priority: int32(rand.Int())})
}

// Deletes an item of a given key.
func (t *Collection) Delete(key []byte) (wasDeleted bool, err error) {
	root := atomic.LoadPointer(&t.root)
	left, middle, right, err := t.store.split(t, (*nodeLoc)(root), key)
	if err != nil || middle.isEmpty() {
		return false, err
	}
	r, err := t.store.join(left, right)
	if err != nil {
		return false, err
	}
	if !atomic.CompareAndSwapPointer(&t.root, root, unsafe.Pointer(r)) {
		return false, errors.New("concurrent mutation attempted")
	}
	return true, nil
}

// Retrieves the item with the "smallest" key.
// The returned item should be treated as immutable.
func (t *Collection) MinItem(withValue bool) (*Item, error) {
	return t.store.walk(t, withValue,
		func(n *node) (*nodeLoc, bool) { return &n.left, true })
}

// Retrieves the item with the "largest" key.
// The returned item should be treated as immutable.
func (t *Collection) MaxItem(withValue bool) (*Item, error) {
	return t.store.walk(t, withValue,
		func(n *node) (*nodeLoc, bool) { return &n.right, true })
}

// Evict some clean items found by randomly walking a tree branch.
func (t *Collection) EvictSomeItems() (numEvicted uint64) {
	t.store.walk(t, false, func(n *node) (*nodeLoc, bool) {
		if !n.item.Loc().isEmpty() {
			atomic.StorePointer(&n.item.item, unsafe.Pointer(nil))
			numEvicted++
		}
		next := &n.left
		if (rand.Int() & 0x01) == 0x01 {
			next = &n.right
		}
		if next.isEmpty() {
			return nil, false
		}
		return next, true
	})
	return numEvicted
}

type ItemVisitor func(i *Item) bool

// Visit items greater-than-or-equal to the target key.
func (t *Collection) VisitItemsAscend(target []byte, withValue bool, visitor ItemVisitor) error {
	_, err := t.store.visitAscendNode(t, (*nodeLoc)(atomic.LoadPointer(&t.root)),
		target, withValue, visitor)
	return err
}

// Returns total number of items and total key bytes plus value bytes.
func (t *Collection) GetTotals() (numItems uint64, numBytes uint64, err error) {
	n := (*nodeLoc)(atomic.LoadPointer(&t.root))
	if err = n.read(t.store); err != nil || n.isEmpty() {
		return 0, 0, err
	}
	return n.Node().numNodes, n.Node().numBytes, nil
}

// Returns JSON representation of root node file location.
func (t *Collection) MarshalJSON() ([]byte, error) {
	loc := ((*nodeLoc)(atomic.LoadPointer(&t.root))).Loc()
	if loc.isEmpty() {
		return json.Marshal(ploc_empty)
	}
	return json.Marshal(loc)
}

// Unmarshals JSON representation of root node file location.
func (t *Collection) UnmarshalJSON(d []byte) error {
	p := ploc{}
	if err := json.Unmarshal(d, &p); err != nil {
		return err
	}
	atomic.StorePointer(&t.root, unsafe.Pointer(&nodeLoc{loc: unsafe.Pointer(&p)}))
	return nil
}

// Writes dirty items of a collection BUT (WARNING) does NOT write new
// root records.  Use Store.Flush() to write root records, which would
// make these writes visible to the next file re-opening/re-loading.
func (t *Collection) Write() (err error) {
	root := (*nodeLoc)(atomic.LoadPointer(&t.root))
	if err = t.store.flushItems(root); err != nil {
		return err
	}
	if err = t.store.flushNodes(root); err != nil {
		return err
	}
	return nil
}

func (o *Store) flushItems(nloc *nodeLoc) (err error) {
	if nloc == nil || !nloc.Loc().isEmpty() {
		return nil // Flush only unpersisted items of non-empty, unpersisted nodes.
	}
	node := nloc.Node()
	if node == nil {
		return nil
	}
	if err = o.flushItems(&node.left); err != nil {
		return err
	}
	if err = node.item.write(o); err != nil { // Write items in key order.
		return err
	}
	return o.flushItems(&node.right)
}

func (o *Store) flushNodes(nloc *nodeLoc) (err error) {
	if nloc == nil || !nloc.Loc().isEmpty() {
		return nil // Flush only non-empty, unpersisted nodes.
	}
	node := nloc.Node()
	if node == nil {
		return nil
	}
	if err = o.flushNodes(&node.left); err != nil {
		return err
	}
	if err = o.flushNodes(&node.right); err != nil {
		return err
	}
	return nloc.write(o) // Write nodes in children-first order.
}

func (o *Store) union(t *Collection, this *nodeLoc, that *nodeLoc) (res *nodeLoc, err error) {
	if err = this.read(o); err != nil {
		return empty, err
	}
	if err = that.read(o); err != nil {
		return empty, err
	}
	if this.isEmpty() {
		return that, nil
	}
	if that.isEmpty() {
		return this, nil
	}
	thisItem := &this.Node().item
	if err = thisItem.read(o, false); err != nil {
		return empty, err
	}
	thatItem := &that.Node().item
	if err = thatItem.read(o, false); err != nil {
		return empty, err
	}
	if thisItem.Item().Priority > thatItem.Item().Priority {
		left, middle, right, err := o.split(t, that, thisItem.Item().Key)
		if err != nil {
			return empty, err
		}
		newLeft, err := o.union(t, &this.Node().left, left)
		if err != nil {
			return empty, err
		}
		newRight, err := o.union(t, &this.Node().right, right)
		if err != nil {
			return empty, err
		}
		leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, newLeft, newRight)
		if err != nil {
			return empty, err
		}
		if middle.isEmpty() {
			return &nodeLoc{node: unsafe.Pointer(&node{
				item:     *thisItem,
				left:     *newLeft,
				right:    *newRight,
				numNodes: leftNum + rightNum + 1,
				numBytes: leftBytes + rightBytes + thisItem.numBytes(),
			})}, nil
		}
		return &nodeLoc{node: unsafe.Pointer(&node{
			item:     middle.Node().item,
			left:     *newLeft,
			right:    *newRight,
			numNodes: leftNum + rightNum + 1,
			numBytes: leftBytes + rightBytes + middle.Node().item.numBytes(),
		})}, nil
	}
	// We don't use middle because the "that" node has precedence.
	left, _, right, err := o.split(t, this, thatItem.Item().Key)
	if err != nil {
		return empty, err
	}
	newLeft, err := o.union(t, left, &that.Node().left)
	if err != nil {
		return empty, err
	}
	newRight, err := o.union(t, right, &that.Node().right)
	if err != nil {
		return empty, err
	}
	leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, newLeft, newRight)
	if err != nil {
		return empty, err
	}
	return &nodeLoc{node: unsafe.Pointer(&node{
		item:     *thatItem,
		left:     *newLeft,
		right:    *newRight,
		numNodes: leftNum + rightNum + 1,
		numBytes: leftBytes + rightBytes + thatItem.numBytes(),
	})}, nil
}

// Splits a treap into two treaps based on a split key "s".  The
// result is (left, middle, right), where left treap has keys < s,
// right treap has keys > s, and middle is either...
// * empty/nil - meaning key s was not in the original treap.
// * non-empty - returning the original nodeLoc/item that had key s.
func (o *Store) split(t *Collection, n *nodeLoc, s []byte) (
	*nodeLoc, *nodeLoc, *nodeLoc, error) {
	if err := n.read(o); err != nil || n.isEmpty() {
		return empty, empty, empty, err
	}

	nNode := n.Node()
	nItem := &nNode.item
	if err := nItem.read(o, false); err != nil {
		return empty, empty, empty, err
	}

	c := t.compare(s, nItem.Item().Key)
	if c == 0 {
		return &nNode.left, n, &nNode.right, nil
	}

	if c < 0 {
		left, middle, right, err := o.split(t, &nNode.left, s)
		if err != nil {
			return empty, empty, empty, err
		}
		leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, right, &nNode.right)
		if err != nil {
			return empty, empty, empty, err
		}
		return left, middle, &nodeLoc{node: unsafe.Pointer(&node{
			item:     *nItem,
			left:     *right,
			right:    nNode.right,
			numNodes: leftNum + rightNum + 1,
			numBytes: leftBytes + rightBytes + nItem.numBytes(),
		})}, nil
	}

	left, middle, right, err := o.split(t, &nNode.right, s)
	if err != nil {
		return empty, empty, empty, err
	}
	leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, &nNode.left, left)
	if err != nil {
		return empty, empty, empty, err
	}
	return &nodeLoc{node: unsafe.Pointer(&node{
		item:     *nItem,
		left:     nNode.left,
		right:    *left,
		numNodes: leftNum + rightNum + 1,
		numBytes: leftBytes + rightBytes + nItem.numBytes(),
	})}, middle, right, nil
}

// All the keys from this should be < keys from that.
func (o *Store) join(this *nodeLoc, that *nodeLoc) (res *nodeLoc, err error) {
	if err = this.read(o); err != nil {
		return empty, err
	}
	if err = that.read(o); err != nil {
		return empty, err
	}
	if this.isEmpty() {
		return that, nil
	}
	if that.isEmpty() {
		return this, nil
	}
	thisItem := &this.Node().item
	if err = thisItem.read(o, false); err != nil {
		return empty, err
	}
	thatItem := &that.Node().item
	if err = thatItem.read(o, false); err != nil {
		return empty, err
	}
	if thisItem.Item().Priority > thatItem.Item().Priority {
		newRight, err := o.join(&this.Node().right, that)
		if err != nil {
			return empty, err
		}
		leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, &this.Node().left, newRight)
		if err != nil {
			return empty, err
		}
		return &nodeLoc{node: unsafe.Pointer(&node{
			item:     *thisItem,
			left:     this.Node().left,
			right:    *newRight,
			numNodes: leftNum + rightNum + 1,
			numBytes: leftBytes + rightBytes + thisItem.numBytes(),
		})}, nil
	}
	newLeft, err := o.join(this, &that.Node().left)
	if err != nil {
		return empty, err
	}
	leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, newLeft, &that.Node().right)
	if err != nil {
		return empty, err
	}
	return &nodeLoc{node: unsafe.Pointer(&node{
		item:     *thatItem,
		left:     *newLeft,
		right:    that.Node().right,
		numNodes: leftNum + rightNum + 1,
		numBytes: leftBytes + rightBytes + thatItem.numBytes(),
	})}, nil
}

func (o *Store) walk(t *Collection, withValue bool, cfn func(*node) (*nodeLoc, bool)) (
	res *Item, err error) {
	n := (*nodeLoc)(atomic.LoadPointer(&t.root))
	if err = n.read(o); err != nil || n.isEmpty() {
		return nil, err
	}
	for {
		child, ok := cfn(n.Node())
		if !ok {
			return nil, nil
		}
		if err = child.read(o); err != nil {
			return nil, err
		}
		if child.isEmpty() {
			i := &n.Node().item
			if err = i.read(o, withValue); err == nil {
				return i.Item(), nil
			}
			return nil, err
		}
		n = child
	}
	return nil, nil
}

func (o *Store) visitAscendNode(t *Collection, n *nodeLoc, target []byte,
	withValue bool, visitor ItemVisitor) (bool, error) {
	if err := n.read(o); err != nil {
		return false, err
	}
	if n.isEmpty() {
		return true, nil
	}
	nItem := &n.Node().item
	if err := nItem.read(o, false); err != nil {
		return false, err
	}
	if t.compare(target, nItem.Item().Key) <= 0 {
		keepGoing, err := o.visitAscendNode(t, &n.Node().left, target, withValue, visitor)
		if err != nil || !keepGoing {
			return false, err
		}
		if err := nItem.read(o, withValue); err != nil {
			return false, err
		}
		if !visitor(nItem.Item()) {
			return false, nil
		}
	}
	return o.visitAscendNode(t, &n.Node().right, target, withValue, visitor)
}

func (o *Store) writeRoots() error {
	coll := *(*map[string]*Collection)(atomic.LoadPointer(&o.coll))
	sJSON, err := json.Marshal(coll)
	if err != nil {
		return err
	}
	offset := atomic.LoadInt64(&o.size)
	length := 2*len(MAGIC_BEG) + 4 + 4 + len(sJSON) + 8 + 4 + 2*len(MAGIC_END)
	b := bytes.NewBuffer(make([]byte, length)[:0])
	b.Write(MAGIC_BEG)
	b.Write(MAGIC_BEG)
	binary.Write(b, binary.BigEndian, uint32(VERSION))
	binary.Write(b, binary.BigEndian, uint32(length))
	b.Write(sJSON)
	binary.Write(b, binary.BigEndian, int64(offset))
	binary.Write(b, binary.BigEndian, uint32(length))
	b.Write(MAGIC_END)
	b.Write(MAGIC_END)
	if _, err := o.file.WriteAt(b.Bytes()[:length], offset); err != nil {
		return err
	}
	atomic.StoreInt64(&o.size, offset+int64(length))
	return nil
}

func (o *Store) readRoots() error {
	finfo, err := o.file.Stat()
	if err != nil {
		return err
	}
	atomic.StoreInt64(&o.size, finfo.Size())
	if o.size <= 0 {
		return nil
	}
	endBArr := make([]byte, 8+4+2*len(MAGIC_END))
	minSize := int64(2*len(MAGIC_BEG) + 4 + 4 + len(endBArr))
	for {
		for { // Scan backwards for MAGIC_END.
			if atomic.LoadInt64(&o.size) <= minSize {
				return errors.New("couldn't find roots; file corrupted or wrong?")
			}
			if _, err := o.file.ReadAt(endBArr,
				atomic.LoadInt64(&o.size)-int64(len(endBArr))); err != nil {
				return err
			}
			if bytes.Equal(MAGIC_END, endBArr[8+4:8+4+len(MAGIC_END)]) &&
				bytes.Equal(MAGIC_END, endBArr[8+4+len(MAGIC_END):]) {
				break
			}
			atomic.AddInt64(&o.size, -1) // TODO: optimizations to scan backwards faster.
		}
		// Read and check the roots.
		var offset int64
		var length uint32
		endBuf := bytes.NewBuffer(endBArr)
		err = binary.Read(endBuf, binary.BigEndian, &offset)
		if err != nil {
			return err
		}
		if err = binary.Read(endBuf, binary.BigEndian, &length); err != nil {
			return err
		}
		if offset >= 0 && offset < atomic.LoadInt64(&o.size)-int64(minSize) &&
			length == uint32(atomic.LoadInt64(&o.size)-offset) {
			data := make([]byte, atomic.LoadInt64(&o.size)-offset-int64(len(endBArr)))
			if _, err := o.file.ReadAt(data, offset); err != nil {
				return err
			}
			if bytes.Equal(MAGIC_BEG, data[:len(MAGIC_BEG)]) &&
				bytes.Equal(MAGIC_BEG, data[len(MAGIC_BEG):2*len(MAGIC_BEG)]) {
				var version, length0 uint32
				b := bytes.NewBuffer(data[2*len(MAGIC_BEG):])
				if err = binary.Read(b, binary.BigEndian, &version); err != nil {
					return err
				}
				if err = binary.Read(b, binary.BigEndian, &length0); err != nil {
					return err
				}
				if version != VERSION {
					return fmt.Errorf("version mismatch: "+
						"current version: %v != found version: %v", VERSION, version)
				}
				if length0 != length {
					return fmt.Errorf("length mismatch: "+
						"wanted length: %v != found length: %v", length0, length)
				}
				m := make(map[string]*Collection)
				if err = json.Unmarshal(data[2*len(MAGIC_BEG)+4+4:], &m); err != nil {
					return err
				}
				for _, t := range m {
					t.store = o
					t.compare = bytes.Compare
				}
				atomic.StorePointer(&o.coll, unsafe.Pointer(&m))
				return nil
			} // else, perhaps value was unlucky in having MAGIC_END's.
		} // else, perhaps a gkvlite file was stored as a value.
		atomic.AddInt64(&o.size, -1) // Roots were wrong, so keep scanning.
	}
	return nil
}
