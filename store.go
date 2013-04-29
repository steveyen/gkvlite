package gkvlite

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
	// Atomic CAS'ed int64/uint64's must be at the top for 32-bit compatibility.
	size       int64          // Atomic protected; file size or next write position.
	nodeAllocs uint64         // Atomic protected.
	coll       unsafe.Pointer // Copy-on-write map[string]*Collection.
	file       StoreFile      // When nil, it's memory-only or no persistence.
	callbacks  StoreCallbacks // Optional / may be nil.
	readOnly   bool           // When true, Flush()'ing is disallowed.
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

	ItemValLength func(i *Item) int
	ItemValWrite  func(i *Item, w io.WriterAt, offset int64) error
	ItemValRead   func(i *Item, r io.ReaderAt, offset int64, valLength uint32) error

	// Invoked when a Store is reloaded (during NewStoreEx()) from
	// disk, this callback allows the user to optionally supply a key
	// comparison func for each collection.  Otherwise, the default is
	// the bytes.Compare func.
	KeyCompareForCollection func(collName string) KeyCompare
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
// the KeyCompare function on an existing Collection.  In either case,
// a new Collection to use is returned.  A newly created Collection
// and any mutations on it won't be persisted until you do a Flush().
func (s *Store) SetCollection(name string, compare KeyCompare) *Collection {
	if compare == nil {
		compare = bytes.Compare
	}
	for {
		orig := atomic.LoadPointer(&s.coll)
		coll := copyColl(*(*map[string]*Collection)(orig))
		cnew := s.MakePrivateCollection(compare)
		if coll[name] != nil {
			cnew.root = unsafe.Pointer(atomic.LoadPointer(&coll[name].root))
		}
		coll[name] = cnew
		if atomic.CompareAndSwapPointer(&s.coll, orig, unsafe.Pointer(&coll)) {
			return cnew
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
		root:    unsafe.Pointer(empty_nodeLoc),
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

// Updates provided map with statistics.
func (s *Store) Stats(out map[string]uint64) {
	out["fileSize"] = uint64(atomic.LoadInt64(&s.size))
	out["nodeAllocs"] = atomic.LoadUint64(&s.nodeAllocs)
}

// User-supplied key comparison func should return 0 if a == b,
// -1 if a < b, and +1 if a > b.  For example: bytes.Compare()
type KeyCompare func(a, b []byte) int

// A persistable collection of ordered key-values (Item's).
type Collection struct {
	store   *Store
	compare KeyCompare
	root    unsafe.Pointer // Value is *nodeLoc type.

	// Only a single mutator should access the free lists.
	freeNodes    *node
	freeNodeLocs *nodeLoc
}

// A persistable node.
type node struct {
	numNodes, numBytes uint64
	item               itemLoc
	left, right        nodeLoc
	next               *node // For free-list tracking.
}

// A persistable node and its persistence location.
type nodeLoc struct {
	loc  unsafe.Pointer // *ploc - can be nil if node is dirty (not yet persisted).
	node unsafe.Pointer // *node - can be nil if node is not fetched into memory yet.
	next *nodeLoc       // For free-list tracking.
}

var empty_nodeLoc = &nodeLoc{} // Sentinel.

func (nloc *nodeLoc) Loc() *ploc {
	return (*ploc)(atomic.LoadPointer(&nloc.loc))
}

func (nloc *nodeLoc) Node() *node {
	return (*node)(atomic.LoadPointer(&nloc.node))
}

func (nloc *nodeLoc) Copy(src *nodeLoc) {
	if src == nil {
		nloc.Copy(empty_nodeLoc)
		return
	}
	atomic.StorePointer(&nloc.loc, unsafe.Pointer(src.Loc()))
	atomic.StorePointer(&nloc.node, unsafe.Pointer(src.Node()))
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
		b := make([]byte, length)
		pos := 0
		pos = node.item.Loc().write(b, pos)
		pos = node.left.Loc().write(b, pos)
		pos = node.right.Loc().write(b, pos)
		binary.BigEndian.PutUint64(b[pos:pos+8], node.numNodes)
		pos += 8
		binary.BigEndian.PutUint64(b[pos:pos+8], node.numBytes)
		pos += 8
		if pos != length {
			return fmt.Errorf("nodeLoc.write() pos: %v didn't match length: %v",
				pos, length)
		}
		if _, err := o.file.WriteAt(b, offset); err != nil {
			return err
		}
		atomic.StoreInt64(&o.size, offset+int64(length))
		atomic.StorePointer(&nloc.loc,
			unsafe.Pointer(&ploc{Offset: offset, Length: uint32(length)}))
	}
	return nil
}

func (nloc *nodeLoc) read(o *Store) (n *node, err error) {
	if nloc == nil {
		return nil, nil
	}
	n = nloc.Node()
	if n != nil {
		return n, nil
	}
	loc := nloc.Loc()
	if loc.isEmpty() {
		return nil, nil
	}
	if loc.Length != uint32(ploc_length+ploc_length+ploc_length+8+8) {
		return nil, fmt.Errorf("unexpected node loc.Length: %v != %v",
			loc.Length, ploc_length+ploc_length+ploc_length+8+8)
	}
	b := make([]byte, loc.Length)
	if _, err := o.file.ReadAt(b, loc.Offset); err != nil {
		return nil, err
	}
	pos := 0
	atomic.AddUint64(&o.nodeAllocs, 1)
	n = &node{}
	var p *ploc
	p = &ploc{}
	p, pos = p.read(b, pos)
	n.item.loc = unsafe.Pointer(p)
	p = &ploc{}
	p, pos = p.read(b, pos)
	n.left.loc = unsafe.Pointer(p)
	p = &ploc{}
	p, pos = p.read(b, pos)
	n.right.loc = unsafe.Pointer(p)
	n.numNodes = binary.BigEndian.Uint64(b[pos : pos+8])
	pos += 8
	n.numBytes = binary.BigEndian.Uint64(b[pos : pos+8])
	pos += 8
	if pos != len(b) {
		return nil, fmt.Errorf("nodeLoc.read() pos: %v didn't match length: %v",
			pos, len(b))
	}
	atomic.StorePointer(&nloc.node, unsafe.Pointer(n))
	return n, nil
}

func numInfo(o *Store, left *nodeLoc, right *nodeLoc) (
	leftNum uint64, leftBytes uint64, rightNum uint64, rightBytes uint64, err error) {
	leftNode, err := left.read(o)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	rightNode, err := right.read(o)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if !left.isEmpty() && leftNode != nil {
		leftNum = leftNode.numNodes
		leftBytes = leftNode.numBytes
	}
	if !right.isEmpty() && rightNode != nil {
		rightNum = rightNode.numNodes
		rightBytes = rightNode.numBytes
	}
	return leftNum, leftBytes, rightNum, rightBytes, nil
}

// A persistable item.
type Item struct {
	Transient unsafe.Pointer // For any ephemeral data; atomic CAS recommended.
	Key, Val  []byte         // Val may be nil if not fetched into memory yet.
	Priority  int32          // Use rand.Int() for probabilistic balancing.
}

// Number of Key bytes plus number of Val bytes.
func (i *Item) NumBytes(o *Store) int {
	return len(i.Key) + i.NumValBytes(o)
}

func (i *Item) NumValBytes(o *Store) int {
	if o.callbacks.ItemValLength != nil {
		return o.callbacks.ItemValLength(i)
	}
	return len(i.Val)
}

// A persistable item and its persistence location.
type itemLoc struct {
	loc  unsafe.Pointer // *ploc - can be nil if item is dirty (not yet persisted).
	item unsafe.Pointer // *Item - can be nil if item is not fetched into memory yet.
}

var empty_itemLoc = &itemLoc{}

func (i *itemLoc) Loc() *ploc {
	return (*ploc)(atomic.LoadPointer(&i.loc))
}

func (i *itemLoc) Item() *Item {
	return (*Item)(atomic.LoadPointer(&i.item))
}

func (i *itemLoc) Copy(src *itemLoc) {
	if src == nil {
		i.Copy(empty_itemLoc)
		return
	}
	atomic.StorePointer(&i.loc, unsafe.Pointer(src.Loc()))
	atomic.StorePointer(&i.item, unsafe.Pointer(src.Item()))
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
		hlength := 4 + 2 + 4 + 4 + len(iItem.Key)
		vlength := iItem.NumValBytes(o)
		ilength := hlength + vlength
		b := make([]byte, hlength)
		pos := 0
		binary.BigEndian.PutUint32(b[pos:pos+4], uint32(ilength))
		pos += 4
		binary.BigEndian.PutUint16(b[pos:pos+2], uint16(len(iItem.Key)))
		pos += 2
		binary.BigEndian.PutUint32(b[pos:pos+4], uint32(vlength))
		pos += 4
		binary.BigEndian.PutUint32(b[pos:pos+4], uint32(iItem.Priority))
		pos += 4
		pos += copy(b[pos:], iItem.Key)
		if pos != hlength {
			return fmt.Errorf("itemLoc.write() pos: %v didn't match hlength: %v",
				pos, hlength)
		}
		if _, err := o.file.WriteAt(b, offset); err != nil {
			return err
		}
		if o.callbacks.ItemValWrite != nil {
			err := o.callbacks.ItemValWrite(iItem, o.file, offset+int64(pos))
			if err != nil {
				return err
			}
		} else {
			_, err := o.file.WriteAt(iItem.Val, offset+int64(pos))
			if err != nil {
				return err
			}
		}
		atomic.StoreInt64(&o.size, offset+int64(ilength))
		atomic.StorePointer(&i.loc,
			unsafe.Pointer(&ploc{Offset: offset, Length: uint32(ilength)}))
	}
	return nil
}

func (iloc *itemLoc) read(o *Store, withValue bool) (i *Item, err error) {
	if iloc == nil {
		return nil, nil
	}
	i = iloc.Item()
	if i == nil || (i.Val == nil && withValue) {
		loc := iloc.Loc()
		if loc.isEmpty() {
			return nil, nil
		}
		hdrLength := 4 + 2 + 4 + 4
		if loc.Length < uint32(hdrLength) {
			return nil, fmt.Errorf("unexpected item loc.Length: %v < %v",
				loc.Length, hdrLength)
		}
		b := make([]byte, hdrLength)
		if _, err := o.file.ReadAt(b, loc.Offset); err != nil {
			return nil, err
		}
		i = &Item{}
		pos := 0
		length := binary.BigEndian.Uint32(b[pos : pos+4])
		pos += 4
		keyLength := binary.BigEndian.Uint16(b[pos : pos+2])
		pos += 2
		valLength := binary.BigEndian.Uint32(b[pos : pos+4])
		pos += 4
		i.Priority = int32(binary.BigEndian.Uint32(b[pos : pos+4]))
		pos += 4
		if length != uint32(hdrLength)+uint32(keyLength)+valLength {
			return nil, errors.New("mismatched itemLoc lengths")
		}
		if pos != hdrLength {
			return nil, fmt.Errorf("read pos != hdrLength, %v != %v", pos, hdrLength)
		}
		i.Key = make([]byte, keyLength)
		if _, err := o.file.ReadAt(i.Key,
			loc.Offset+int64(hdrLength)); err != nil {
			return nil, err
		}
		if withValue {
			if o.callbacks.ItemValRead != nil {
				err := o.callbacks.ItemValRead(i, o.file,
					loc.Offset+int64(hdrLength)+int64(keyLength), valLength)
				if err != nil {
					return nil, err
				}
			} else {
				i.Val = make([]byte, valLength)
				if _, err := o.file.ReadAt(i.Val,
					loc.Offset+int64(hdrLength)+int64(keyLength)); err != nil {
					return nil, err
				}
			}
		}
		if o.callbacks.AfterItemRead != nil {
			i, err = o.callbacks.AfterItemRead(i)
			if err != nil {
				return nil, err
			}
		}
		atomic.StorePointer(&iloc.item, unsafe.Pointer(i))
	}
	return i, nil
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

func (p *ploc) write(b []byte, pos int) int {
	if p == nil {
		return ploc_empty.write(b, pos)
	}
	binary.BigEndian.PutUint64(b[pos:pos+8], uint64(p.Offset))
	pos += 8
	binary.BigEndian.PutUint32(b[pos:pos+4], p.Length)
	pos += 4
	return pos
}

func (p *ploc) read(b []byte, pos int) (*ploc, int) {
	p.Offset = int64(binary.BigEndian.Uint64(b[pos : pos+8]))
	pos += 8
	p.Length = binary.BigEndian.Uint32(b[pos : pos+4])
	pos += 4
	if p.isEmpty() {
		return nil, pos
	}
	return p, pos
}

// Retrieve an item by its key.  Use withValue of false if you don't
// need the item's value (Item.Val may be nil), which might be able
// to save on I/O and memory resources, especially for large values.
// The returned Item should be treated as immutable.
func (t *Collection) GetItem(key []byte, withValue bool) (i *Item, err error) {
	n := (*nodeLoc)(atomic.LoadPointer(&t.root))
	for {
		nNode, err := n.read(t.store)
		if err != nil || n.isEmpty() || nNode == nil {
			return nil, err
		}
		i := &nNode.item
		iItem, err := i.read(t.store, false)
		if err != nil {
			return nil, err
		}
		if iItem == nil || iItem.Key == nil {
			return nil, errors.New("missing item after item.read() in GetItem()")
		}
		c := t.compare(key, iItem.Key)
		if c < 0 {
			n = &nNode.left
		} else if c > 0 {
			n = &nNode.right
		} else {
			if withValue {
				iItem, err = i.read(t.store, withValue)
				if err != nil {
					return nil, err
				}
			}
			return iItem, nil
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
	if item.Priority < 0 {
		return errors.New("Item.Priority must be non-negative")
	}
	root := atomic.LoadPointer(&t.root)
	n := t.mkNode(nil, nil, nil,
		1, uint64(len(item.Key)) + uint64(item.NumValBytes(t.store)))
	n.item = itemLoc{item: unsafe.Pointer(&Item{
		Key:      item.Key,
		Val:      item.Val,
		Priority: item.Priority,
	})} // Separate item initialization to avoid garbage.
	nloc := t.mkNodeLoc(n)
	r, err := t.store.union(t, (*nodeLoc)(root), nloc)
	if err != nil {
		return err
	}
	if nloc != r {
		t.freeNodeLoc(nloc)
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
	left, middle, right, _, _, err := t.store.split(t, (*nodeLoc)(root), key)
	if err != nil || middle.isEmpty() {
		return false, err
	}
	r, err := t.store.join(t, left, right)
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
type ItemVisitorEx func(i *Item, depth uint64) bool

// Visit items greater-than-or-equal to the target key in ascending order.
func (t *Collection) VisitItemsAscend(target []byte, withValue bool, v ItemVisitor) error {
	return t.VisitItemsAscendEx(target, withValue,
		func(i *Item, depth uint64) bool { return v(i) })
}

// Visit items less-than the target key in descending order.
func (t *Collection) VisitItemsDescend(target []byte, withValue bool, v ItemVisitor) error {
	return t.VisitItemsDescendEx(target, withValue,
		func(i *Item, depth uint64) bool { return v(i) })
}

// Visit items greater-than-or-equal to the target key in ascending order; with depth info.
func (t *Collection) VisitItemsAscendEx(target []byte, withValue bool,
	visitor ItemVisitorEx) error {
	_, err := t.store.visitNodes(t, (*nodeLoc)(atomic.LoadPointer(&t.root)),
		target, withValue, visitor, 0, ascendChoice)
	return err
}

// Visit items less-than the target key in descending order; with depth info.
func (t *Collection) VisitItemsDescendEx(target []byte, withValue bool,
	visitor ItemVisitorEx) error {
	_, err := t.store.visitNodes(t, (*nodeLoc)(atomic.LoadPointer(&t.root)),
		target, withValue, visitor, 0, descendChoice)
	return err
}

func ascendChoice(cmp int, n *node) (bool, *nodeLoc, *nodeLoc) {
	return cmp <= 0, &n.left, &n.right
}

func descendChoice(cmp int, n *node) (bool, *nodeLoc, *nodeLoc) {
	return cmp > 0, &n.right, &n.left
}

// Returns total number of items and total key bytes plus value bytes.
func (t *Collection) GetTotals() (numItems uint64, numBytes uint64, err error) {
	n := (*nodeLoc)(atomic.LoadPointer(&t.root))
	nNode, err := n.read(t.store)
	if err != nil || n.isEmpty() || nNode == nil {
		return 0, 0, err
	}
	return nNode.numNodes, nNode.numBytes, nil
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

// Assumes that the caller serializes invocations.
func (t *Collection) mkNodeLoc(n *node) *nodeLoc {
	nloc := t.freeNodeLocs
	if nloc == nil {
		nloc = &nodeLoc{}
	}
	t.freeNodeLocs = nloc.next
	nloc.loc = unsafe.Pointer(nil)
	nloc.node = unsafe.Pointer(n)
	nloc.next = nil
	return nloc
}

func (t *Collection) freeNodeLoc(nloc *nodeLoc) {
	if nloc == nil {
		return
	}
	nloc.loc = unsafe.Pointer(nil)
	nloc.node = unsafe.Pointer(nil)
	nloc.next = t.freeNodeLocs
	t.freeNodeLocs = nloc
}

// Assumes that the caller serializes invocations.
func (t *Collection) mkNode(itemIn *itemLoc, leftIn *nodeLoc, rightIn *nodeLoc,
	numNodesIn uint64, numBytesIn uint64) *node {
	n := t.freeNodes
	if n == nil {
		atomic.AddUint64(&t.store.nodeAllocs, 1)
		n = &node{}
	}
	t.freeNodes = n.next
	n.item.Copy(itemIn)
	n.left.Copy(leftIn)
	n.right.Copy(rightIn)
	n.numNodes = numNodesIn
	n.numBytes = numBytesIn
	n.next = nil
	return n
}

func (t *Collection) freeNode(n *node) {
	if n == nil {
		return
	}
	n.item = *empty_itemLoc
	n.left = *empty_nodeLoc
	n.right = *empty_nodeLoc
	n.numNodes = 0
	n.numBytes = 0
	n.next = t.freeNodes
	t.freeNodes = n
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
	thisNode, err := this.read(o)
	if err != nil {
		return empty_nodeLoc, err
	}
	thatNode, err := that.read(o)
	if err != nil {
		return empty_nodeLoc, err
	}
	if this.isEmpty() || thisNode == nil {
		return that, nil
	}
	if that.isEmpty() || thatNode == nil {
		return this, nil
	}
	thisItemLoc := &thisNode.item
	thisItem, err := thisItemLoc.read(o, false)
	if err != nil {
		return empty_nodeLoc, err
	}
	thatItemLoc := &thatNode.item
	thatItem, err := thatItemLoc.read(o, false)
	if err != nil {
		return empty_nodeLoc, err
	}
	if thisItem.Priority > thatItem.Priority {
		left, middle, right, leftIsNew, rightIsNew, err := o.split(t, that, thisItem.Key)
		if err != nil {
			return empty_nodeLoc, err
		}
		newLeft, err := o.union(t, &thisNode.left, left)
		if err != nil {
			return empty_nodeLoc, err
		}
		if leftIsNew && left != newLeft {
			t.freeNodeLoc(left)
		}
		newRight, err := o.union(t, &thisNode.right, right)
		if err != nil {
			return empty_nodeLoc, err
		}
		if rightIsNew && right != newRight {
			t.freeNodeLoc(right)
		}
		leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, newLeft, newRight)
		if err != nil {
			return empty_nodeLoc, err
		}
		if middle.isEmpty() {
			return t.mkNodeLoc(t.mkNode(thisItemLoc, newLeft, newRight,
				leftNum + rightNum + 1,
				leftBytes + rightBytes + uint64(thisItem.NumBytes(o)))), nil
		}
		middleNode, err := middle.read(o)
		if err != nil {
			return empty_nodeLoc, err
		}
		middleItem, err := middleNode.item.read(o, false)
		if err != nil {
			return empty_nodeLoc, err
		}
		return t.mkNodeLoc(t.mkNode(&middleNode.item, newLeft, newRight,
			leftNum + rightNum + 1,
			leftBytes + rightBytes + uint64(middleItem.NumBytes(o)))), nil
	}
	// We don't use middle because the "that" node has precedence.
	left, _, right, leftIsNew, rightIsNew, err := o.split(t, this, thatItem.Key)
	if err != nil {
		return empty_nodeLoc, err
	}
	newLeft, err := o.union(t, left, &thatNode.left)
	if err != nil {
		return empty_nodeLoc, err
	}
	if leftIsNew && left != newLeft {
		t.freeNodeLoc(left)
	}
	newRight, err := o.union(t, right, &thatNode.right)
	if err != nil {
		return empty_nodeLoc, err
	}
	if rightIsNew && right != newRight {
		t.freeNodeLoc(right)
	}
	leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, newLeft, newRight)
	if err != nil {
		return empty_nodeLoc, err
	}
	return t.mkNodeLoc(t.mkNode(thatItemLoc, newLeft, newRight,
		leftNum + rightNum + 1,
		leftBytes + rightBytes + uint64(thatItem.NumBytes(o)))), nil
}

// Splits a treap into two treaps based on a split key "s".  The
// result is (left, middle, right), where left treap has keys < s,
// right treap has keys > s, and middle is either...
// * empty/nil - meaning key s was not in the original treap.
// * non-empty - returning the original nodeLoc/item that had key s.
// The two bool's indicate whether the left/right returned nodeLoc's are new.
func (o *Store) split(t *Collection, n *nodeLoc, s []byte) (
	*nodeLoc, *nodeLoc, *nodeLoc, bool, bool, error) {
	nNode, err := n.read(o)
	if err != nil || n.isEmpty() || nNode == nil {
		return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, false, false, err
	}

	nItemLoc := &nNode.item
	nItem, err := nItemLoc.read(o, false)
	if err != nil {
		return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, false, false, err
	}

	c := t.compare(s, nItem.Key)
	if c == 0 {
		return &nNode.left, n, &nNode.right, false, false, nil
	}

	if c < 0 {
		left, middle, right, leftIsNew, rightIsNew, err := o.split(t, &nNode.left, s)
		if err != nil {
			return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, false, false, err
		}
		leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, right, &nNode.right)
		if err != nil {
			return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, false, false, err
		}
		newRight := t.mkNodeLoc(t.mkNode(nItemLoc, right, &nNode.right,
			leftNum + rightNum + 1,
			leftBytes + rightBytes + uint64(nItem.NumBytes(o))))
		if rightIsNew {
			t.freeNodeLoc(right)
		}
		return left, middle, newRight, leftIsNew, true, nil
	}

	left, middle, right, leftIsNew, rightIsNew, err := o.split(t, &nNode.right, s)
	if err != nil {
		return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, false, false, err
	}
	leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, &nNode.left, left)
	if err != nil {
		return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, false, false, err
	}
	newLeft := t.mkNodeLoc(t.mkNode(nItemLoc, &nNode.left, left,
		leftNum + rightNum + 1,
		leftBytes + rightBytes + uint64(nItem.NumBytes(o))))
	if leftIsNew {
		t.freeNodeLoc(left)
	}
	return newLeft, middle, right, true, rightIsNew, nil
}

// All the keys from this should be < keys from that.
func (o *Store) join(t *Collection, this *nodeLoc, that *nodeLoc) (
	res *nodeLoc, err error) {
	thisNode, err := this.read(o)
	if err != nil {
		return empty_nodeLoc, err
	}
	thatNode, err := that.read(o)
	if err != nil {
		return empty_nodeLoc, err
	}
	if this.isEmpty() || thisNode == nil {
		return that, nil
	}
	if that.isEmpty() || thatNode == nil {
		return this, nil
	}
	thisItemLoc := &thisNode.item
	thisItem, err := thisItemLoc.read(o, false)
	if err != nil {
		return empty_nodeLoc, err
	}
	thatItemLoc := &thatNode.item
	thatItem, err := thatItemLoc.read(o, false)
	if err != nil {
		return empty_nodeLoc, err
	}
	if thisItem.Priority > thatItem.Priority {
		newRight, err := o.join(t, &thisNode.right, that)
		if err != nil {
			return empty_nodeLoc, err
		}
		leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, &thisNode.left, newRight)
		if err != nil {
			return empty_nodeLoc, err
		}
		return t.mkNodeLoc(t.mkNode(thisItemLoc, &thisNode.left, newRight,
			leftNum + rightNum + 1,
			leftBytes + rightBytes + uint64(thisItem.NumBytes(o)))), nil
	}
	newLeft, err := o.join(t, this, &thatNode.left)
	if err != nil {
		return empty_nodeLoc, err
	}
	leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, newLeft, &thatNode.right)
	if err != nil {
		return empty_nodeLoc, err
	}
	return t.mkNodeLoc(t.mkNode(thatItemLoc, newLeft, &thatNode.right,
		leftNum + rightNum + 1,
		leftBytes + rightBytes + uint64(thatItem.NumBytes(o)))), nil
}

func (o *Store) walk(t *Collection, withValue bool, cfn func(*node) (*nodeLoc, bool)) (
	res *Item, err error) {
	n := (*nodeLoc)(atomic.LoadPointer(&t.root))
	nNode, err := n.read(o)
	if err != nil || n.isEmpty() || nNode == nil {
		return nil, err
	}
	for {
		child, ok := cfn(nNode)
		if !ok {
			return nil, nil
		}
		childNode, err := child.read(o)
		if err != nil {
			return nil, err
		}
		if child.isEmpty() || childNode == nil {
			i, err := nNode.item.read(o, withValue)
			if err != nil {
				return nil, err
			}
			return i, nil
		}
		nNode = childNode
	}
	return nil, nil
}

func (o *Store) visitNodes(t *Collection, n *nodeLoc, target []byte,
	withValue bool, visitor ItemVisitorEx, depth uint64,
	choiceFunc func(int, *node) (bool, *nodeLoc, *nodeLoc)) (bool, error) {
	nNode, err := n.read(o)
	if err != nil {
		return false, err
	}
	if n.isEmpty() || nNode == nil {
		return true, nil
	}
	nItemLoc := &nNode.item
	nItem, err := nItemLoc.read(o, false)
	if err != nil {
		return false, err
	}
	choice, choiceT, choiceF := choiceFunc(t.compare(target, nItem.Key), nNode)
	if choice {
		keepGoing, err :=
			o.visitNodes(t, choiceT, target, withValue, visitor, depth+1, choiceFunc)
		if err != nil || !keepGoing {
			return false, err
		}
		nItem, err := nItemLoc.read(o, withValue)
		if err != nil {
			return false, err
		}
		if !visitor(nItem, depth) {
			return false, nil
		}
	}
	return o.visitNodes(t, choiceF, target, withValue, visitor, depth+1, choiceFunc)
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
				for collName, t := range m {
					t.store = o
					if o.callbacks.KeyCompareForCollection != nil {
						t.compare = o.callbacks.KeyCompareForCollection(collName)
					}
					if t.compare == nil {
						t.compare = bytes.Compare
					}
				}
				atomic.StorePointer(&o.coll, unsafe.Pointer(&m))
				return nil
			} // else, perhaps value was unlucky in having MAGIC_END's.
		} // else, perhaps a gkvlite file was stored as a value.
		atomic.AddInt64(&o.size, -1) // Roots were wrong, so keep scanning.
	}
	return nil
}
