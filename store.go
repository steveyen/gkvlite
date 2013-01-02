package gkvlite

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
)

// A persistable store holding collections of ordered keys & values.
// The persistence is append-only based on immutable, copy-on-write
// treaps for robustness.  This implementation is single-threaded, so
// users should serialize their accesses.
type Store struct {
	Coll     map[string]*Collection `json:"c"` // Exposed only for json'ification.
	file     *os.File
	size     int64
	readOnly bool
}

const VERSION = uint32(0)

var MAGIC_BEG []byte = []byte("0g1t2r")
var MAGIC_END []byte = []byte("3e4a5p")

// Use nil for file for in-memory-only (non-persistent) usage.
func NewStore(file *os.File) (res *Store, err error) {
	if file == nil { // Return a memory-only Store.
		return &Store{Coll: make(map[string]*Collection)}, nil
	}
	res = &Store{Coll: make(map[string]*Collection), file: file}
	if err := res.readRoots(); err == nil {
		return res, nil
	}
	return nil, err
}

// SetCollection() is used to create a named Collection, or to modify
// the KeyCompare function on an existing, named Collection.  The new
// Collection and any operations on it won't be reflected into
// persistence until you do a Flush().
func (s *Store) SetCollection(name string, compare KeyCompare) *Collection {
	if compare == nil {
		compare = bytes.Compare
	}
	if s.Coll[name] == nil {
		s.Coll[name] = &Collection{store: s, compare: compare}
	}
	s.Coll[name].compare = compare
	return s.Coll[name]
}

// Retrieves a named Collection.
func (s *Store) GetCollection(name string) *Collection {
	return s.Coll[name]
}

func (s *Store) GetCollectionNames() []string {
	res := make([]string, len(s.Coll))[:0]
	for name, _ := range s.Coll {
		res = append(res, name)
	}
	return res
}

// The Collection removal won't be reflected into persistence until
// you do a Flush().  Invoking RemoveCollection(x) and then
// SetCollection(x) is a fast way to empty a Collection.
func (s *Store) RemoveCollection(name string) {
	delete(s.Coll, name)
}

// Writes (appends) any unpersisted data to file.  As a
// greater-window-of-data-loss versus higher-performance tradeoff,
// consider having many mutations (Set()'s & Delete()'s) and then
// have a less occasional Flush() instead of Flush()'ing after every
// mutation.  Users may also wish to file.Sync() after a Flush() for
// extra data-loss protection.
func (s *Store) Flush() (err error) {
	if s.readOnly {
		return errors.New("readonly, so cannot Flush()")
	}
	if s.file == nil {
		return errors.New("no file / in-memory only, so cannot Flush()")
	}
	for _, t := range s.Coll {
		if err = t.store.flushItems(&t.root); err != nil {
			return err
		}
		if err = t.store.flushNodes(&t.root); err != nil {
			return err
		}
	}
	return s.writeRoots()
}

// Returns a non-persistable snapshot, including any mutations that
// have not been Flush()'ed to disk yet.  The snapshot has its Flush()
// disabled because the original store "owns" writes to the os.File.
// Caller should ensure that the returned snapshot store and the
// original store are used in "single-threaded" manner as they share
// internal memory structures.  On isolation: if you make updates to a
// snapshot store, those updates will not be seen by the original
// store; and vice-versa for mutations on the original store.  To
// persist the snapshot (and any updates on it) to a new file, use
// snapshot.CopyTo().
func (s *Store) Snapshot() (snapshot *Store) {
	res := &Store{
		Coll:     make(map[string]*Collection),
		file:     s.file,
		size:     s.size,
		readOnly: true,
	}
	for name, coll := range s.Coll {
		res.Coll[name] = &Collection{
			store:   res,
			compare: coll.compare,
			root:    coll.root,
		}
	}
	return res
}

// Copy all active collections and their items to a different file.
// If flushEvery > 0, then during the item copying, Flush() will be
// invoked at every flushEvery'th item and at the end of the item
// copying.  The copy will not include any old items or nodes so the
// copy should be more compact if flushEvery is relatively large.
func (s *Store) CopyTo(dstFile *os.File, flushEvery int) (res *Store, err error) {
	if dstStore, err := NewStore(dstFile); err == nil {
		for name, srcColl := range s.Coll {
			dstColl := dstStore.SetCollection(name, srcColl.compare)
			if minItem, err := srcColl.MinItem(true); err == nil && minItem != nil {
				numItems := 0
				var errCopyItem error = nil
				if err = srcColl.VisitItemsAscend(minItem.Key, true, func(i *Item) bool {
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
				}); err != nil {
					return nil, err
				}
				if errCopyItem != nil {
					return nil, errCopyItem
				}
			}
		}
		if flushEvery > 0 {
			if err = dstStore.Flush(); err != nil {
				return nil, err
			}
		}
		return dstStore, nil
	}
	return nil, err
}

// User-supplied key comparison func should return 0 if a == b,
// -1 if a < b, and +1 if a > b.
type KeyCompare func(a, b []byte) int

// A persistable collection of ordered key-values (Item's).
type Collection struct {
	store   *Store
	compare KeyCompare
	root    nodeLoc
}

// A persistable node.
type node struct {
	item        itemLoc
	left, right nodeLoc
}

// A persistable node and its persistence location.
type nodeLoc struct {
	loc  *ploc // Can be nil if node is dirty (not yet persisted).
	node *node // Can be nil if node is not fetched into memory yet.
}

var empty = &nodeLoc{} // Sentinel.

func (nloc *nodeLoc) isEmpty() bool {
	return nloc == nil || (nloc.loc.isEmpty() && nloc.node == nil)
}

func (nloc *nodeLoc) write(o *Store) error {
	if nloc != nil && nloc.loc.isEmpty() && nloc.node != nil {
		offset := o.size
		length := ploc_length + ploc_length + ploc_length
		b := bytes.NewBuffer(make([]byte, length)[:0])
		if err := nloc.node.item.loc.write(b); err != nil {
			return err
		}
		if err := nloc.node.left.loc.write(b); err != nil {
			return err
		}
		if err := nloc.node.right.loc.write(b); err != nil {
			return err
		}
		if _, err := o.file.WriteAt(b.Bytes()[:length], offset); err != nil {
			return err
		}
		o.size = offset + int64(length)
		nloc.loc = &ploc{Offset: offset, Length: uint32(length)}
	}
	return nil
}

func (nloc *nodeLoc) read(o *Store) (err error) {
	if nloc != nil && nloc.node == nil && !nloc.loc.isEmpty() {
		b := make([]byte, nloc.loc.Length)
		if _, err := o.file.ReadAt(b, nloc.loc.Offset); err != nil {
			return err
		}
		buf := bytes.NewBuffer(b)
		n := &node{}
		p := &ploc{}
		if p, err = p.read(buf); err != nil {
			return err
		}
		n.item.loc = p
		p = &ploc{}
		if p, err = p.read(buf); err != nil {
			return err
		}
		n.left.loc = p
		p = &ploc{}
		if p, err = p.read(buf); err != nil {
			return err
		}
		n.right.loc = p
		nloc.node = n
	}
	return nil
}

// A persistable item.
type Item struct {
	Key, Val []byte // Val may be nil if not fetched into memory yet.
	Priority int32  // Use rand.Int() for probabilistic balancing.
}

// A persistable item and its persistence location.
type itemLoc struct {
	loc  *ploc // Can be nil if item is dirty (not yet persisted).
	item *Item // Can be nil if item is not fetched into memory yet.
}

func (i *itemLoc) write(o *Store) error {
	// TODO: allow saving a db file as a value in another db file.
	if i.loc.isEmpty() {
		if i.item != nil {
			offset := o.size
			length := 4 + 4 + 4 + 4 + len(i.item.Key) + len(i.item.Val)
			b := bytes.NewBuffer(make([]byte, length)[:0])
			binary.Write(b, binary.BigEndian, uint32(length))
			binary.Write(b, binary.BigEndian, uint32(len(i.item.Key)))
			binary.Write(b, binary.BigEndian, uint32(len(i.item.Val)))
			binary.Write(b, binary.BigEndian, int32(i.item.Priority))
			b.Write(i.item.Key)
			b.Write(i.item.Val) // TODO: handle large values more efficiently.
			if _, err := o.file.WriteAt(b.Bytes()[:length], offset); err != nil {
				return err
			}
			o.size = offset + int64(length)
			i.loc = &ploc{Offset: offset, Length: uint32(length)}
		} else {
			return errors.New("flushItems saw node with no itemLoc and no item")
		}
	}
	return nil
}

func (i *itemLoc) read(o *Store, withValue bool) (err error) {
	if i != nil && (i.item == nil || (i.item.Val == nil && withValue)) && !i.loc.isEmpty() {
		b := make([]byte, i.loc.Length)
		if _, err := o.file.ReadAt(b, i.loc.Offset); err != nil {
			return err
		}
		buf := bytes.NewBuffer(b)
		item := &Item{}
		var length, keyLength, valLength uint32
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
		hdrLength := 4 + 4 + 4 + 4
		if length != uint32(hdrLength)+keyLength+valLength {
			return errors.New("mismatched itemLoc lengths")
		}
		item.Key = b[hdrLength : hdrLength+int(keyLength)]
		if withValue {
			item.Val = b[hdrLength+int(keyLength) : hdrLength+int(keyLength+valLength)]
		}
		i.item = item
	}
	return nil
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
func (t *Collection) GetItem(key []byte, withValue bool) (*Item, error) {
	n := &t.root
	err := n.read(t.store)
	for {
		if err != nil || n.isEmpty() {
			break
		}
		i := &n.node.item
		if err := i.read(t.store, false); err != nil {
			return nil, err
		}
		if i == nil || i.item == nil || i.item.Key == nil {
			return nil, errors.New("no item after item.read() in GetItem()")
		}
		c := t.compare(key, i.item.Key)
		if c < 0 {
			n = &n.node.left
			err = n.read(t.store)
		} else if c > 0 {
			n = &n.node.right
			err = n.read(t.store)
		} else {
			if withValue {
				if err := i.read(t.store, withValue); err != nil {
					return nil, err
				}
			}
			return i.item, nil
		}
	}
	return nil, err
}

// Retrieve a value by its key.  Returns nil if the item is not in the
// collection.  The returned value should be treated as immutable.
func (t *Collection) Get(key []byte) (val []byte, err error) {
	if i, err := t.GetItem(key, true); err == nil && i != nil {
		return i.Val, nil
	}
	return nil, err
}

// Replace or insert an item of a given key.
// A random item Priority (e.g., rand.Int()) will usually work well,
// but advanced users may consider using non-random item priorities
// at the risk of unbalancing the lookup tree.
func (t *Collection) SetItem(item *Item) (err error) {
	if r, err := t.store.union(t, &t.root,
		&nodeLoc{node: &node{item: itemLoc{item: &Item{
			Key:      item.Key,
			Val:      item.Val,
			Priority: item.Priority,
		}}}}); err == nil {
		t.root = *r
	}
	return err
}

// Replace or insert an item of a given key.
func (t *Collection) Set(key []byte, val []byte) error {
	return t.SetItem(&Item{Key: key, Val: val, Priority: int32(rand.Int())})
}

// Deletes an item of a given key.
func (t *Collection) Delete(key []byte) (err error) {
	if left, _, right, err := t.store.split(t, &t.root, key); err == nil {
		if r, err := t.store.join(left, right); err == nil {
			t.root = *r
		}
	}
	return err
}

// Retrieves the item with the "smallest" key.
// The returned item should be treated as immutable.
func (t *Collection) MinItem(withValue bool) (*Item, error) {
	return t.store.edge(t, withValue, func(n *node) *nodeLoc { return &n.left })
}

// Retrieves the item with the "largest" key.
// The returned item should be treated as immutable.
func (t *Collection) MaxItem(withValue bool) (*Item, error) {
	return t.store.edge(t, withValue, func(n *node) *nodeLoc { return &n.right })
}

type ItemVisitor func(i *Item) bool

// Visit items greater-than-or-equal to the target key.
func (t *Collection) VisitItemsAscend(target []byte, withValue bool, visitor ItemVisitor) error {
	_, err := t.store.visitAscendNode(t, &t.root, target, withValue, visitor)
	return err
}

// Returns JSON representation of root node file location.
func (t *Collection) MarshalJSON() ([]byte, error) {
	if t.root.loc.isEmpty() {
		return json.Marshal(ploc_empty)
	}
	return json.Marshal(t.root.loc)
}

// Unmarshals JSON representation of root node file location.
func (t *Collection) UnmarshalJSON(d []byte) (err error) {
	p := ploc{}
	if err := json.Unmarshal(d, &p); err == nil {
		t.root.loc = &p
	}
	return err
}

func (o *Store) flushItems(nloc *nodeLoc) (err error) {
	if nloc == nil || !nloc.loc.isEmpty() || nloc.node == nil {
		return nil // Flush only unpersisted items of non-empty, unpersisted nodes.
	}
	if err = o.flushItems(&nloc.node.left); err != nil {
		return err
	}
	if err = nloc.node.item.write(o); err != nil { // Write items in key order.
		return err
	}
	return o.flushItems(&nloc.node.right)
}

func (o *Store) flushNodes(nloc *nodeLoc) (err error) {
	if nloc == nil || !nloc.loc.isEmpty() || nloc.node == nil {
		return nil // Flush only non-empty, unpersisted nodes.
	}
	if err = o.flushNodes(&nloc.node.left); err != nil {
		return err
	}
	if err = o.flushNodes(&nloc.node.right); err != nil {
		return err
	}
	return nloc.write(o) // Write nodes in children-first order.
}

func (o *Store) union(t *Collection, this *nodeLoc, that *nodeLoc) (res *nodeLoc, err error) {
	if err := this.read(o); err == nil {
		if err := that.read(o); err == nil {
			if this.isEmpty() {
				return that, nil
			}
			if that.isEmpty() {
				return this, nil
			}
			thisItem := &this.node.item
			if err := thisItem.read(o, false); err == nil {
				thatItem := &that.node.item
				if err := thatItem.read(o, false); err == nil {
					if thisItem.item.Priority > thatItem.item.Priority {
						left, middle, right, err := o.split(t, that, thisItem.item.Key)
						if err == nil {
							newLeft, err := o.union(t, &this.node.left, left)
							if err == nil {
								newRight, err := o.union(t, &this.node.right, right)
								if err == nil {
									if middle.isEmpty() {
										return &nodeLoc{node: &node{
											item:  *thisItem,
											left:  *newLeft,
											right: *newRight,
										}}, nil
									} else {
										return &nodeLoc{node: &node{
											item:  middle.node.item,
											left:  *newLeft,
											right: *newRight,
										}}, nil
									}
								}
							}
						}
					} else {
						// We don't use middle because the "that" node has precendence.
						left, _, right, err := o.split(t, this, thatItem.item.Key)
						if err == nil {
							newLeft, err := o.union(t, left, &that.node.left)
							if err == nil {
								newRight, err := o.union(t, right, &that.node.right)
								if err == nil {
									return &nodeLoc{node: &node{
										item:  *thatItem,
										left:  *newLeft,
										right: *newRight,
									}}, nil
								}
							}
						}
					}
				}
			}
		}
	}
	return empty, err
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
	nItem := &n.node.item
	if err := nItem.read(o, false); err != nil {
		return empty, empty, empty, err
	}

	c := t.compare(s, nItem.item.Key)
	if c == 0 {
		return &n.node.left, n, &n.node.right, nil
	}

	if c < 0 {
		left, middle, right, err := o.split(t, &n.node.left, s)
		if err != nil {
			return empty, empty, empty, err
		}
		return left, middle, &nodeLoc{node: &node{
			item:  *nItem,
			left:  *right,
			right: n.node.right,
		}}, nil
	}

	left, middle, right, err := o.split(t, &n.node.right, s)
	if err != nil {
		return empty, empty, empty, err
	}
	return &nodeLoc{node: &node{
		item:  *nItem,
		left:  n.node.left,
		right: *left,
	}}, middle, right, nil
}

// All the keys from this should be < keys from that.
func (o *Store) join(this *nodeLoc, that *nodeLoc) (res *nodeLoc, err error) {
	if err := this.read(o); err == nil {
		if err := that.read(o); err == nil {
			if this.isEmpty() {
				return that, nil
			}
			if that.isEmpty() {
				return this, nil
			}
			thisItem := &this.node.item
			if err := thisItem.read(o, false); err == nil {
				thatItem := &that.node.item
				if err := thatItem.read(o, false); err == nil {
					if thisItem.item.Priority > thatItem.item.Priority {
						if newRight, err := o.join(&this.node.right, that); err == nil {
							return &nodeLoc{node: &node{
								item:  *thisItem,
								left:  this.node.left,
								right: *newRight,
							}}, nil
						}
					} else {
						if newLeft, err := o.join(this, &that.node.left); err == nil {
							return &nodeLoc{node: &node{
								item:  *thatItem,
								left:  *newLeft,
								right: that.node.right,
							}}, nil
						}
					}
				}
			}
		}
	}
	return empty, err
}

func (o *Store) edge(t *Collection, withValue bool, cfn func(*node) *nodeLoc) (
	res *Item, err error) {
	n := &t.root
	if err = n.read(o); err != nil || n.isEmpty() {
		return nil, err
	}
	for {
		child := cfn(n.node)
		if err := child.read(o); err != nil {
			return nil, err
		}
		if child.isEmpty() {
			i := &n.node.item
			if err = i.read(o, withValue); err == nil {
				return i.item, nil
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
	nItem := &n.node.item
	if err := nItem.read(o, false); err != nil {
		return false, err
	}
	if t.compare(target, nItem.item.Key) <= 0 {
		keepGoing, err := o.visitAscendNode(t, &n.node.left, target, withValue, visitor)
		if err != nil || !keepGoing {
			return false, err
		}
		if err := nItem.read(o, withValue); err != nil {
			return false, err
		}
		if !visitor(nItem.item) {
			return false, nil
		}
	}
	return o.visitAscendNode(t, &n.node.right, target, withValue, visitor)
}

func (o *Store) writeRoots() (err error) {
	if sJSON, err := json.Marshal(o); err == nil {
		offset := o.size
		length := 2*len(MAGIC_BEG) + 4 + 4 + len(sJSON) + 8 + 2*len(MAGIC_END)
		b := bytes.NewBuffer(make([]byte, length)[:0])
		b.Write(MAGIC_BEG)
		b.Write(MAGIC_BEG)
		binary.Write(b, binary.BigEndian, uint32(VERSION))
		binary.Write(b, binary.BigEndian, uint32(length))
		b.Write(sJSON)
		binary.Write(b, binary.BigEndian, int64(offset))
		b.Write(MAGIC_END)
		b.Write(MAGIC_END)
		if _, err := o.file.WriteAt(b.Bytes()[:length], offset); err == nil {
			o.size = offset + int64(length)
		}
	}
	return err
}

func (o *Store) readRoots() error {
	finfo, err := o.file.Stat()
	if err == nil {
		o.size = finfo.Size()
		if o.size > 0 {
			endBuff := make([]byte, 8+2*len(MAGIC_END))
			minSize := int64(2*len(MAGIC_BEG) + 4 + 4 + len(endBuff))
			for {
				for { // Scan backwards for MAGIC_END.
					if o.size <= minSize {
						return errors.New("couldn't find roots; file corrupted or wrong?")
					}
					if _, err := o.file.ReadAt(endBuff,
						o.size-int64(len(endBuff))); err != nil {
						return err
					}
					if bytes.Equal(MAGIC_END, endBuff[8:8+len(MAGIC_END)]) &&
						bytes.Equal(MAGIC_END, endBuff[8+len(MAGIC_END):]) {
						break
					}
					o.size = o.size - 1 // TODO: optimizations to scan backwards faster.
				}
				// Read and check the roots.
				var offset int64
				err = binary.Read(bytes.NewBuffer(endBuff), binary.BigEndian, &offset)
				if err != nil {
					return err
				}
				if offset >= 0 && offset < o.size-int64(minSize) {
					data := make([]byte, o.size-offset-int64(len(endBuff)))
					if _, err := o.file.ReadAt(data, offset); err != nil {
						return err
					}
					if bytes.Equal(MAGIC_BEG, data[:len(MAGIC_BEG)]) &&
						bytes.Equal(MAGIC_BEG, data[len(MAGIC_BEG):2*len(MAGIC_BEG)]) {
						var version, length uint32
						b := bytes.NewBuffer(data[2*len(MAGIC_BEG):])
						if err = binary.Read(b, binary.BigEndian, &version); err != nil {
							return err
						}
						if err = binary.Read(b, binary.BigEndian, &length); err != nil {
							return err
						}
						if version != VERSION {
							return fmt.Errorf("version mismatch: "+
								"current version: %v != found version: %v",
								VERSION, version)
						}
						if length != uint32(o.size-offset) {
							return fmt.Errorf("length mismatch: "+
								"wanted length: %v != found length: %v",
								uint32(o.size-offset), length)
						}
						m := &Store{}
						if err = json.Unmarshal(data[2*len(MAGIC_BEG)+4+4:], &m); err != nil {
							return err
						}
						o.Coll = m.Coll
						for _, t := range o.Coll {
							t.store = o
							t.compare = bytes.Compare
						}
						return nil
					}
				}
				o.size = o.size - 1 // Roots were wrong, so keep scanning.
			}
		}
	}
	return err
}
