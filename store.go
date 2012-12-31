package gtreap

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// A persistent store holding collections of ordered keys & values.
// The persistence is append-only based on immutable, copy-on-write
// treaps.  This implementation is single-threaded, so users should
// serialize their accesses.
//
// TODO: use atomic.CAS and unsafe.Pointers for safe snapshot'ability.
// TODO: allow read-only snapshots.
// TODO: saving gtreap.Store data files into a gtreap.Store data file.
//
type Store struct {
	Coll map[string]*PTreap `json:"c"` // Exposed only for json'ification.
	file *os.File
	size int64
}

const VERSION = uint32(0)

var MAGIC_BEG []byte = []byte("0g1t2r")
var MAGIC_END []byte = []byte("3e4a5p")

func NewStore(file *os.File) (res *Store, err error) {
	if file == nil { // Return a memory-only Store.
		return &Store{Coll: make(map[string]*PTreap)}, nil
	}
	res = &Store{Coll: make(map[string]*PTreap), file: file}
	if err := res.readRoots(); err == nil {
		return res, nil
	}
	return nil, err
}

func (s *Store) SetCollection(name string, compare KeyCompare) *PTreap {
	if s.Coll[name] == nil {
		s.Coll[name] = &PTreap{store: s, compare: compare}
	}
	s.Coll[name].compare = compare
	return s.Coll[name]
}

func (s *Store) GetCollection(name string) *PTreap {
	return s.Coll[name]
}

func (s *Store) GetCollectionNames() []string {
	res := make([]string, len(s.Coll))[:0]
	for name, _ := range s.Coll {
		res = append(res, name)
	}
	return res
}

func (s *Store) RemoveCollection(name string) {
	delete(s.Coll, name)
}

// Writes any unpersisted data to file.  Users might also file.Sync()
// afterwards for extra data-loss protection.
func (s *Store) Flush() (err error) {
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

// User-supplied key comparison func should return 0 if a == b,
// -1 if a < b, and +1 if a > b.
type KeyCompare func(a, b []byte) int

// A persisted treap.
type PTreap struct {
	store   *Store
	compare KeyCompare
	root    pnodeLoc
}

// A persisted node.
type pnode struct {
	item        pitemLoc
	left, right pnodeLoc
}

// A persisted node and its persistence location.
type pnodeLoc struct {
	loc  *ploc  // Can be nil if node is dirty (not yet persisted).
	node *pnode // Can be nil if node is not fetched into memory yet.
}

func (nloc *pnodeLoc) isEmpty() bool {
	return nloc == nil || (nloc.loc.isEmpty() && nloc.node == nil)
}

func (nloc *pnodeLoc) write(o *Store) error {
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

func (nloc *pnodeLoc) read(o *Store) (err error) {
	if nloc != nil && nloc.node == nil && !nloc.loc.isEmpty() {
		b := make([]byte, nloc.loc.Length)
		if _, err := o.file.ReadAt(b, nloc.loc.Offset); err != nil {
			return err
		}
		buf := bytes.NewBuffer(b)
		n := &pnode{}
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

var empty = &pnodeLoc{}

// A persisted item.
type PItem struct {
	Key, Val []byte // Val may be nil if not fetched into memory yet.
	Priority int32
}

// A persisted item and its persistence location.
type pitemLoc struct {
	loc  *ploc  // Can be nil if item is dirty (not yet persisted).
	item *PItem // Can be nil if item is not fetched into memory yet.
}

func (i *pitemLoc) write(o *Store) error {
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
			b.Write(i.item.Val)
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

func (i *pitemLoc) read(o *Store, withValue bool) (err error) {
	if i != nil && i.item == nil && !i.loc.isEmpty() {
		b := make([]byte, i.loc.Length)
		if _, err := o.file.ReadAt(b, i.loc.Offset); err != nil {
			return err
		}
		buf := bytes.NewBuffer(b)
		item := &PItem{}
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
			return errors.New("mismatched pitemLoc lengths")
		}
		item.Key = b[hdrLength : hdrLength+int(keyLength)]
		item.Val = b[hdrLength+int(keyLength) : hdrLength+int(keyLength+valLength)]
		i.item = item
	}
	return nil
}

// Offset/location of persisted range of bytes.
type ploc struct {
	Offset int64  `json:"o"` // Usable for os.ReadAt/WriteAt() at file offset 0.
	Length uint32 `json:"l"` // Number of bytes.
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

func (p *ploc) isEmpty() bool {
	return p == nil || (p.Offset == int64(0) && p.Length == uint32(0))
}

const ploc_length int = 8 + 4

var ploc_empty *ploc = &ploc{}

func (t *PTreap) Get(key []byte, withValue bool) (*PItem, error) {
	n, err := t.store.loadNodeLoc(&t.root)
	for {
		if err != nil || n.isEmpty() {
			break
		}
		i, err := t.store.loadItemLoc(&n.node.item, false)
		if err != nil {
			return nil, err
		}
		if i == nil || i.item == nil || i.item.Key == nil {
			return nil, errors.New("no item after loadMetaItemLoc() in get()")
		}
		c := t.compare(key, i.item.Key)
		if c < 0 {
			n, err = t.store.loadNodeLoc(&n.node.left)
		} else if c > 0 {
			n, err = t.store.loadNodeLoc(&n.node.right)
		} else {
			if withValue {
				t.store.loadItemLoc(i, true)
			}
			return i.item, nil
		}
	}
	return nil, err
}

// Replace or insert an item of a given key.
func (t *PTreap) Upsert(item *PItem) (err error) {
	if r, err := t.store.union(t, &t.root,
		&pnodeLoc{node: &pnode{item: pitemLoc{item: &PItem{
			Key:      item.Key,
			Val:      item.Val,
			Priority: item.Priority,
		}}}}); err == nil {
		t.root = *r
	}
	return err
}

func (t *PTreap) Delete(key []byte) (err error) {
	if left, _, right, err := t.store.split(t, &t.root, key); err == nil {
		if r, err := t.store.join(left, right); err == nil {
			t.root = *r
		}
	}
	return err
}

func (t *PTreap) Min(withValue bool) (*PItem, error) {
	return t.store.edge(t, withValue, func(n *pnode) *pnodeLoc { return &n.left })
}

func (t *PTreap) Max(withValue bool) (*PItem, error) {
	return t.store.edge(t, withValue, func(n *pnode) *pnodeLoc { return &n.right })
}

type PItemVisitor func(i *PItem) bool

// Visit items greater-than-or-equal to the target.
func (t *PTreap) VisitAscend(target []byte, withValue bool, visitor PItemVisitor) error {
	_, err := t.store.visitAscendNode(t, &t.root, target, withValue, visitor)
	return err
}

func (t *PTreap) MarshalJSON() ([]byte, error) {
	if t.root.loc.isEmpty() {
		return json.Marshal(ploc_empty)
	}
	return json.Marshal(t.root.loc)
}

func (t *PTreap) UnmarshalJSON(d []byte) (err error) {
	p := ploc{}
	if err := json.Unmarshal(d, &p); err == nil {
		t.root.loc = &p
	}
	return err
}

func (o *Store) flushItems(nloc *pnodeLoc) (err error) {
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

func (o *Store) flushNodes(nloc *pnodeLoc) (err error) {
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

func (o *Store) loadNodeLoc(nloc *pnodeLoc) (*pnodeLoc, error) {
	if nloc != nil && nloc.node == nil && !nloc.loc.isEmpty() {
		if err := nloc.read(o); err != nil {
			return nil, err
		}
	}
	return nloc, nil
}

func (o *Store) loadItemLoc(iloc *pitemLoc, withValue bool) (*pitemLoc, error) {
	if iloc != nil && iloc.item == nil && !iloc.loc.isEmpty() {
		if err := iloc.read(o, withValue); err != nil {
			return nil, err
		}
	}
	return iloc, nil
}

func (o *Store) union(t *PTreap, this *pnodeLoc, that *pnodeLoc) (res *pnodeLoc, err error) {
	if thisNode, err := o.loadNodeLoc(this); err == nil {
		if thatNode, err := o.loadNodeLoc(that); err == nil {
			if thisNode.isEmpty() {
				return thatNode, nil
			}
			if thatNode.isEmpty() {
				return thisNode, nil
			}
			if thisItem, err := o.loadItemLoc(&thisNode.node.item, false); err == nil {
				if thatItem, err := o.loadItemLoc(&thatNode.node.item, false); err == nil {
					if thisItem.item.Priority > thatItem.item.Priority {
						left, middle, right, err := o.split(t, that, thisItem.item.Key)
						if err == nil {
							if middle.isEmpty() {
								newLeft, err := o.union(t, &thisNode.node.left, left)
								if err == nil {
									newRight, err := o.union(t, &thisNode.node.right, right)
									if err == nil {
										return &pnodeLoc{node: &pnode{
											item:  *thisItem,
											left:  *newLeft,
											right: *newRight,
										}}, nil
									}
								}
							} else {
								newLeft, err := o.union(t, &thisNode.node.left, left)
								if err == nil {
									newRight, err := o.union(t, &thisNode.node.right, right)
									if err == nil {
										return &pnodeLoc{node: &pnode{
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
							newLeft, err := o.union(t, left, &thatNode.node.left)
							if err == nil {
								newRight, err := o.union(t, right, &thatNode.node.right)
								if err == nil {
									return &pnodeLoc{node: &pnode{
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
// * non-empty - returning the original pnodeLoc/item that had key s.
func (o *Store) split(t *PTreap, n *pnodeLoc, s []byte) (
	*pnodeLoc, *pnodeLoc, *pnodeLoc, error) {
	nNode, err := o.loadNodeLoc(n)
	if err != nil || nNode.isEmpty() {
		return empty, empty, empty, err
	}
	nItem, err := o.loadItemLoc(&nNode.node.item, false)
	if err != nil {
		return empty, empty, empty, err
	}

	c := t.compare(s, nItem.item.Key)
	if c == 0 {
		return &nNode.node.left, n, &nNode.node.right, nil
	}

	if c < 0 {
		left, middle, right, err := o.split(t, &nNode.node.left, s)
		if err != nil {
			return empty, empty, empty, err
		}
		return left, middle, &pnodeLoc{node: &pnode{
			item:  *nItem,
			left:  *right,
			right: nNode.node.right,
		}}, nil
	}

	left, middle, right, err := o.split(t, &nNode.node.right, s)
	if err != nil {
		return empty, empty, empty, err
	}
	return &pnodeLoc{node: &pnode{
		item:  *nItem,
		left:  nNode.node.left,
		right: *left,
	}}, middle, right, nil
}

// All the keys from this should be < keys from that.
func (o *Store) join(this *pnodeLoc, that *pnodeLoc) (res *pnodeLoc, err error) {
	if thisNode, err := o.loadNodeLoc(this); err == nil {
		if thatNode, err := o.loadNodeLoc(that); err == nil {
			if thisNode.isEmpty() {
				return thatNode, nil
			}
			if thatNode.isEmpty() {
				return thisNode, nil
			}
			if thisItem, err := o.loadItemLoc(&thisNode.node.item, false); err == nil {
				if thatItem, err := o.loadItemLoc(&thatNode.node.item, false); err == nil {
					if thisItem.item.Priority > thatItem.item.Priority {
						if newRight, err := o.join(&thisNode.node.right, that); err == nil {
							return &pnodeLoc{node: &pnode{
								item:  *thisItem,
								left:  thisNode.node.left,
								right: *newRight,
							}}, nil
						}
					} else {
						if newLeft, err := o.join(this, &thatNode.node.left); err == nil {
							return &pnodeLoc{node: &pnode{
								item:  *thatItem,
								left:  *newLeft,
								right: thatNode.node.right,
							}}, nil
						}
					}
				}
			}
		}
	}
	return empty, err
}

func (o *Store) edge(t *PTreap, withValue bool, cfn func(*pnode) *pnodeLoc) (
	*PItem, error) {
	n, err := o.loadNodeLoc(&t.root)
	if err != nil || n.isEmpty() {
		return nil, err
	}
	for {
		child, err := o.loadNodeLoc(cfn(n.node))
		if err != nil {
			return nil, err
		}
		if child.isEmpty() {
			if i, err := o.loadItemLoc(&n.node.item, false); err == nil {
				if withValue {
					i, err = o.loadItemLoc(i, true)
				}
				if err == nil {
					return i.item, nil
				}
			}
			return nil, err
		}
		n = child
	}
	return nil, nil
}

func (o *Store) visitAscendNode(t *PTreap, n *pnodeLoc, target []byte,
	withValue bool, visitor PItemVisitor) (bool, error) {
	nNode, err := o.loadNodeLoc(n)
	if err != nil {
		return false, err
	}
	if nNode.isEmpty() {
		return true, nil
	}
	nItem, err := o.loadItemLoc(&nNode.node.item, false)
	if err != nil {
		return false, err
	}
	if t.compare(target, nItem.item.Key) <= 0 {
		keepGoing, err := o.visitAscendNode(t, &nNode.node.left, target, withValue, visitor)
		if err != nil || !keepGoing {
			return false, err
		}
		if withValue {
			nItem, err = o.loadItemLoc(nItem, true)
			if err != nil {
				return false, err
			}
		}
		if !visitor(nItem.item) {
			return false, nil
		}
	}
	return o.visitAscendNode(t, &nNode.node.right, target, withValue, visitor)
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
				for { // Scan for MAGIC_END.
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
					o.size = o.size - 1 // TODO: optimizations to skip backwards faster.
				}
				// Read and check the roots.
				var offset int64
				binary.Read(bytes.NewBuffer(endBuff), binary.BigEndian, &offset)
				if offset >= 0 && offset < o.size-int64(minSize) {
					data := make([]byte, o.size-offset-int64(len(endBuff)))
					if _, err := o.file.ReadAt(data, offset); err != nil {
						return err
					}
					if bytes.Equal(MAGIC_BEG, data[:len(MAGIC_BEG)]) &&
						bytes.Equal(MAGIC_BEG, data[len(MAGIC_BEG):2*len(MAGIC_BEG)]) {
						var version, length uint32
						b := bytes.NewBuffer(data[2*len(MAGIC_BEG):])
						binary.Read(b, binary.BigEndian, &version)
						binary.Read(b, binary.BigEndian, &length)
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
						err := json.Unmarshal(data[2*len(MAGIC_BEG)+4+4:], &m)
						if err != nil {
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
