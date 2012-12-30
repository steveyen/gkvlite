package gtreap

import (
	"errors"
	"os"
)

// A persistent store holding collections of ordered keys & values.
// The persistence is append-only based on immutable, copy-on-write
// treaps.  This implementation is single-threaded, so users should
// serialize their accesses.
//
// TODO: use atomic.CAS and unsafe.Pointers for safe snapshot'ability.
// TODO: allow read-only snapshots.
//
type Store struct {
	coll map[string]*PTreap
	file *os.File
}

func NewStore(file *os.File) (*Store, error) {
	if file == nil { // Return a memory-only Store.
		return &Store{coll: make(map[string]*PTreap)}, nil
	}
	return nil, errors.New("not implemented yet")
}

func (s *Store) AddCollection(name string, compare KeyCompare) *PTreap {
	if s.coll[name] == nil {
		s.coll[name] = &PTreap{store: s, compare: compare}
	}
	return s.coll[name]
}

func (s *Store) GetCollection(name string) *PTreap {
	return s.coll[name]
}

func (s *Store) RemoveCollection(name string) {
	delete(s.coll, name)
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

// A persisted node and its persistence location.
type pnodeLoc struct {
	loc  *ploc  // Can be nil if node is dirty (not yet persisted).
	node *pnode // Can be nil if node is not fetched into memory yet.
}

func (nloc *pnodeLoc) isEmpty() bool {
	return nloc == nil || (nloc.loc == nil && nloc.node == nil)
}

var empty = &pnodeLoc{}

// A persisted node.
type pnode struct {
	item        PItemLoc
	left, right pnodeLoc
}

// A persisted item.
type PItem struct {
	Key, Val []byte // Val may be nil if not fetched into memory yet.
	Priority int32
}

// A persisted item and its persistence location.
type PItemLoc struct {
	loc  *ploc  // Can be nil if item is dirty (not yet persisted).
	item *PItem // Can be nil if item is not fetched into memory yet.
}

// Offset/location of persisted range of bytes.
type ploc struct {
	offset int64  // Usable for os.Seek/ReadAt/WriteAt() at file offset 0.
	length uint32 // Number of bytes.
}

func (t *PTreap) Get(key []byte, withValue bool) (*PItem, error) {
	n, err := t.store.loadNodeLoc(&t.root)
	for {
		if err != nil || n.isEmpty() {
			break
		}
		i, err := t.store.loadMetaItemLoc(&n.node.item)
		if err != nil {
			return nil, err
		}
		if i == nil || i.item == nil || i.item.Key == nil {
			panic("no item after loadMetaItemLoc() in get()")
		}
		c := t.compare(key, i.item.Key)
		if c < 0 {
			n, err = t.store.loadNodeLoc(&n.node.left)
		} else if c > 0 {
			n, err = t.store.loadNodeLoc(&n.node.right)
		} else {
			if withValue {
				t.store.loadItemLoc(i)
			}
			return i.item, nil
		}
	}
	return nil, err
}

// Replace or insert an item of a given key.
func (t *PTreap) Upsert(item *PItem) error {
	r, err := t.store.union(t, &t.root,
		&pnodeLoc{node: &pnode{item: PItemLoc{item: &PItem{
			Key:      item.Key,
			Val:      item.Val,
			Priority: item.Priority,
		}}}})
	if err == nil {
		t.root = *r
	}
	return err
}

func (t *PTreap) Delete(key []byte) error {
	left, _, right, err := t.store.split(t, &t.root, key)
	if err == nil {
		r, err := t.store.join(left, right)
		if err == nil {
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

func (o *Store) loadNodeLoc(nloc *pnodeLoc) (*pnodeLoc, error) {
	if nloc != nil && nloc.node == nil && nloc.loc != nil {
		// TODO.
	}
	return nloc, nil
}

func (o *Store) loadItemLoc(iloc *PItemLoc) (*PItemLoc, error) {
	if iloc != nil && iloc.item == nil && iloc.loc != nil {
		// TODO.
	}
	return iloc, nil
}

func (o *Store) loadMetaItemLoc(iloc *PItemLoc) (*PItemLoc, error) {
	if iloc != nil && iloc.item == nil && iloc.loc != nil {
		// TODO.
	}
	return iloc, nil
}

func (o *Store) union(t *PTreap, this *pnodeLoc, that *pnodeLoc) (*pnodeLoc, error) {
	thisNode, err := o.loadNodeLoc(this)
	if err != nil {
		return empty, err
	}
	thatNode, err := o.loadNodeLoc(that)
	if err != nil {
		return empty, err
	}

	if thisNode.isEmpty() {
		thisNode = empty
	}
	if thatNode.isEmpty() {
		thatNode = empty
	}

	if thisNode.isEmpty() {
		return that, nil
	}
	if thatNode.isEmpty() {
		return this, nil
	}

	thisItem, err := o.loadMetaItemLoc(&thisNode.node.item)
	if err != nil {
		return empty, err
	}
	thatItem, err := o.loadMetaItemLoc(&thatNode.node.item)
	if err != nil {
		return empty, err
	}

	if thisItem.item.Priority > thatItem.item.Priority {
		left, middle, right, err := o.split(t, that, thisItem.item.Key)
		if err != nil {
			return empty, err
		}
		if middle.isEmpty() {
			newLeft, err := o.union(t, &thisNode.node.left, left)
			if err != nil {
				return empty, err
			}
			newRight, err := o.union(t, &thisNode.node.right, right)
			if err != nil {
				return nil, err
			}
			return &pnodeLoc{node: &pnode{
				item:  *thisItem,
				left:  *newLeft,
				right: *newRight,
			}}, nil
		}

		newLeft, err := o.union(t, &thisNode.node.left, left)
		if err != nil {
			return empty, err
		}
		newRight, err := o.union(t, &thisNode.node.right, right)
		if err != nil {
			return empty, err
		}
		return &pnodeLoc{node: &pnode{
			item:  middle.node.item,
			left:  *newLeft,
			right: *newRight,
		}}, nil
	}

	// We don't use middle because the "that" node has precendence.
	left, _, right, err := o.split(t, this, thatItem.item.Key)
	if err != nil {
		return empty, err
	}
	newLeft, err := o.union(t, left, &thatNode.node.left)
	if err != nil {
		return empty, err
	}
	newRight, err := o.union(t, right, &thatNode.node.right)
	if err != nil {
		return empty, err
	}
	return &pnodeLoc{node: &pnode{
		item:  *thatItem,
		left:  *newLeft,
		right: *newRight,
	}}, nil
}

// Splits a treap into two treaps based on a split key "s".  The
// result is (left, middle, right), where left treap has keys < s,
// right treap has keys > s, and middle is either...
// * empty/nil - meaning key s was not in the original treap.
// * non-empty - returning the pnodeLoc that had item s.
func (o *Store) split(t *PTreap, n *pnodeLoc, s []byte) (
	*pnodeLoc, *pnodeLoc, *pnodeLoc, error) {
	nNode, err := o.loadNodeLoc(n)
	if err != nil || nNode.isEmpty() {
		return empty, empty, empty, err
	}
	nItem, err := o.loadMetaItemLoc(&nNode.node.item)
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

// All the keys from this are < keys from that.
func (o *Store) join(this *pnodeLoc, that *pnodeLoc) (*pnodeLoc, error) {
	thisNode, err := o.loadNodeLoc(this)
	if err != nil {
		return empty, err
	}
	thatNode, err := o.loadNodeLoc(that)
	if err != nil {
		return empty, err
	}

	if thisNode.isEmpty() {
		thisNode = empty
	}
	if thatNode.isEmpty() {
		thatNode = empty
	}

	if thisNode.isEmpty() {
		return that, nil
	}
	if thatNode.isEmpty() {
		return this, nil
	}

	thisItem, err := o.loadMetaItemLoc(&thisNode.node.item)
	if err != nil {
		return empty, err
	}
	thatItem, err := o.loadMetaItemLoc(&thatNode.node.item)
	if err != nil {
		return empty, err
	}

	if thisItem.item.Priority > thatItem.item.Priority {
		newRight, err := o.join(&thisNode.node.right, that)
		if err != nil {
			return empty, err
		}
		return &pnodeLoc{node: &pnode{
			item:  *thisItem,
			left:  thisNode.node.left,
			right: *newRight,
		}}, nil
	}

	newLeft, err := o.join(this, &thatNode.node.left)
	if err != nil {
		return empty, err
	}
	return &pnodeLoc{node: &pnode{
		item:  *thatItem,
		left:  *newLeft,
		right: thatNode.node.right,
	}}, nil
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
			i, err := o.loadMetaItemLoc(&n.node.item)
			if err == nil {
				if withValue {
					i, err = o.loadItemLoc(i)
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
	nItem, err := o.loadMetaItemLoc(&nNode.node.item)
	if err != nil {
		return false, err
	}
	if t.compare(target, nItem.item.Key) <= 0 {
		keepGoing, err := o.visitAscendNode(t, &nNode.node.left, target, withValue, visitor)
		if err != nil || !keepGoing {
			return false, err
		}
		if withValue {
			nItem, err = o.loadItemLoc(nItem)
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
