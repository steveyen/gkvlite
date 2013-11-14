package gkvlite

import (
	"fmt"
)

// The core algorithms for treaps are straightforward.  However, that
// algorithmic simplicity is obscured by the additional useful
// features, such as persistence, garbage-avoidance, stats tracking,
// and error handling.  For a simple, memory-only implementation of
// the union/split/join treap algorithms that may be easier to
// understand, see:
// https://github.com/steveyen/gtreap/blob/master/treap.go

// Memory management rules: the union/split/join functions will not
// free their input nodeLoc's, but are responsible instead for
// invoking markReclaimable() on the directly pointed-at input nodes.
// The union/split/join functions must copy the input nodeLoc's if
// they wish to keep the nodeLoc's data.
//
// The caller is responsible for freeing the returned nodeLoc's and
// (if appropriate) the input nodeLoc's.  The caller also takes
// responsibility for markReclaimable() on returned output nodes.

// Returns a treap that is the union of this treap and that treap.
func (o *Store) union(t *Collection, this *nodeLoc, that *nodeLoc,
	reclaimMark *node) (
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
		return t.mkNodeLoc(nil).Copy(that), nil
	}
	if that.isEmpty() || thatNode == nil {
		return t.mkNodeLoc(nil).Copy(this), nil
	}
	thisItemLoc := &thisNode.item
	thisItem, err := thisItemLoc.read(t, false)
	if err != nil {
		return empty_nodeLoc, err
	}
	thatItemLoc := &thatNode.item
	thatItem, err := thatItemLoc.read(t, false)
	if err != nil {
		return empty_nodeLoc, err
	}
	if thisItem.Priority > thatItem.Priority {
		left, middle, right, err :=
			o.split(t, that, thisItem.Key, reclaimMark)
		if err != nil {
			return empty_nodeLoc, err
		}
		newLeft, err := o.union(t, &thisNode.left, left, reclaimMark)
		if err != nil {
			return empty_nodeLoc, err
		}
		newRight, err := o.union(t, &thisNode.right, right, reclaimMark)
		if err != nil {
			return empty_nodeLoc, err
		}
		leftNum, leftBytes, rightNum, rightBytes, err :=
			numInfo(o, newLeft, newRight)
		if err != nil {
			return empty_nodeLoc, err
		}
		var middleNode *node
		if !middle.isEmpty() {
			middleNode, err = middle.read(o)
			if err != nil {
				return empty_nodeLoc, err
			}
			middleItemLoc := &middleNode.item
			res = t.mkNodeLoc(t.mkNode(middleItemLoc, newLeft, newRight,
				leftNum+rightNum+1,
				leftBytes+rightBytes+uint64(middleItemLoc.NumBytes(t))))
		} else {
			res = t.mkNodeLoc(t.mkNode(thisItemLoc, newLeft, newRight,
				leftNum+rightNum+1,
				leftBytes+rightBytes+uint64(thisItemLoc.NumBytes(t))))
		}
		t.freeNodeLoc(left)
		t.freeNodeLoc(right)
		t.freeNodeLoc(middle)
		t.freeNodeLoc(newLeft)
		t.freeNodeLoc(newRight)
		t.markReclaimable(thisNode, reclaimMark)
		t.markReclaimable(middleNode, reclaimMark)
		return res, nil
	}
	// We don't use middle because the "that" node has precedence.
	left, middle, right, err :=
		o.split(t, this, thatItem.Key, reclaimMark)
	if err != nil {
		return empty_nodeLoc, err
	}
	newLeft, err := o.union(t, left, &thatNode.left, reclaimMark)
	if err != nil {
		return empty_nodeLoc, err
	}
	newRight, err := o.union(t, right, &thatNode.right, reclaimMark)
	if err != nil {
		return empty_nodeLoc, err
	}
	leftNum, leftBytes, rightNum, rightBytes, err :=
		numInfo(o, newLeft, newRight)
	if err != nil {
		return empty_nodeLoc, err
	}
	res = t.mkNodeLoc(t.mkNode(thatItemLoc, newLeft, newRight,
		leftNum+rightNum+1,
		leftBytes+rightBytes+uint64(thatItemLoc.NumBytes(t))))
	middleNode := middle.Node()
	t.freeNodeLoc(left)
	t.freeNodeLoc(right)
	t.freeNodeLoc(middle)
	t.freeNodeLoc(newLeft)
	t.freeNodeLoc(newRight)
	t.markReclaimable(thatNode, reclaimMark)
	t.markReclaimable(middleNode, reclaimMark)
	return res, nil
}

// Splits a treap into two treaps based on a split key "s".  The
// result is (left, middle, right), where left treap has keys < s,
// right treap has keys > s, and middle is either...
// * empty/nil - meaning key s was not in the original treap.
// * non-empty - returning the original nodeLoc/item that had key s.
func (o *Store) split(t *Collection, n *nodeLoc, s []byte,
	reclaimMark *node) (
	*nodeLoc, *nodeLoc, *nodeLoc, error) {
	nNode, err := n.read(o)
	if err != nil || n.isEmpty() || nNode == nil {
		return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, err
	}

	nItemLoc := &nNode.item
	nItem, err := nItemLoc.read(t, false)
	if err != nil {
		return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, err
	}

	c := t.compare(s, nItem.Key)
	if c == 0 {
		left := t.mkNodeLoc(nil).Copy(&nNode.left)
		right := t.mkNodeLoc(nil).Copy(&nNode.right)
		middle := t.mkNodeLoc(nil).Copy(n)
		return left, middle, right, nil
	}

	if c < 0 {
		if nNode.left.isEmpty() {
			return empty_nodeLoc, empty_nodeLoc, t.mkNodeLoc(nil).Copy(n), nil
		}
		left, middle, right, err :=
			o.split(t, &nNode.left, s, reclaimMark)
		if err != nil {
			return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, err
		}
		leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, right, &nNode.right)
		if err != nil {
			return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, err
		}
		newRight := t.mkNodeLoc(t.mkNode(nItemLoc, right, &nNode.right,
			leftNum+rightNum+1,
			leftBytes+rightBytes+uint64(nItemLoc.NumBytes(t))))
		t.freeNodeLoc(right)
		t.markReclaimable(nNode, reclaimMark)
		return left, middle, newRight, nil
	}

	if nNode.right.isEmpty() {
		return t.mkNodeLoc(nil).Copy(n), empty_nodeLoc, empty_nodeLoc, nil
	}
	left, middle, right, err :=
		o.split(t, &nNode.right, s, reclaimMark)
	if err != nil {
		return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, err
	}
	leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, &nNode.left, left)
	if err != nil {
		return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, err
	}
	newLeft := t.mkNodeLoc(t.mkNode(nItemLoc, &nNode.left, left,
		leftNum+rightNum+1,
		leftBytes+rightBytes+uint64(nItemLoc.NumBytes(t))))
	t.freeNodeLoc(left)
	t.markReclaimable(nNode, reclaimMark)
	return newLeft, middle, right, nil
}

// Joins this treap and that treap into one treap.  Unlike union(),
// the join() function assumes all keys from this treap should be less
// than keys from that treap.
func (o *Store) join(t *Collection, this *nodeLoc, that *nodeLoc,
	reclaimMark *node) (
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
		return t.mkNodeLoc(nil).Copy(that), nil
	}
	if that.isEmpty() || thatNode == nil {
		return t.mkNodeLoc(nil).Copy(this), nil
	}
	thisItemLoc := &thisNode.item
	thisItem, err := thisItemLoc.read(t, false)
	if err != nil {
		return empty_nodeLoc, err
	}
	thatItemLoc := &thatNode.item
	thatItem, err := thatItemLoc.read(t, false)
	if err != nil {
		return empty_nodeLoc, err
	}
	if thisItem.Priority > thatItem.Priority {
		newRight, err :=
			o.join(t, &thisNode.right, that, reclaimMark)
		if err != nil {
			return empty_nodeLoc, err
		}
		leftNum, leftBytes, rightNum, rightBytes, err :=
			numInfo(o, &thisNode.left, newRight)
		if err != nil {
			return empty_nodeLoc, err
		}
		res = t.mkNodeLoc(t.mkNode(thisItemLoc, &thisNode.left, newRight,
			leftNum+rightNum+1,
			leftBytes+rightBytes+uint64(thisItemLoc.NumBytes(t))))
		t.markReclaimable(thisNode, reclaimMark)
		t.freeNodeLoc(newRight)
		return res, nil
	}
	newLeft, err :=
		o.join(t, this, &thatNode.left, reclaimMark)
	if err != nil {
		return empty_nodeLoc, err
	}
	leftNum, leftBytes, rightNum, rightBytes, err :=
		numInfo(o, newLeft, &thatNode.right)
	if err != nil {
		return empty_nodeLoc, err
	}
	res = t.mkNodeLoc(t.mkNode(thatItemLoc, newLeft, &thatNode.right,
		leftNum+rightNum+1,
		leftBytes+rightBytes+uint64(thatItemLoc.NumBytes(t))))
	t.markReclaimable(thatNode, reclaimMark)
	t.freeNodeLoc(newLeft)
	return res, nil
}

func (o *Store) walk(t *Collection, withValue bool, cfn func(*node) (*nodeLoc, bool)) (
	res *Item, err error) {
	rnl := t.rootAddRef()
	defer t.rootDecRef(rnl)
	n := rnl.root
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
			i, err := nNode.item.read(t, withValue)
			if err != nil {
				return nil, err
			}
			o.ItemAddRef(t, i)
			return i, nil
		}
		nNode = childNode
	}
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
	nItem, err := nItemLoc.read(t, false)
	if err != nil {
		return false, err
	}
	if nItem == nil {
		panic(fmt.Sprintf("visitNodes nItem nil: %#v", nNode))
	}
	choice, choiceT, choiceF := choiceFunc(t.compare(target, nItem.Key), nNode)
	if choice {
		keepGoing, err :=
			o.visitNodes(t, choiceT, target, withValue, visitor, depth+1, choiceFunc)
		if err != nil || !keepGoing {
			return false, err
		}
		nItem, err := nItemLoc.read(t, withValue)
		if err != nil {
			return false, err
		}
		if !visitor(nItem, depth) {
			return false, nil
		}
	}
	return o.visitNodes(t, choiceF, target, withValue, visitor, depth+1, choiceFunc)
}
