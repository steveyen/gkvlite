package gkvlite

func (o *Store) union(t *Collection, this *nodeLoc, that *nodeLoc) (
	res *nodeLoc, resIsNew bool, err error) {
	thisNode, err := this.read(o)
	if err != nil {
		return empty_nodeLoc, false, err
	}
	thatNode, err := that.read(o)
	if err != nil {
		return empty_nodeLoc, false, err
	}
	if this.isEmpty() || thisNode == nil {
		return that, false, nil
	}
	if that.isEmpty() || thatNode == nil {
		return this, false, nil
	}
	thisItemLoc := &thisNode.item
	thisItem, err := thisItemLoc.read(t, false)
	if err != nil {
		return empty_nodeLoc, false, err
	}
	thatItemLoc := &thatNode.item
	thatItem, err := thatItemLoc.read(t, false)
	if err != nil {
		return empty_nodeLoc, false, err
	}
	if thisItem.Priority > thatItem.Priority {
		left, middle, right, leftIsNew, rightIsNew, err :=
			o.split(t, that, thisItem.Key)
		if err != nil {
			return empty_nodeLoc, false, err
		}
		newLeft, newLeftIsNew, err := o.union(t, &thisNode.left, left)
		if err != nil {
			return empty_nodeLoc, false, err
		}
		if leftIsNew && left != newLeft {
			t.freeNodeLoc(left)
		}
		newRight, newRightIsNew, err := o.union(t, &thisNode.right, right)
		if err != nil {
			return empty_nodeLoc, false, err
		}
		if rightIsNew && right != newRight {
			t.freeNodeLoc(right)
		}
		leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, newLeft, newRight)
		if err != nil {
			return empty_nodeLoc, false, err
		}
		if middle.isEmpty() {
			res = t.mkNodeLoc(t.mkNode(thisItemLoc, newLeft, newRight,
				leftNum+rightNum+1,
				leftBytes+rightBytes+uint64(thisItem.NumBytes(t))))
			if newLeftIsNew {
				t.freeNodeLoc(newLeft)
			}
			if newRightIsNew {
				t.freeNodeLoc(newRight)
			}
			t.markReclaimable(thisNode)
			return res, true, nil
		}
		middleNode, err := middle.read(o)
		if err != nil {
			return empty_nodeLoc, false, err
		}
		middleItem, err := middleNode.item.read(t, false)
		if err != nil {
			return empty_nodeLoc, false, err
		}
		res = t.mkNodeLoc(t.mkNode(&middleNode.item, newLeft, newRight,
			leftNum+rightNum+1,
			leftBytes+rightBytes+uint64(middleItem.NumBytes(t))))
		if newLeftIsNew {
			t.freeNodeLoc(newLeft)
		}
		if newRightIsNew {
			t.freeNodeLoc(newRight)
		}
		if middle != that {
			t.markReclaimable(middleNode)
		}
		t.markReclaimable(thisNode)
		return res, true, nil
	}
	// We don't use middle because the "that" node has precedence.
	left, middle, right, leftIsNew, rightIsNew, err :=
		o.split(t, this, thatItem.Key)
	if err != nil {
		return empty_nodeLoc, false, err
	}
	newLeft, newLeftIsNew, err := o.union(t, left, &thatNode.left)
	if err != nil {
		return empty_nodeLoc, false, err
	}
	if leftIsNew && left != newLeft {
		t.freeNodeLoc(left)
	}
	newRight, newRightIsNew, err := o.union(t, right, &thatNode.right)
	if err != nil {
		return empty_nodeLoc, false, err
	}
	if rightIsNew && right != newRight {
		t.freeNodeLoc(right)
	}
	leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, newLeft, newRight)
	if err != nil {
		return empty_nodeLoc, false, err
	}
	res = t.mkNodeLoc(t.mkNode(thatItemLoc, newLeft, newRight,
		leftNum+rightNum+1,
		leftBytes+rightBytes+uint64(thatItem.NumBytes(t))))
	if newLeftIsNew {
		t.freeNodeLoc(newLeft)
	}
	if newRightIsNew {
		t.freeNodeLoc(newRight)
	}
	t.markReclaimable(thatNode)
	if !middle.isEmpty() && middle != this {
		t.markReclaimable(middle.Node())
	}
	return res, true, nil
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
	nItem, err := nItemLoc.read(t, false)
	if err != nil {
		return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, false, false, err
	}

	c := t.compare(s, nItem.Key)
	if c == 0 {
		return &nNode.left, n, &nNode.right, false, false, nil
	}

	if c < 0 {
		left, middle, right, leftIsNew, rightIsNew, err :=
			o.split(t, &nNode.left, s)
		if err != nil {
			return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, false, false, err
		}
		leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, right, &nNode.right)
		if err != nil {
			return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, false, false, err
		}
		newRight := t.mkNodeLoc(t.mkNode(nItemLoc, right, &nNode.right,
			leftNum+rightNum+1,
			leftBytes+rightBytes+uint64(nItem.NumBytes(t))))
		if rightIsNew {
			t.freeNodeLoc(right)
		}
		if middle != &nNode.left {
			t.markReclaimable(nNode)
		}
		return left, middle, newRight, leftIsNew, true, nil
	}

	left, middle, right, leftIsNew, rightIsNew, err :=
		o.split(t, &nNode.right, s)
	if err != nil {
		return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, false, false, err
	}
	leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, &nNode.left, left)
	if err != nil {
		return empty_nodeLoc, empty_nodeLoc, empty_nodeLoc, false, false, err
	}
	newLeft := t.mkNodeLoc(t.mkNode(nItemLoc, &nNode.left, left,
		leftNum+rightNum+1,
		leftBytes+rightBytes+uint64(nItem.NumBytes(t))))
	if leftIsNew {
		t.freeNodeLoc(left)
	}
	if middle != &nNode.right {
		t.markReclaimable(nNode)
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
		newRight, err := o.join(t, &thisNode.right, that)
		if err != nil {
			return empty_nodeLoc, err
		}
		leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, &thisNode.left, newRight)
		if err != nil {
			return empty_nodeLoc, err
		}
		t.markReclaimable(thisNode)
		return t.mkNodeLoc(t.mkNode(thisItemLoc, &thisNode.left, newRight,
			leftNum+rightNum+1,
			leftBytes+rightBytes+uint64(thisItem.NumBytes(t)))), nil
	}
	newLeft, err := o.join(t, this, &thatNode.left)
	if err != nil {
		return empty_nodeLoc, err
	}
	leftNum, leftBytes, rightNum, rightBytes, err := numInfo(o, newLeft, &thatNode.right)
	if err != nil {
		return empty_nodeLoc, err
	}
	t.markReclaimable(thatNode)
	return t.mkNodeLoc(t.mkNode(thatItemLoc, newLeft, &thatNode.right,
		leftNum+rightNum+1,
		leftBytes+rightBytes+uint64(thatItem.NumBytes(t)))), nil
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
