package gkvlite

import (
	"sync/atomic"
	"unsafe"
)

var reclaimable_node = &node{} // Sentinel.

func (t *Collection) markReclaimable(n *node) {
	if n == nil || n.next != nil || n == reclaimable_node {
		return
	}
	n.next = reclaimable_node // Use next pointer as sentinel.
}

func (t *Collection) reclaimNodes(n *node) {
	if n == nil {
		return
	}
	if n.next != reclaimable_node {
		return
	}
	var left *node
	var right *node
	if !n.left.isEmpty() {
		left = n.left.Node()
	}
	if !n.right.isEmpty() {
		right = n.right.Node()
	}
	t.freeNode(n)
	t.reclaimNodes(left)
	t.reclaimNodes(right)
}

// Assumes that the caller serializes invocations.
func (t *Collection) mkNode(itemIn *itemLoc, leftIn *nodeLoc, rightIn *nodeLoc,
	numNodesIn uint64, numBytesIn uint64) *node {
	t.stats.MkNodes++
	t.freeLock.Lock()
	n := t.freeNodes
	if n == nil {
		t.freeLock.Unlock()
		atomic.AddUint64(&t.store.nodeAllocs, 1)
		t.stats.AllocNodes++
		n = &node{}
	} else {
		t.freeNodes = n.next
		t.freeLock.Unlock()
	}
	n.item.Copy(itemIn)
	n.left.Copy(leftIn)
	n.right.Copy(rightIn)
	n.numNodes = numNodesIn
	n.numBytes = numBytesIn
	n.next = nil
	return n
}

func (t *Collection) freeNode(n *node) {
	return

	if n == nil || n == reclaimable_node {
		return
	}
	if n.next != nil && n.next != reclaimable_node {
		panic("double free node")
	}
	n.item = *empty_itemLoc
	n.left = *empty_nodeLoc
	n.right = *empty_nodeLoc
	n.numNodes = 0
	n.numBytes = 0

	t.freeLock.Lock()
	n.next = t.freeNodes
	t.freeNodes = n
	t.stats.FreeNodes++
	t.freeLock.Unlock()
}

// Assumes that the caller serializes invocations.
func (t *Collection) mkNodeLoc(n *node) *nodeLoc {
	t.stats.MkNodeLocs++
	nloc := t.freeNodeLocs
	if nloc == nil {
		t.stats.AllocNodeLocs++
		nloc = &nodeLoc{}
	}
	t.freeNodeLocs = nloc.next
	nloc.loc = unsafe.Pointer(nil)
	nloc.node = unsafe.Pointer(n)
	nloc.next = nil
	return nloc
}

// Assumes that the caller serializes invocations.
func (t *Collection) freeNodeLoc(nloc *nodeLoc) {
	return

	if nloc == nil || nloc == empty_nodeLoc {
		return
	}
	if nloc.next != nil {
		panic("double free nloc")
	}
	t.stats.FreeNodeLocs++
	nloc.loc = unsafe.Pointer(nil)
	nloc.node = unsafe.Pointer(nil)
	nloc.next = t.freeNodeLocs
	t.freeNodeLocs = nloc
}

func (t *Collection) mkRootNodeLoc(root *nodeLoc) *rootNodeLoc {
	t.freeLock.Lock()
	rnl := t.freeRootNodeLocs
	if rnl == nil {
		t.freeLock.Unlock()
		rnl = &rootNodeLoc{}
	} else {
		t.freeRootNodeLocs = rnl.next
		t.freeLock.Unlock()
	}
	rnl.refs = 1
	rnl.root = root
	rnl.next = nil
	return rnl
}

func (t *Collection) freeRootNodeLoc(rnl *rootNodeLoc) {
	return

	if rnl == nil {
		return
	}
	rnl.refs = 0
	rnl.root = nil

	t.freeLock.Lock()
	rnl.next = t.freeRootNodeLocs
	t.freeRootNodeLocs = rnl
	t.freeLock.Unlock()
}
