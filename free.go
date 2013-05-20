package gkvlite

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

var reclaimable_node = &node{} // Sentinel.

var freeNodeLock sync.Mutex
var freeNodes *node

var freeNodeLocLock sync.Mutex
var freeNodeLocs *nodeLoc

var freeRootNodeLocLock sync.Mutex
var freeRootNodeLocs *rootNodeLoc

var freeStats FreeStats

type FreeStats struct {
	MkNodes    int64
	FreeNodes  int64
	AllocNodes int64

	MkNodeLocs    int64
	FreeNodeLocs  int64
	AllocNodeLocs int64

	MkRootNodeLocs    int64
	FreeRootNodeLocs  int64
	AllocRootNodeLocs int64
}

func (t *Collection) markReclaimable(n *node) {
	if n == nil || n.next != nil || n == reclaimable_node {
		return
	}
	n.next = reclaimable_node // Use next pointer as sentinel.
}

func (t *Collection) reclaimNodes_unlocked(n *node) {
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
	t.freeNode_unlocked(n)
	t.reclaimNodes_unlocked(left)
	t.reclaimNodes_unlocked(right)
}

// Assumes that the caller serializes invocations.
func (t *Collection) mkNode(itemIn *itemLoc, leftIn *nodeLoc, rightIn *nodeLoc,
	numNodesIn uint64, numBytesIn uint64) *node {
	freeNodeLock.Lock()
	freeStats.MkNodes++
	t.stats.MkNodes++
	n := freeNodes
	if n == nil {
		freeStats.AllocNodes++
		t.stats.AllocNodes++
		freeNodeLock.Unlock()
		atomic.AddUint64(&t.store.nodeAllocs, 1)
		n = &node{}
	} else {
		freeNodes = n.next
		freeNodeLock.Unlock()
	}
	n.item.Copy(itemIn)
	n.left.Copy(leftIn)
	n.right.Copy(rightIn)
	n.numNodes = numNodesIn
	n.numBytes = numBytesIn
	n.next = nil
	return n
}

func (t *Collection) freeNode_unlocked(n *node) {
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

	n.next = freeNodes
	freeNodes = n
	freeStats.FreeNodes++
	t.stats.FreeNodes++
}

// Assumes that the caller serializes invocations.
func (t *Collection) mkNodeLoc(n *node) *nodeLoc {
	freeNodeLocLock.Lock()
	freeStats.MkNodeLocs++
	t.stats.MkNodeLocs++
	nloc := freeNodeLocs
	if nloc == nil {
		freeStats.AllocNodeLocs++
		t.stats.AllocNodeLocs++
		freeNodeLocLock.Unlock()
		nloc = &nodeLoc{}
	} else {
		freeNodeLocs = nloc.next
		freeNodeLocLock.Unlock()
	}
	nloc.loc = unsafe.Pointer(nil)
	nloc.node = unsafe.Pointer(n)
	nloc.next = nil
	return nloc
}

// Assumes that the caller serializes invocations.
func (t *Collection) freeNodeLoc(nloc *nodeLoc) {
	if nloc == nil || nloc == empty_nodeLoc {
		return
	}
	if nloc.next != nil {
		panic("double free nodeLoc")
	}
	nloc.loc = unsafe.Pointer(nil)
	nloc.node = unsafe.Pointer(nil)

	freeNodeLocLock.Lock()
	nloc.next = freeNodeLocs
	freeNodeLocs = nloc
	freeStats.FreeNodeLocs++
	t.stats.FreeNodeLocs++
	freeNodeLocLock.Unlock()
}

func (t *Collection) mkRootNodeLoc(root *nodeLoc) *rootNodeLoc {
	freeRootNodeLocLock.Lock()
	freeStats.MkRootNodeLocs++
	t.stats.MkRootNodeLocs++
	rnl := freeRootNodeLocs
	if rnl == nil {
		freeStats.AllocRootNodeLocs++
		t.stats.AllocRootNodeLocs++
		freeRootNodeLocLock.Unlock()
		rnl = &rootNodeLoc{}
	} else {
		freeRootNodeLocs = rnl.next
		freeRootNodeLocLock.Unlock()
	}
	rnl.refs = 1
	rnl.root = root
	rnl.next = nil
	rnl.chainedCollection = nil
	rnl.chainedRootNodeLoc = nil
	return rnl
}

func (t *Collection) freeRootNodeLoc(rnl *rootNodeLoc) {
	if rnl == nil {
		return
	}
	if rnl.next != nil {
		panic("double free rootNodeLoc")
	}
	rnl.refs = 0
	rnl.root = nil
	rnl.chainedCollection = nil
	rnl.chainedRootNodeLoc = nil

	freeRootNodeLocLock.Lock()
	rnl.next = freeRootNodeLocs
	freeRootNodeLocs = rnl
	freeStats.FreeRootNodeLocs++
	t.stats.FreeRootNodeLocs++
	freeRootNodeLocLock.Unlock()
}
