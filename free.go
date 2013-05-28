package gkvlite

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

var freeNodeLock sync.Mutex
var freeNodes *node

var freeNodeLocLock sync.Mutex
var freeNodeLocs *nodeLoc

var freeRootNodeLocLock sync.Mutex
var freeRootNodeLocs *rootNodeLoc

var freeStats FreeStats

type FreeStats struct {
	MkNodes      int64
	FreeNodes    int64 // Number of invocations of the freeNode() API.
	AllocNodes   int64
	CurFreeNodes int64 // Current length of freeNodes list.

	MkNodeLocs      int64
	FreeNodeLocs    int64 // Number of invocations of the freeNodeLoc() API.
	AllocNodeLocs   int64
	CurFreeNodeLocs int64 // Current length of freeNodeLocs list.

	MkRootNodeLocs      int64
	FreeRootNodeLocs    int64 // Number of invocations of the freeRootNodeLoc() API.
	AllocRootNodeLocs   int64
	CurFreeRootNodeLocs int64 // Current length of freeRootNodeLocs list.
}

func (t *Collection) markReclaimable(n *node, reclaimMark *node) {
	t.rootLock.Lock()
	defer t.rootLock.Unlock()
	if n == nil || n.next != nil || n == reclaimMark {
		return
	}
	n.next = reclaimMark
}

func (t *Collection) reclaimNodes_unlocked(n *node,
	reclaimLater *[2]*node, reclaimMark *node) int64 {
	if n == nil {
		return 0
	}
	if reclaimLater != nil {
		for i := 0; i < len(reclaimLater); i++ {
			if reclaimLater[i] == n {
				reclaimLater[i] = nil
			}
		}
	}
	if n.next != reclaimMark {
		return 0
	}
	var left *node
	var right *node
	if !n.left.isEmpty() {
		left = n.left.Node()
	}
	if !n.right.isEmpty() {
		right = n.right.Node()
	}
	t.freeNode_unlocked(n, reclaimMark)
	numLeft := t.reclaimNodes_unlocked(left, reclaimLater, reclaimMark)
	numRight := t.reclaimNodes_unlocked(right, reclaimLater, reclaimMark)
	return 1 + numLeft + numRight
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
		freeStats.CurFreeNodes--
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

func (t *Collection) freeNode_unlocked(n *node, reclaimMark *node) {
	if n == nil || n == reclaimMark {
		return
	}
	if n.next != nil && n.next != reclaimMark {
		panic("double free node")
	}
	n.item = *empty_itemLoc
	n.left = *empty_nodeLoc
	n.right = *empty_nodeLoc
	n.numNodes = 0
	n.numBytes = 0

	n.next = freeNodes
	freeNodes = n
	freeStats.CurFreeNodes++
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
		freeStats.CurFreeNodeLocs--
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
	freeStats.CurFreeNodeLocs++
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
		freeStats.CurFreeRootNodeLocs--
		freeRootNodeLocLock.Unlock()
	}
	rnl.refs = 1
	rnl.root = root
	rnl.next = nil
	rnl.chainedCollection = nil
	rnl.chainedRootNodeLoc = nil
	for i := 0; i < len(rnl.reclaimLater); i++ {
		rnl.reclaimLater[i] = nil
	}
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
	for i := 0; i < len(rnl.reclaimLater); i++ {
		rnl.reclaimLater[i] = nil
	}
	freeRootNodeLocLock.Lock()
	rnl.next = freeRootNodeLocs
	freeRootNodeLocs = rnl
	freeStats.CurFreeRootNodeLocs++
	freeStats.FreeRootNodeLocs++
	t.stats.FreeRootNodeLocs++
	freeRootNodeLocLock.Unlock()
}
