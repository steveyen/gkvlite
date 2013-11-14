package gkvlite

import (
	"fmt"
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

var allocStats AllocStats

type AllocStats struct {
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

func withAllocLocks(cb func()) {
	freeNodeLock.Lock()
	freeNodeLocLock.Lock()
	freeRootNodeLocLock.Lock()
	defer freeNodeLock.Unlock()
	defer freeNodeLocLock.Unlock()
	defer freeRootNodeLocLock.Unlock()
	cb()
}

func (t *Collection) markReclaimable(n *node, reclaimMark *node) {
	t.rootLock.Lock()
	defer t.rootLock.Unlock()
	if n == nil || n.next != nil || n == reclaimMark {
		return
	}
	n.next = reclaimMark
}

func (t *Collection) reclaimMarkUpdate(nloc *nodeLoc,
	oldReclaimMark, newReclaimMark *node) *node {
	if nloc.isEmpty() {
		return nil
	}
	n := nloc.Node()
	t.rootLock.Lock()
	if n != nil && n.next == oldReclaimMark {
		n.next = newReclaimMark
		t.rootLock.Unlock()
		t.reclaimMarkUpdate(&n.left, oldReclaimMark, newReclaimMark)
		t.reclaimMarkUpdate(&n.right, oldReclaimMark, newReclaimMark)
	} else {
		t.rootLock.Unlock()
	}
	return n
}

func (t *Collection) reclaimNodes_unlocked(n *node,
	reclaimLater *[3]*node, reclaimMark *node) int64 {
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
	allocStats.MkNodes++
	t.allocStats.MkNodes++
	n := freeNodes
	if n == nil {
		allocStats.AllocNodes++
		t.allocStats.AllocNodes++
		freeNodeLock.Unlock()
		atomic.AddUint64(&t.store.nodeAllocs, 1)
		n = &node{}
	} else {
		freeNodes = n.next
		allocStats.CurFreeNodes--
		freeNodeLock.Unlock()
	}
	if itemIn != nil {
		i := itemIn.Item()
		if i != nil {
			t.store.ItemAddRef(t, i)
		}
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
	i := n.item.Item()
	if i != nil {
		t.store.ItemDecRef(t, i)
	}
	n.item = *empty_itemLoc
	n.left = *empty_nodeLoc
	n.right = *empty_nodeLoc
	n.numNodes = 0
	n.numBytes = 0
	n.next = freeNodes
	freeNodes = n
	allocStats.CurFreeNodes++
	allocStats.FreeNodes++
	t.allocStats.FreeNodes++
}

// Assumes that the caller serializes invocations.
func (t *Collection) mkNodeLoc(n *node) *nodeLoc {
	freeNodeLocLock.Lock()
	allocStats.MkNodeLocs++
	t.allocStats.MkNodeLocs++
	nloc := freeNodeLocs
	if nloc == nil {
		allocStats.AllocNodeLocs++
		t.allocStats.AllocNodeLocs++
		freeNodeLocLock.Unlock()
		nloc = &nodeLoc{}
	} else {
		freeNodeLocs = nloc.next
		allocStats.CurFreeNodeLocs--
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
	allocStats.CurFreeNodeLocs++
	allocStats.FreeNodeLocs++
	t.allocStats.FreeNodeLocs++
	freeNodeLocLock.Unlock()
}

func (t *Collection) mkRootNodeLoc(root *nodeLoc) *rootNodeLoc {
	freeRootNodeLocLock.Lock()
	allocStats.MkRootNodeLocs++
	t.allocStats.MkRootNodeLocs++
	rnl := freeRootNodeLocs
	if rnl == nil {
		allocStats.AllocRootNodeLocs++
		t.allocStats.AllocRootNodeLocs++
		freeRootNodeLocLock.Unlock()
		rnl = &rootNodeLoc{}
	} else {
		freeRootNodeLocs = rnl.next
		allocStats.CurFreeRootNodeLocs--
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
		if rnl.reclaimLater[i] != nil {
			panic(fmt.Sprintf("non-nil rnl.reclaimLater[%d]: %v",
				i, rnl.reclaimLater[i]))
		}
	}
	freeRootNodeLocLock.Lock()
	rnl.next = freeRootNodeLocs
	freeRootNodeLocs = rnl
	allocStats.CurFreeRootNodeLocs++
	allocStats.FreeRootNodeLocs++
	t.allocStats.FreeRootNodeLocs++
	freeRootNodeLocLock.Unlock()
}
