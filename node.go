package gkvlite

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
)

// A persistable node.
type node struct {
	numNodes, numBytes uint64
	item               itemLoc
	left, right        nodeLoc
	next               *node // For free-list tracking.
}

func (n *node) Evict() *Item {
	if !n.item.Loc().isEmpty() {
		i := n.item.Item()
		if i != nil && n.item.casItem(i, nil) {
			return i
		}
	}
	return nil
}

var nodeLocGL = sync.RWMutex{}

const nodeMutex = false

// A persistable node and its persistence location.
type nodeLoc struct {
	loc  *ploc    // *ploc - can be nil if node is dirty (not yet persisted).
	node *node    // *node - can be nil if node is not fetched into memory yet.
	next *nodeLoc // For free-list tracking.
}

var emptyNodeLoc = nodeLoc{} // Sentinel.

func (nloc *nodeLoc) Loc() *ploc {
	if nodeMutex {
		nodeLocGL.RLock()
		defer nodeLocGL.RUnlock()
	}
	return nloc.loc
}

func (nloc *nodeLoc) setLoc(n *ploc) {
	if nodeMutex {
		nodeLocGL.Lock()
		defer nodeLocGL.Unlock()
	}
	nloc.loc = n
}

func (nloc *nodeLoc) Node() *node {
	if nodeMutex {
		nodeLocGL.RLock()
		defer nodeLocGL.RUnlock()
	}
	return nloc.node
}

func (nloc *nodeLoc) setNode(n *node) {
	if nodeMutex {
		nodeLocGL.Lock()
		defer nodeLocGL.Unlock()
	}
	nloc.node = n
}

func (nloc *nodeLoc) LocNode() (*ploc, *node) {
	if nodeMutex {
		nodeLocGL.RLock()
		defer nodeLocGL.RUnlock()
	}
	return nloc.loc, nloc.node
}
func (n *node) setNumBytes(b []byte, pos int) {
	n.numBytes = binary.BigEndian.Uint64(b[pos : pos+8])
}
func (n *node) setNumNodes(b []byte, pos int) {
	n.numNodes = binary.BigEndian.Uint64(b[pos : pos+8])
}

func (nloc *nodeLoc) Copy(src *nodeLoc) *nodeLoc {
	if src == nil {
		return nloc.Copy(&emptyNodeLoc)
	}

	if nodeMutex {
		nodeLocGL.Lock()
		defer nodeLocGL.Unlock()
	}
	// NOTE: This trick only works because of the global lock. No reason to lock
	// src independently of nlock.
	nloc.loc = src.loc
	nloc.node = src.node
	return nloc
}

func (nloc *nodeLoc) isEmpty() bool {
	if nodeMutex {
		nodeLocGL.RLock()
		defer nodeLocGL.RUnlock()
	}
	return nloc == nil || (nloc.loc.isEmpty() && nloc.node == nil)
}

func (nloc *nodeLoc) write(o *Store) error {
	loc, node := nloc.LocNode()

	if nloc != nil && loc.isEmpty() {
		if node == nil {
			return nil
		}
		// Load current size from store
		offset := o.getSize()
		length := plocLength + plocLength + plocLength + 8 + 8
		b, err := node.populateDiskStruct(length)
		if err != nil {
			return err
		}
		if _, err := o.file.WriteAt(b, offset); err != nil {
			return err
		}
		o.setSize(offset + int64(length))
		nloc.setLoc(&ploc{Offset: offset, Length: uint32(length)})
	}
	return nil
}

// populateDiskStruct Created a byte array to write to the disk
// This is popluated from the current node data
func (nd node) populateDiskStruct(length int) (b []byte, err error) {
	b = make([]byte, length)
	var pos int
	pos = nd.item.Loc().write(b, pos)
	pos = nd.left.Loc().write(b, pos)
	pos = nd.right.Loc().write(b, pos)
	binary.BigEndian.PutUint64(b[pos:pos+8], nd.numNodes)
	pos += 8
	binary.BigEndian.PutUint64(b[pos:pos+8], nd.numBytes)
	pos += 8
	if pos != length {
		return b, fmt.Errorf("nodeLoc.write() pos: %v didn't match length: %v",
			pos, length)
	}
	return
}

// Populate a new node structure from the byte array supplied
// The byte array contains the data read from the disk
func populateNode(b []byte) (n *node, err error) {
	n = &node{}
	var p *ploc
	var pos int
	p = &ploc{}
	p, pos = p.read(b, pos)
	n.item.loc = p
	p = &ploc{}
	p, pos = p.read(b, pos)
	n.left.loc = p
	p = &ploc{}
	p, pos = p.read(b, pos)
	n.right.loc = p
	n.setNumNodes(b, pos)
	pos += 8
	n.setNumBytes(b, pos)
	pos += 8
	if pos != len(b) {
		return nil, fmt.Errorf("nodeLoc.read() pos: %v didn't match length: %v",
			pos, len(b))
	}
	return n, nil
}
func (nloc *nodeLoc) read(o *Store) (n *node, err error) {
	if nloc == nil {
		return nil, nil
	}
	loc, n := nloc.LocNode()
	if n != nil {
		return n, nil
	}
	if loc.isEmpty() {
		return nil, nil
	}
	if loc.Length != uint32(plocLength+plocLength+plocLength+8+8) {
		return nil, fmt.Errorf("unexpected node loc.Length: %v != %v",
			loc.Length, plocLength+plocLength+plocLength+8+8)
	}
	b := make([]byte, loc.Length)
	if _, err := o.file.ReadAt(b, loc.Offset); err != nil {
		return nil, err
	}

	atomic.AddUint64(&o.nodeAllocs, 1)

	n, err = populateNode(b)
	if err != nil {
		return n, err
	}

	nloc.setNode(n)
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

func dump(o *Store, n *nodeLoc, level int) {
	if n.isEmpty() {
		return
	}
	nNode, _ := n.read(o)
	dump(o, &nNode.left, level+1)
	dumpIndent(level)
	k := "<evicted>"
	if nNode.item.Item() != nil {
		k = string(nNode.item.Item().Key)
	}
	fmt.Printf("%p - %v\n", nNode, k)
	dump(o, &nNode.right, level+1)
}

func dumpIndent(level int) {
	for i := 0; i < level; i++ {
		fmt.Print(" ")
	}
}
