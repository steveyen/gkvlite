package gkvlite

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"unsafe"
)

// A persistable node.
type node struct {
	numNodes, numBytes uint64
	item               itemLoc
	left, right        nodeLoc
	next               *node // For free-list tracking.
}

// A persistable node and its persistence location.
type nodeLoc struct {
	loc  unsafe.Pointer // *ploc - can be nil if node is dirty (not yet persisted).
	node unsafe.Pointer // *node - can be nil if node is not fetched into memory yet.
	next *nodeLoc       // For free-list tracking.
}

var empty_nodeLoc = &nodeLoc{} // Sentinel.

func (nloc *nodeLoc) Loc() *ploc {
	return (*ploc)(atomic.LoadPointer(&nloc.loc))
}

func (nloc *nodeLoc) Node() *node {
	return (*node)(atomic.LoadPointer(&nloc.node))
}

func (nloc *nodeLoc) Copy(src *nodeLoc) *nodeLoc {
	if src == nil {
		return nloc.Copy(empty_nodeLoc)
	}
	atomic.StorePointer(&nloc.loc, unsafe.Pointer(src.Loc()))
	atomic.StorePointer(&nloc.node, unsafe.Pointer(src.Node()))
	return nloc
}

func (nloc *nodeLoc) isEmpty() bool {
	return nloc == nil || (nloc.Loc().isEmpty() && nloc.Node() == nil)
}

func (nloc *nodeLoc) write(o *Store) error {
	if nloc != nil && nloc.Loc().isEmpty() {
		node := nloc.Node()
		if node == nil {
			return nil
		}
		offset := atomic.LoadInt64(&o.size)
		length := ploc_length + ploc_length + ploc_length + 8 + 8
		b := make([]byte, length)
		pos := 0
		pos = node.item.Loc().write(b, pos)
		pos = node.left.Loc().write(b, pos)
		pos = node.right.Loc().write(b, pos)
		binary.BigEndian.PutUint64(b[pos:pos+8], node.numNodes)
		pos += 8
		binary.BigEndian.PutUint64(b[pos:pos+8], node.numBytes)
		pos += 8
		if pos != length {
			return fmt.Errorf("nodeLoc.write() pos: %v didn't match length: %v",
				pos, length)
		}
		if _, err := o.file.WriteAt(b, offset); err != nil {
			return err
		}
		atomic.StoreInt64(&o.size, offset+int64(length))
		atomic.StorePointer(&nloc.loc,
			unsafe.Pointer(&ploc{Offset: offset, Length: uint32(length)}))
	}
	return nil
}

func (nloc *nodeLoc) read(o *Store) (n *node, err error) {
	if nloc == nil {
		return nil, nil
	}
	n = nloc.Node()
	if n != nil {
		return n, nil
	}
	loc := nloc.Loc()
	if loc.isEmpty() {
		return nil, nil
	}
	if loc.Length != uint32(ploc_length+ploc_length+ploc_length+8+8) {
		return nil, fmt.Errorf("unexpected node loc.Length: %v != %v",
			loc.Length, ploc_length+ploc_length+ploc_length+8+8)
	}
	b := make([]byte, loc.Length)
	if _, err := o.file.ReadAt(b, loc.Offset); err != nil {
		return nil, err
	}
	pos := 0
	atomic.AddUint64(&o.nodeAllocs, 1)
	n = &node{}
	var p *ploc
	p = &ploc{}
	p, pos = p.read(b, pos)
	n.item.loc = unsafe.Pointer(p)
	p = &ploc{}
	p, pos = p.read(b, pos)
	n.left.loc = unsafe.Pointer(p)
	p = &ploc{}
	p, pos = p.read(b, pos)
	n.right.loc = unsafe.Pointer(p)
	n.numNodes = binary.BigEndian.Uint64(b[pos : pos+8])
	pos += 8
	n.numBytes = binary.BigEndian.Uint64(b[pos : pos+8])
	pos += 8
	if pos != len(b) {
		return nil, fmt.Errorf("nodeLoc.read() pos: %v didn't match length: %v",
			pos, len(b))
	}
	atomic.StorePointer(&nloc.node, unsafe.Pointer(n))
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
