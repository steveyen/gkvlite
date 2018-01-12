package gkvlite

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
)

// KeyCompare defines a function for custom key comparison
// User-supplied key comparison func should return 0 if a == b,
// -1 if a < b, and +1 if a > b.  For example: bytes.Compare()
type KeyCompare func(a, b []byte) int

// Collection implements a persistable ordered key-values (Item's).
type Collection struct {
	name    string // May be "" for a private collection.
	store   *Store
	compare KeyCompare

	rootLock *sync.Mutex
	root     *rootNodeLoc // Protected by rootLock.

	allocStats AllocStats // User must serialize access (e.g., see locks in alloc.go).

	AppData interface{} // For app-specific data.
}

type rootNodeLoc struct {
	// The rootNodeLoc fields are protected by Collection.rootLock.
	refs int64 // Reference counter.
	root *nodeLoc
	next *rootNodeLoc // For free-list tracking.

	reclaimMark node // Address is used as a sentinel.

	// We might own a reference count on another Collection/rootNodeLoc.
	// When our reference drops to 0 and we're free'd, then also release
	// our reference count on the next guy in the chain.
	chainedCollection  *Collection
	chainedRootNodeLoc *rootNodeLoc

	// More nodes to maybe reclaim when our reference count goes to 0.
	// But they might be repeated, so we scan for them during reclaimation.
	reclaimLater [3]*node
}

// Name returns as a string the name of the collection
func (t *Collection) Name() string {
	return t.name
}

func (t *Collection) closeCollection() { // Just "close" is a keyword.
	if t == nil {
		return
	}
	t.rootLock.Lock()
	r := t.root
	t.root = nil
	t.rootLock.Unlock()
	t.reclaimMarkUpdate(r.root, nil, &r.reclaimMark)
	if r != nil {
		t.rootDecRef(r)
	}
}

// GetItem from the collection by key
// Use withValue of false if you don't
// need the item's value (Item.Val may be nil), which might be able
// to save on I/O and memory resources, especially for large values.
// The returned Item should be treated as immutable.
func (t *Collection) GetItem(key []byte, withValue bool) (i *Item, err error) {
	rnl := t.rootAddRef()
	defer t.rootDecRef(rnl)
	n := rnl.root
	for {
		nNode, err := n.read(t.store)
		if err != nil || n.isEmpty() || nNode == nil {
			return nil, err
		}
		i := &nNode.item
		iItem, err := i.read(t, false)
		if err != nil {
			return nil, err
		}
		if iItem == nil || iItem.Key == nil {
			return nil, errors.New("missing item after item.read() in GetItem()")
		}
		c := t.compare(key, iItem.Key)
		if c < 0 {
			n = &nNode.left
		} else if c > 0 {
			n = &nNode.right
		} else {
			if withValue {
				iItem, err = i.read(t, withValue)
				if err != nil {
					return nil, err
				}
			}
			t.store.ItemAddRef(t, iItem)
			return iItem, nil
		}
	}
}

// Get value by key
// Retrieve a value by its key.  Returns nil if the item is not in the
// collection.  The returned value should be treated as immutable.
func (t *Collection) Get(key []byte) (val []byte, err error) {
	i, err := t.GetItem(key, true)
	if err != nil {
		return nil, err
	}
	if i != nil {
		return i.Val, nil
	}
	return nil, nil
}

// SetItem in a collection
// Replace or insert an item of a given key.
// A random item Priority (e.g., rand.Int31()) will usually work well,
// but advanced users may consider using non-random item priorities
// at the risk of unbalancing the lookup tree.  The input Item instance
// should be considered immutable and owned by the Collection.
func (t *Collection) SetItem(item *Item) (err error) {
	if t.store.readOnly {
		return errors.New("store is read only")
	}
	if item.Key == nil || len(item.Key) > 0xffff || len(item.Key) == 0 ||
		item.Val == nil {
		return errors.New("Item.Key/Val missing or too long")
	}
	if item.Priority < 0 {
		return errors.New("Item.Priority must be non-negative")
	}
	rnl := t.rootAddRef()
	defer t.rootDecRef(rnl)
	root := rnl.root
	n := t.mkNode(nil, nil, nil, 1, uint64(len(item.Key))+uint64(item.NumValBytes(t)))
	t.store.ItemAddRef(t, item)
	n.item.item = item // Avoid garbage via separate init.
	nloc := t.mkNodeLoc(n)
	defer t.freeNodeLoc(nloc)
	r, err := t.store.union(t, root, nloc, &rnl.reclaimMark)
	if err != nil {
		return err
	}
	rnlNew := t.mkRootNodeLoc(r)
	// Can't reclaim n right now because r might point to n.
	rnlNew.reclaimLater[0] = t.reclaimMarkUpdate(nloc,
		&rnl.reclaimMark, &rnlNew.reclaimMark)
	if !t.rootCAS(rnl, rnlNew) {
		return errors.New("concurrent mutation attempted")
	}
	t.rootDecRef(rnl)
	return nil
}

// Set a key and value
// Replace or insert an item of a given key.
func (t *Collection) Set(key []byte, val []byte) error {
	return t.SetItem(&Item{Key: key, Val: val, Priority: rand.Int31()})
}

// Delete an item of a given key.
func (t *Collection) Delete(key []byte) (wasDeleted bool, err error) {
	if t.store.readOnly {
		return false, errors.New("store is read only")
	}
	rnl := t.rootAddRef()
	defer t.rootDecRef(rnl)
	root := rnl.root
	i, err := t.GetItem(key, false)
	if err != nil || i == nil {
		return false, err
	}
	t.store.ItemDecRef(t, i)
	left, middle, right, err := t.store.split(t, root, key, &rnl.reclaimMark)
	if err != nil {
		return false, err
	}
	defer t.freeNodeLoc(left)
	defer t.freeNodeLoc(right)
	defer t.freeNodeLoc(middle)
	if middle.isEmpty() {
		return false, fmt.Errorf("concurrent delete, key: %v", key)
	}
	r, err := t.store.join(t, left, right, &rnl.reclaimMark)
	if err != nil {
		return false, err
	}
	rnlNew := t.mkRootNodeLoc(r)
	// Can't reclaim immediately due to readers.
	rnlNew.reclaimLater[0] = t.reclaimMarkUpdate(left,
		&rnl.reclaimMark, &rnlNew.reclaimMark)
	rnlNew.reclaimLater[1] = t.reclaimMarkUpdate(right,
		&rnl.reclaimMark, &rnlNew.reclaimMark)
	rnlNew.reclaimLater[2] = t.reclaimMarkUpdate(middle,
		&rnl.reclaimMark, &rnlNew.reclaimMark)
	t.markReclaimable(rnlNew.reclaimLater[2], &rnlNew.reclaimMark)
	if !t.rootCAS(rnl, rnlNew) {
		return false, errors.New("concurrent mutation attempted")
	}
	t.rootDecRef(rnl)
	return true, nil
}

// MinItem returns the minimum item
// Retrieves the item with the "smallest" key.
// The returned item should be treated as immutable.
func (t *Collection) MinItem(withValue bool) (*Item, error) {
	return t.store.walk(t, withValue,
		func(n *node) (*nodeLoc, bool) { return &n.left, true })
}

// MaxItem returns the maximum item
// Retrieves the item with the "largest" key.
// The returned item should be treated as immutable.
func (t *Collection) MaxItem(withValue bool) (*Item, error) {
	return t.store.walk(t, withValue,
		func(n *node) (*nodeLoc, bool) { return &n.right, true })
}

// EvictSomeItems from the tree trading performance for memory
// Evict some clean items found by randomly walking a tree branch.
// For concurrent users, only the single mutator thread should call
// EvictSomeItems(), making it serialized with mutations.
func (t *Collection) EvictSomeItems() (numEvicted uint64) {
	if t.store.readOnly {
		return 0
	}
	i, err := t.store.walk(t, false, func(n *node) (*nodeLoc, bool) {
		if j := n.Evict(); j != nil {
			t.store.ItemDecRef(t, j)
			numEvicted++
		}
		next := &n.left
		if (rand.Int() & 0x01) == 0x01 {
			next = &n.right
		}
		if next.isEmpty() {
			return nil, false
		}
		return next, true
	})
	if i != nil && err != nil {
		t.store.ItemDecRef(t, i)
	}
	return numEvicted
}

// ItemVisitor is a function type for things that can visit an item
// return true if you wish to keep visiting
type ItemVisitor func(i *Item) bool

// ItemVisitorEx is a function type for things that can visit an item
// as ItemVisitor but with current depth information provided
// return true if you wish to keep visiting
type ItemVisitorEx func(i *Item, depth uint64) bool

// VisitItemsAscend visits the collection's items in ascending order
// Specifically visit items greater-than-or-equal to the target key in ascending order.
func (t *Collection) VisitItemsAscend(target []byte, withValue bool, v ItemVisitor) error {
	return t.VisitItemsAscendEx(target, withValue,
		func(i *Item, depth uint64) bool { return v(i) })
}

// VisitItemsDescend visits the collection's items in descending order
// Specifically visit items less-than the target key in descending order.
func (t *Collection) VisitItemsDescend(target []byte, withValue bool, v ItemVisitor) error {
	return t.VisitItemsDescendEx(target, withValue,
		func(i *Item, depth uint64) bool { return v(i) })
}

// MaxBlockCnt is the maximum number of blocks we will split the collection into
const MaxBlockCnt = 1024

// BlockMangler will possibly re-arrange the order of the blocks
type BlockMangler func([][]byte) [][]byte

func (t *Collection) VisitItemsRandom(
	visitor ItemVisitorEx,
) error {
	numBlocks, lenBlock, err := t.determineBlocks()
	//log.Println("There are ", numBlocks, " of Length ", lenBlock)
	if err != nil {
		return err
	}
	if (lenBlock < 1) || (numBlocks < 1) {
		return fmt.Errorf("Impossible block sizes,%d,%d", lenBlock, numBlocks)
	}
	blockStore := make([][]byte, 0, numBlocks)

	var j int
	v := func(i *Item, depth uint64) bool {
		if j == 0 {
			blockStore = append(blockStore, i.Key)
			j = 1
		} else if j >= lenBlock {
			j = 0
		} else {
			j++
		}
		return true
	}
	si, err := t.MinItem(false)
	if err != nil {
		return err
	}
	err = t.VisitItemsAscendEx(si.Key, false, v)
	if err != nil {
		return err
	}
	blockStore = RandBm(blockStore)

	fmt.Println("lenBlock -s:", lenBlock)
	for j := lenBlock + 1; j > 0; j-- {
		for i, si := range blockStore {
			first := true
			vis := func(itm *Item, depth uint64) bool {

				if first {
					first = false
					return visitor(itm, depth)
				}
				first = true
				blockStore[i] = itm.Key
				return false
			}
			//ii, _ := strconv.Atoi(string(si))
			//log.Println("Starting a visit at:", ii)
			err = t.VisitItemsAscendEx(si, true, vis)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// VisitItemsAscendBlockEx divides the collection keys into blocks
// and visits the items in those
// This is useful not for speed, but if you need a none linear visit order
func (t *Collection) VisitItemsAscendBlockEx(
	withValue bool,
	blockMan BlockMangler,
	visitor ItemVisitorEx,
) error {
	numBlocks, lenBlock, err := t.determineBlocks()
	//log.Println("There are ", numBlocks, " of Length ", lenBlock)
	if err != nil {
		return err
	}
	if (lenBlock < 1) || (numBlocks < 1) {
		return fmt.Errorf("Impossible block sizes,%d,%d", lenBlock, numBlocks)
	}
	blockStore := make([][]byte, 0, numBlocks)

	var j int
	v := func(i *Item, depth uint64) bool {
		if j == 0 {
			blockStore = append(blockStore, i.Key)
			j = 1
		} else if j >= lenBlock {
			j = 0
		} else {
			j++
		}
		return true
	}
	si, err := t.MinItem(false)
	if err != nil {
		return err
	}
	err = t.VisitItemsAscendEx(si.Key, false, v)
	if err != nil {
		return err
	}
	if blockMan != nil {
		blockStore = blockMan(blockStore)
	}
	for _, si := range blockStore {
		j := 0
		vis := func(i *Item, depth uint64) bool {
			if j > lenBlock {
				panic("impossible")
				return false
			} else if j == (lenBlock) {
				visitor(i, depth)
				return false
			}
			j++
			return visitor(i, depth)
		}
		//ii, _ := strconv.Atoi(string(si))
		//log.Println("Starting a visit at:", ii)
		err = t.VisitItemsAscendEx(si, withValue, vis)
		if err != nil {
			return err
		}
	}
	return nil
}
func (t *Collection) determineBlocks() (num, leng int, err error) {
	var cnt int64
	cnt, err = t.Len()
	if err != nil {
		return 0, 0, err
	}
	if cnt > MaxBlockCnt {
		size := cnt / MaxBlockCnt
		if cnt%MaxBlockCnt != 0 {
			size++
		}
		return MaxBlockCnt, int(size), nil
	}
	return int(cnt), 1, nil
}
func RandBm(slice [][]byte) [][]byte {
	for i := range slice {
		j := rand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
	return slice
}

// Len returns the number of items in the collection
// beware this requires walking the whole structure
// Len is lengthy
func (t *Collection) Len() (l int64, err error) {
	visitor := func(i *Item, depth uint64) bool {
		l++
		return true
	}
	si, err := t.MinItem(false)
	if err != nil {
		return
	}
	err = t.VisitItemsAscendEx(si.Key, false, visitor)
	return
}

// VisitItemsAscendEx items greater-than-or-equal to the target key in ascending order; with depth info.
func (t *Collection) VisitItemsAscendEx(target []byte, withValue bool,
	visitor ItemVisitorEx) error {
	rnl := t.rootAddRef()
	defer t.rootDecRef(rnl)

	var prevVisitItem *Item
	var errCheckedVisitor error

	checkedVisitor := func(i *Item, depth uint64) bool {
		if prevVisitItem != nil && t.compare(prevVisitItem.Key, i.Key) > 0 {
			errCheckedVisitor = fmt.Errorf("corrupted / out-of-order index"+
				", key: %s vs %s, coll: %p, collName: %s, store: %p, storeFile: %v",
				string(prevVisitItem.Key), string(i.Key), t, t.name, t.store, t.store.file)
			return false
		}
		prevVisitItem = i
		return visitor(i, depth)
	}

	_, err := t.store.visitNodes(t, rnl.root,
		target, withValue, checkedVisitor, 0, ascendChoice)
	if errCheckedVisitor != nil {
		return errCheckedVisitor
	}
	return err
}

// VisitItemsDescendEx items less-than the target key in descending order; with depth info.
func (t *Collection) VisitItemsDescendEx(target []byte, withValue bool,
	visitor ItemVisitorEx) error {
	rnl := t.rootAddRef()
	defer t.rootDecRef(rnl)

	_, err := t.store.visitNodes(t, rnl.root,
		target, withValue, visitor, 0, descendChoice)
	return err
}

func ascendChoice(cmp int, n *node) (bool, *nodeLoc, *nodeLoc) {
	return cmp <= 0, &n.left, &n.right
}

func descendChoice(cmp int, n *node) (bool, *nodeLoc, *nodeLoc) {
	return cmp > 0, &n.right, &n.left
}

// GetTotals returns total number of items and total key bytes plus value bytes.
func (t *Collection) GetTotals() (numItems uint64, numBytes uint64, err error) {
	rnl := t.rootAddRef()
	defer t.rootDecRef(rnl)
	n := rnl.root
	nNode, err := n.read(t.store)
	if err != nil || n.isEmpty() || nNode == nil {
		return 0, 0, err
	}
	return nNode.numNodes, nNode.numBytes, nil
}

// MarshalJSON is a standard JSON marshaller
// Returns JSON representation of root node file location.
func (t *Collection) MarshalJSON() ([]byte, error) {
	rnl := t.rootAddRef()
	defer t.rootDecRef(rnl)
	return rnl.MarshalJSON()
}

// MarshalJSON is a standard JSON marshaller
// Returns JSON representation of root node file location.
func (rnl *rootNodeLoc) MarshalJSON() ([]byte, error) {
	loc := rnl.root.Loc()
	if loc.isEmpty() {
		return json.Marshal(plocEmpty)
	}
	return json.Marshal(loc)
}

// UnmarshalJSON is a standard JSON unmarshaller
// Unmarshals JSON representation of root node file location.
func (t *Collection) UnmarshalJSON(d []byte) error {
	p := ploc{}
	if err := json.Unmarshal(d, &p); err != nil {
		return err
	}
	if t.rootLock == nil {
		t.rootLock = &sync.Mutex{}
	}
	nloc := t.mkNodeLoc(nil)
	nloc.loc = &p
	if !t.rootCAS(nil, t.mkRootNodeLoc(nloc)) {
		return errors.New("Concurrent mutation during UnmarshalJSON()")
	}
	return nil
}

// AllocStats returns the allocation stats for the collection
func (t *Collection) AllocStats() (res AllocStats) {
	withAllocLocks(func() { res = t.allocStats })
	return res
}

// Write dirty items of a collection BUT (WARNING) does NOT write new
// root records.  Use Store.Flush() to write root records, which would
// make these writes visible to the next file re-opening/re-loading.
func (t *Collection) Write() error {
	if t.store.readOnly {
		return errors.New("store is read only")
	}
	rnl := t.rootAddRef()
	defer t.rootDecRef(rnl)
	return t.write(rnl.root)
}

func (t *Collection) write(nloc *nodeLoc) error {
	if err := t.writeItems(nloc); err != nil {
		return err
	}
	if err := t.writeNodes(nloc); err != nil {
		return err
	}
	return nil
}

func (t *Collection) writeItems(nloc *nodeLoc) (err error) {
	if nloc == nil || !nloc.Loc().isEmpty() {
		return nil // Write only unpersisted items of non-empty, unpersisted nodes.
	}
	node := nloc.Node()
	if node == nil {
		return nil
	}
	if err = t.writeItems(&node.left); err != nil {
		return err
	}
	if err = node.item.write(t); err != nil { // Write items in key order.
		return err
	}
	return t.writeItems(&node.right)
}

func (t *Collection) writeNodes(nloc *nodeLoc) (err error) {
	if nloc == nil || !nloc.Loc().isEmpty() {
		return nil // Write only non-empty, unpersisted nodes.
	}
	node := nloc.Node()
	if node == nil {
		return nil
	}
	if err = t.writeNodes(&node.left); err != nil {
		return err
	}
	if err = t.writeNodes(&node.right); err != nil {
		return err
	}
	return nloc.write(t.store) // Write nodes in children-first order.
}

func (t *Collection) rootCAS(prev, next *rootNodeLoc) bool {
	t.rootLock.Lock()
	defer t.rootLock.Unlock()

	if t.root != prev {
		return false // TODO: Callers need to release resources.
	}
	t.root = next

	if prev != nil && prev.refs > 2 {
		// Since the prev is in-use, hook up its chain to disallow
		// next's nodes from being reclaimed until prev is done.
		if prev.chainedCollection != nil ||
			prev.chainedRootNodeLoc != nil {
			panic(fmt.Sprintf("chain already taken, coll: %v", t.Name()))
		}
		prev.chainedCollection = t
		prev.chainedRootNodeLoc = t.root
		t.root.refs++ // This ref is owned by prev.
	}

	return true
}

func (t *Collection) rootAddRef() *rootNodeLoc {
	t.rootLock.Lock()
	defer t.rootLock.Unlock()
	t.root.refs++
	return t.root
}

func (t *Collection) rootDecRef(r *rootNodeLoc) {
	t.rootLock.Lock()
	freeNodeLock.Lock()
	t.rootDecRefUnlocked(r)
	freeNodeLock.Unlock()
	t.rootLock.Unlock()
}

func (t *Collection) rootDecRefUnlocked(r *rootNodeLoc) {
	r.refs--
	if r.refs > 0 {
		return
	}
	if r.chainedCollection != nil && r.chainedRootNodeLoc != nil {
		r.chainedCollection.rootDecRefUnlocked(r.chainedRootNodeLoc)
	}
	t.reclaimNodesUnlocked(r.root.Node(), &r.reclaimLater, &r.reclaimMark)
	for i := 0; i < len(r.reclaimLater); i++ {
		if r.reclaimLater[i] != nil {
			t.reclaimNodesUnlocked(r.reclaimLater[i], nil, &r.reclaimMark)
			r.reclaimLater[i] = nil
		}
	}
	t.freeNodeLoc(r.root)
	t.freeRootNodeLoc(r)
}
