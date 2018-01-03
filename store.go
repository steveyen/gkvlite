// Package gkvlite provides a simple, ordered, ACID, key-value
// persistence library.  It provides persistent, immutable data
// structure abstrations.
package gkvlite

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
)

// Store defines the store for the nodes
// A persistable store holding collections of ordered keys & values.
type Store struct {
	m sync.Mutex

	// Atomic CAS'ed int64/uint64's must be at the top for 32-bit compatibility.
	size       int64                   // Atomic protected; file size or next write position.
	nodeAllocs uint64                  // Atomic protected; total node allocation stats.
	coll       *map[string]*Collection // Copy-on-write map[string]*Collection.
	file       StoreFile               // When nil, we're memory-only or no persistence.
	callbacks  StoreCallbacks          // Optional / may be nil.
	readOnly   bool                    // When true, Flush()'ing is disallowed.
}

func (s *Store) setColl(n *map[string]*Collection) {
	s.m.Lock()
	defer s.m.Unlock()
	s.coll = n
}

func (s *Store) getColl() *map[string]*Collection {
	s.m.Lock()
	defer s.m.Unlock()
	return s.coll
}

func (s *Store) casColl(o, n *map[string]*Collection) bool {
	s.m.Lock()
	defer s.m.Unlock()
	if s.coll == o {
		s.coll = n
		return true
	}
	return false
}

// StoreFile interface defines the os.File methods we require.  Application
// specific implementations may add concurrency, caching, stats, etc.
type StoreFile interface {
	io.ReaderAt
	io.WriterAt
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
}
// StoreCallbacks provides the interface to the callback mechanism
// Allows applications to override or interpose before/after events.
type StoreCallbacks struct {
	BeforeItemWrite, AfterItemRead ItemCallback

	// Optional callback to allocate an Item with an Item.Key.  If
	// your app uses ref-counting, the returned Item should have
	// logical ref-count of 1.
	ItemAlloc func(c *Collection, keyLength uint32) *Item

	// Optional callback to allow you to track gkvlite's ref-counts on
	// an Item.  Apps might use this for buffer management and putting
	// Item's on a free-list.
	ItemAddRef func(c *Collection, i *Item)

	// Optional callback to allow you to track gkvlite's ref-counts on
	// an Item.  Apps might use this for buffer management and putting
	// Item's on a free-list.
	ItemDecRef func(c *Collection, i *Item)

	// Optional callback to control on-disk size, in bytes, of an item's value.
	ItemValLength func(c *Collection, i *Item) int

	// Optional callback to allow you to write item bytes differently.
	ItemValWrite func(c *Collection, i *Item,
		w io.WriterAt, offset int64) error

	// Optional callback to read item bytes differently.  For example,
	// the app might have an optimization to just remember the reader
	// & file offsets in the item.Transient field for lazy reading.
	ItemValRead func(c *Collection, i *Item,
		r io.ReaderAt, offset int64, valLength uint32) error

	// Invoked when a Store is reloaded (during NewStoreEx()) from
	// disk, this callback allows the user to optionally supply a key
	// comparison func for each collection.  Otherwise, the default is
	// the bytes.Compare func.
	KeyCompareForCollection func(collName string) KeyCompare
}

// ItemCallback defines the function interface to an item callback
type ItemCallback func(*Collection, *Item) (*Item, error)

// VERSION of the file format in use
const VERSION = uint32(4)

// MAGIC_BEG definest the start magic value
var MAGIC_BEG = []byte("0g1t2r")
// MAGIC_END definest the end magic value
var MAGIC_END = []byte("3e4a5p")

var rootsEndLen = 8 + 4 + 2*len(MAGIC_END)
var rootsLen = int64(2*len(MAGIC_BEG) + 4 + 4 + rootsEndLen)

// NewStore return a new store at the requested file
// Provide a nil StoreFile for in-memory-only (non-persistent) usage.
func NewStore(file StoreFile) (*Store, error) {
	return NewStoreEx(file, StoreCallbacks{})
}

// NewStoreEx as NewStore but Expanded
func NewStoreEx(file StoreFile,
	callbacks StoreCallbacks) (*Store, error) {
	coll := make(map[string]*Collection)
	res := &Store{coll: &coll, callbacks: callbacks}
	if file == nil || !reflect.ValueOf(file).Elem().IsValid() {
		return res, nil // Memory-only Store.
	}
	res.file = file
	if err := res.readRoots(); err != nil {
		return nil, err
	}
	return res, nil
}

// SetCollection is used to create a named Collection, or to modify
// the KeyCompare function on an existing Collection.  In either case,
// a new Collection to use is returned.  A newly created Collection
// and any mutations on it won't be persisted until you do a Flush().
func (s *Store) SetCollection(name string, compare KeyCompare) *Collection {
	if compare == nil {
		compare = bytes.Compare
	}
	for {
		orig := s.getColl()
		coll := copyColl(*(*map[string]*Collection)(orig))
		cnew := s.MakePrivateCollection(compare)
		cnew.name = name
		cold := coll[name]
		if cold != nil {
			cnew.rootLock = cold.rootLock
			cnew.root = cold.rootAddRef()
		}
		coll[name] = cnew
		if s.casColl(orig, &coll) {
			cold.closeCollection()
			return cnew
		}
		cnew.closeCollection()
	}
}

// MakePrivateCollection returns a new, unregistered (non-named) collection.  This allows
// advanced users to manage collections of private collections.
func (s *Store) MakePrivateCollection(compare KeyCompare) *Collection {
	if compare == nil {
		compare = bytes.Compare
	}
	return &Collection{
		store:    s,
		compare:  compare,
		rootLock: &sync.Mutex{},
		root:     &rootNodeLoc{refs: 1, root: &empty_nodeLoc},
	}
}

// GetCollection retrieves a named Collection.
func (s *Store) GetCollection(name string) *Collection {
	return (*s.getColl())[name]
}

// GetCollectionNames returns the named collections within the store
func (s *Store) GetCollectionNames() []string {
	return collNames(*s.getColl())
}

func collNames(coll map[string]*Collection) []string {
	res := make([]string, 0, len(coll))
	for name := range coll {
		res = append(res, name)
	}
	sort.Strings(res) // Sorting because common callers need stability.
	return res
}

// RemoveCollection by name from store
// The Collection removal won't be reflected into persistence until
// you do a Flush().  Invoking RemoveCollection(x) and then
// SetCollection(x) is a fast way to empty a Collection.
func (s *Store) RemoveCollection(name string) {
	for {
		orig := s.getColl()
		coll := copyColl(*(*map[string]*Collection)(orig))
		cold := coll[name]
		delete(coll, name)
		if s.casColl(orig, &coll) {
			cold.closeCollection()
			return
		}
	}
}

func copyColl(orig map[string]*Collection) map[string]*Collection {
	res := make(map[string]*Collection)
	for name, c := range orig {
		res[name] = c
	}
	return res
}

// Flush stale data to disk
// Writes (appends) any dirty, unpersisted data to file.  As a
// greater-window-of-data-loss versus higher-performance tradeoff,
// consider having many mutations (Set()'s & Delete()'s) and then
// have a less occasional Flush() instead of Flush()'ing after every
// mutation.  Users may also wish to file.Sync() after a Flush() for
// extra data-loss protection.
func (s *Store) Flush() error {
	if s.readOnly {
		return errors.New("readonly, so cannot Flush()")
	}
	if s.file == nil {
		return errors.New("no file / in-memory only, so cannot Flush()")
	}
	coll := *s.getColl()
	rnls := map[string]*rootNodeLoc{}
	cnames := collNames(coll)
	for _, name := range cnames {
		c := coll[name]
		rnls[name] = c.rootAddRef()
	}
	defer func() {
		for _, name := range cnames {
			coll[name].rootDecRef(rnls[name])
		}
	}()
	for _, name := range cnames {
		if err := coll[name].write(rnls[name].root); err != nil {
			return err
		}
	}
	return s.writeRoots(rnls)
}

// FlushRevert Revert the last Flush(), bringing the Store back to its state at
// its next-to-last Flush() or to an empty Store (with no Collections)
// if there were no next-to-last Flush().  This call will truncate the
// Store file.
func (s *Store) FlushRevert() error {
	if s.file == nil {
		return errors.New("no file / in-memory only, so cannot FlushRevert()")
	}
	orig := s.getColl()
	coll := make(map[string]*Collection)
	if s.casColl(orig, &coll) {
		for _, cold := range *(*map[string]*Collection)(orig) {
			cold.closeCollection()
		}
	}
	if atomic.LoadInt64(&s.size) > rootsLen {
		atomic.AddInt64(&s.size, -1)
	}
	err := s.readRootsScan(true)
	if err != nil {
		return err
	}
	if s.readOnly {
		return nil
	}
	return s.file.Truncate(atomic.LoadInt64(&s.size))
}

// Snapshot a read only copy of current store
// Returns a read-only snapshot, including any mutations on the
// original Store that have not been Flush()'ed to disk yet.  The
// snapshot has its mutations and Flush() operations disabled because
// the original store "owns" writes to the StoreFile.
func (s *Store) Snapshot() (snapshot *Store) {
	coll := copyColl(*s.getColl())
	res := &Store{
		coll:      &coll,
		file:      s.file,
		size:      atomic.LoadInt64(&s.size),
		readOnly:  true,
		callbacks: s.callbacks,
	}
	for _, name := range collNames(coll) {
		collOrig := coll[name]
		coll[name] = &Collection{
			store:    res,
			compare:  collOrig.compare,
			rootLock: collOrig.rootLock,
			root:     collOrig.rootAddRef(),
		}
	}
	return res
}
// Close the store after use
func (s *Store) Close() {
	s.file = nil
	cptr := s.getColl()
	if cptr == nil || !s.casColl(cptr, nil) {
		return
	}
	coll := *(*map[string]*Collection)(cptr)
	for _, name := range collNames(coll) {
		coll[name].closeCollection()
	}
}
// CopyTo copies the store to another file
// Copy all active collections and their items to a different file.
// If flushEvery > 0, then during the item copying, Flush() will be
// invoked at every flushEvery'th item and at the end of the item
// copying.  The copy will not include any old items or nodes so the
// copy should be more compact if flushEvery is relatively large.
func (s *Store) CopyTo(dstFile StoreFile, flushEvery int) (res *Store, err error) {
	dstStore, err := NewStore(dstFile)
	if err != nil {
		return nil, err
	}
	coll := *s.getColl()

	var max_depth uint64
	for _, name := range collNames(coll) {
		srcColl := coll[name]
		dstColl := dstStore.SetCollection(name, srcColl.compare)
		minItem, err := srcColl.MinItem(true)
		if err != nil {
			return nil, err
		}
		if minItem == nil {
			continue
		}
		defer s.ItemDecRef(srcColl, minItem)
		numItems := 0
		var errCopyItem error
		err = srcColl.VisitItemsAscendEx(minItem.Key, true, func(i *Item, depth uint64) bool {
			if errCopyItem = dstColl.SetItem(i); errCopyItem != nil {
				return false
			}
			numItems++
			if depth > max_depth {
				max_depth = depth
			}
			if flushEvery > 0 && numItems%flushEvery == 0 {
				// Flush out some of the read items cached into memory
				srcColl.EvictSomeItems()
				// Flush none persisted items to disk
				if errCopyItem = dstStore.Flush(); errCopyItem != nil {
					return false
				}
			}
			return true
		})
		if err != nil {
			return nil, err
		}
		if errCopyItem != nil {
			return nil, errCopyItem
		}
		if false {
			fmt.Printf("CopyTo cnt = %d, max_depth = %d\n", numItems, max_depth)
		}
	}
	if flushEvery > 0 {
		if err = dstStore.Flush(); err != nil {
			return nil, err
		}
	}
	return dstStore, nil
}

// Stats updates the provided map with statistics.
func (s *Store) Stats(out map[string]uint64) {
	out["fileSize"] = uint64(atomic.LoadInt64(&s.size))
	out["nodeAllocs"] = atomic.LoadUint64(&s.nodeAllocs)
}

func (o *Store) writeRoots(rnls map[string]*rootNodeLoc) error {
	sJSON, err := json.Marshal(rnls)
	if err != nil {
		return err
	}
	offset := atomic.LoadInt64(&o.size)
	length := 2*len(MAGIC_BEG) + 4 + 4 + len(sJSON) + 8 + 4 + 2*len(MAGIC_END)
	b := bytes.NewBuffer(make([]byte, length)[:0])
	b.Write(MAGIC_BEG)
	b.Write(MAGIC_BEG)
	binary.Write(b, binary.BigEndian, uint32(VERSION))
	binary.Write(b, binary.BigEndian, uint32(length))
	b.Write(sJSON)
	binary.Write(b, binary.BigEndian, int64(offset))
	binary.Write(b, binary.BigEndian, uint32(length))
	b.Write(MAGIC_END)
	b.Write(MAGIC_END)
	if _, err := o.file.WriteAt(b.Bytes()[:length], offset); err != nil {
		return err
	}
	atomic.StoreInt64(&o.size, offset+int64(length))
	return nil
}

func (o *Store) readRoots() error {
	finfo, err := o.file.Stat()
	if err != nil {
		return err
	}
	atomic.StoreInt64(&o.size, finfo.Size())
	if o.size <= 0 {
		return nil
	}
	return o.readRootsScan(false)
}

func (o *Store) readRootsScan(defaultToEmpty bool) (err error) {
	rootsEnd := make([]byte, rootsEndLen)
	for {
		for { // Scan backwards for MAGIC_END.
			if atomic.LoadInt64(&o.size) <= rootsLen {
				if defaultToEmpty {
					atomic.StoreInt64(&o.size, 0)
					return nil
				}
				return errors.New("couldn't find roots; file corrupted or wrong?")
			}
			if _, err := o.file.ReadAt(rootsEnd,
				atomic.LoadInt64(&o.size)-int64(len(rootsEnd))); err != nil {
				return err
			}
			if bytes.Equal(MAGIC_END, rootsEnd[8+4:8+4+len(MAGIC_END)]) &&
				bytes.Equal(MAGIC_END, rootsEnd[8+4+len(MAGIC_END):]) {
				break
			}
			atomic.AddInt64(&o.size, -1) // TODO: optimizations to scan backwards faster.
		}
		// Read and check the roots.
		var offset int64
		var length uint32
		endBuf := bytes.NewBuffer(rootsEnd)
		err = binary.Read(endBuf, binary.BigEndian, &offset)
		if err != nil {
			return err
		}
		if err = binary.Read(endBuf, binary.BigEndian, &length); err != nil {
			return err
		}
		if offset >= 0 && offset < atomic.LoadInt64(&o.size)-int64(rootsLen) &&
			length == uint32(atomic.LoadInt64(&o.size)-offset) {
			data := make([]byte, atomic.LoadInt64(&o.size)-offset-int64(len(rootsEnd)))
			if _, err := o.file.ReadAt(data, offset); err != nil {
				return err
			}
			if bytes.Equal(MAGIC_BEG, data[:len(MAGIC_BEG)]) &&
				bytes.Equal(MAGIC_BEG, data[len(MAGIC_BEG):2*len(MAGIC_BEG)]) {
				var version, length0 uint32
				b := bytes.NewBuffer(data[2*len(MAGIC_BEG):])
				if err = binary.Read(b, binary.BigEndian, &version); err != nil {
					return err
				}
				if err = binary.Read(b, binary.BigEndian, &length0); err != nil {
					return err
				}
				if version != VERSION {
					return fmt.Errorf("version mismatch: "+
						"current version: %v != found version: %v", VERSION, version)
				}
				if length0 != length {
					return fmt.Errorf("length mismatch: "+
						"wanted length: %v != found length: %v", length0, length)
				}
				m := make(map[string]*Collection)
				if err = json.Unmarshal(data[2*len(MAGIC_BEG)+4+4:], &m); err != nil {
					return err
				}
				for collName, t := range m {
					t.name = collName
					t.store = o
					if o.callbacks.KeyCompareForCollection != nil {
						t.compare = o.callbacks.KeyCompareForCollection(collName)
					}
					if t.compare == nil {
						t.compare = bytes.Compare
					}
				}
				o.setColl(&m)
				return nil
			} // else, perhaps value was unlucky in having MAGIC_END's.
		} // else, perhaps a gkvlite file was stored as a value.
		atomic.AddInt64(&o.size, -1) // Roots were wrong, so keep scanning.
	}
}
// ItemAlloc allocates an item in the requested collection
func (o *Store) ItemAlloc(c *Collection, keyLength uint32) *Item {
	if o.callbacks.ItemAlloc != nil {
		return o.callbacks.ItemAlloc(c, keyLength)
	}
	return &Item{Key: make([]byte, keyLength)}
}
// ItemAddRef allows callbacks to be called on item add
func (o *Store) ItemAddRef(c *Collection, i *Item) {
	if o.callbacks.ItemAddRef != nil {
		o.callbacks.ItemAddRef(c, i)
	}
}
// ItemDecRef allows callbacks to be called on item remove
func (o *Store) ItemDecRef(c *Collection, i *Item) {
	if o.callbacks.ItemDecRef != nil {
		o.callbacks.ItemDecRef(c, i)
	}
}
// ItemValRead reads the value of an item
func (o *Store) ItemValRead(c *Collection, i *Item,
	r io.ReaderAt, offset int64, valLength uint32) error {
	if o.callbacks.ItemValRead != nil {
		return o.callbacks.ItemValRead(c, i, r, offset, valLength)
	}
	i.Val = make([]byte, valLength)
	_, err := r.ReadAt(i.Val, offset)
	return err
}
// ItemValWrite writes the value to an item
func (o *Store) ItemValWrite(c *Collection, i *Item, w io.WriterAt, offset int64) error {
	if o.callbacks.ItemValWrite != nil {
		return o.callbacks.ItemValWrite(c, i, w, offset)
	}
	_, err := w.WriteAt(i.Val, offset)
	return err
}
