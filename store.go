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
	"unsafe"
)

// A persistable store holding collections of ordered keys & values.
type Store struct {
	// Atomic CAS'ed int64/uint64's must be at the top for 32-bit compatibility.
	size       int64          // Atomic protected; file size or next write position.
	nodeAllocs uint64         // Atomic protected; total node allocation stats.
	coll       unsafe.Pointer // Copy-on-write map[string]*Collection.
	file       StoreFile      // When nil, we're memory-only or no persistence.
	callbacks  StoreCallbacks // Optional / may be nil.
	readOnly   bool           // When true, Flush()'ing is disallowed.
}

// The StoreFile interface is implemented by os.File.  Application
// specific implementations may add concurrency, caching, stats, etc.
type StoreFile interface {
	io.ReaderAt
	io.WriterAt
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
}

// Allows applications to override or interpose before/after events.
type StoreCallbacks struct {
	BeforeItemWrite, AfterItemRead ItemCallback

	// Optional callback to allocate an Item with an Item.Key.  If
	// your app uses ref-counting, the returned Item should have
	// logical ref-count of 1.
	ItemAlloc func(c *Collection, keyLength uint16) *Item

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

type ItemCallback func(*Collection, *Item) (*Item, error)

const VERSION = uint32(4)

var MAGIC_BEG []byte = []byte("0g1t2r")
var MAGIC_END []byte = []byte("3e4a5p")

var rootsEndLen int = 8 + 4 + 2*len(MAGIC_END)
var rootsLen int64 = int64(2*len(MAGIC_BEG) + 4 + 4 + rootsEndLen)

// Provide a nil StoreFile for in-memory-only (non-persistent) usage.
func NewStore(file StoreFile) (*Store, error) {
	return NewStoreEx(file, StoreCallbacks{})
}

func NewStoreEx(file StoreFile,
	callbacks StoreCallbacks) (*Store, error) {
	coll := make(map[string]*Collection)
	res := &Store{coll: unsafe.Pointer(&coll), callbacks: callbacks}
	if file == nil || !reflect.ValueOf(file).Elem().IsValid() {
		return res, nil // Memory-only Store.
	}
	res.file = file
	if err := res.readRoots(); err != nil {
		return nil, err
	}
	return res, nil
}

// SetCollection() is used to create a named Collection, or to modify
// the KeyCompare function on an existing Collection.  In either case,
// a new Collection to use is returned.  A newly created Collection
// and any mutations on it won't be persisted until you do a Flush().
func (s *Store) SetCollection(name string, compare KeyCompare) *Collection {
	if compare == nil {
		compare = bytes.Compare
	}
	for {
		orig := atomic.LoadPointer(&s.coll)
		coll := copyColl(*(*map[string]*Collection)(orig))
		cnew := s.MakePrivateCollection(compare)
		cnew.name = name
		cold := coll[name]
		if cold != nil {
			cnew.rootLock = cold.rootLock
			cnew.root = cold.rootAddRef()
		}
		coll[name] = cnew
		if atomic.CompareAndSwapPointer(&s.coll, orig, unsafe.Pointer(&coll)) {
			cold.closeCollection()
			return cnew
		}
		cnew.closeCollection()
	}
}

// Returns a new, unregistered (non-named) collection.  This allows
// advanced users to manage collections of private collections.
func (s *Store) MakePrivateCollection(compare KeyCompare) *Collection {
	if compare == nil {
		compare = bytes.Compare
	}
	return &Collection{
		store:    s,
		compare:  compare,
		rootLock: &sync.Mutex{},
		root:     &rootNodeLoc{refs: 1, root: empty_nodeLoc},
	}
}

// Retrieves a named Collection.
func (s *Store) GetCollection(name string) *Collection {
	coll := *(*map[string]*Collection)(atomic.LoadPointer(&s.coll))
	return coll[name]
}

func (s *Store) GetCollectionNames() []string {
	return collNames(*(*map[string]*Collection)(atomic.LoadPointer(&s.coll)))
}

func collNames(coll map[string]*Collection) []string {
	res := make([]string, 0, len(coll))
	for name, _ := range coll {
		res = append(res, name)
	}
	sort.Strings(res) // Sorting because common callers need stability.
	return res
}

// The Collection removal won't be reflected into persistence until
// you do a Flush().  Invoking RemoveCollection(x) and then
// SetCollection(x) is a fast way to empty a Collection.
func (s *Store) RemoveCollection(name string) {
	for {
		orig := atomic.LoadPointer(&s.coll)
		coll := copyColl(*(*map[string]*Collection)(orig))
		cold := coll[name]
		delete(coll, name)
		if atomic.CompareAndSwapPointer(&s.coll, orig, unsafe.Pointer(&coll)) {
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
	coll := *(*map[string]*Collection)(atomic.LoadPointer(&s.coll))
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

// Reverts the last Flush(), bringing the Store back to its state at
// its next-to-last Flush() or to an empty Store (with no Collections)
// if there were no next-to-last Flush().  This call will truncate the
// Store file.
func (s *Store) FlushRevert() error {
	if s.file == nil {
		return errors.New("no file / in-memory only, so cannot FlushRevert()")
	}
	orig := atomic.LoadPointer(&s.coll)
	coll := make(map[string]*Collection)
	if atomic.CompareAndSwapPointer(&s.coll, orig, unsafe.Pointer(&coll)) {
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

// Returns a read-only snapshot, including any mutations on the
// original Store that have not been Flush()'ed to disk yet.  The
// snapshot has its mutations and Flush() operations disabled because
// the original store "owns" writes to the StoreFile.
func (s *Store) Snapshot() (snapshot *Store) {
	coll := copyColl(*(*map[string]*Collection)(atomic.LoadPointer(&s.coll)))
	res := &Store{
		coll:      unsafe.Pointer(&coll),
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

func (s *Store) Close() {
	s.file = nil
	cptr := atomic.LoadPointer(&s.coll)
	if cptr == nil ||
		!atomic.CompareAndSwapPointer(&s.coll, cptr, unsafe.Pointer(nil)) {
		return
	}
	coll := *(*map[string]*Collection)(cptr)
	for _, name := range collNames(coll) {
		coll[name].closeCollection()
	}
}

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
	coll := *(*map[string]*Collection)(atomic.LoadPointer(&s.coll))
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
		var errCopyItem error = nil
		err = srcColl.VisitItemsAscend(minItem.Key, true, func(i *Item) bool {
			if errCopyItem = dstColl.SetItem(i); errCopyItem != nil {
				return false
			}
			numItems++
			if flushEvery > 0 && numItems%flushEvery == 0 {
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
	}
	if flushEvery > 0 {
		if err = dstStore.Flush(); err != nil {
			return nil, err
		}
	}
	return dstStore, nil
}

// Updates the provided map with statistics.
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
				atomic.StorePointer(&o.coll, unsafe.Pointer(&m))
				return nil
			} // else, perhaps value was unlucky in having MAGIC_END's.
		} // else, perhaps a gkvlite file was stored as a value.
		atomic.AddInt64(&o.size, -1) // Roots were wrong, so keep scanning.
	}
}

func (o *Store) ItemAlloc(c *Collection, keyLength uint16) *Item {
	if o.callbacks.ItemAlloc != nil {
		return o.callbacks.ItemAlloc(c, keyLength)
	}
	return &Item{Key: make([]byte, keyLength)}
}

func (o *Store) ItemAddRef(c *Collection, i *Item) {
	if o.callbacks.ItemAddRef != nil {
		o.callbacks.ItemAddRef(c, i)
	}
}

func (o *Store) ItemDecRef(c *Collection, i *Item) {
	if o.callbacks.ItemDecRef != nil {
		o.callbacks.ItemDecRef(c, i)
	}
}

func (o *Store) ItemValRead(c *Collection, i *Item,
	r io.ReaderAt, offset int64, valLength uint32) error {
	if o.callbacks.ItemValRead != nil {
		return o.callbacks.ItemValRead(c, i, r, offset, valLength)
	}
	i.Val = make([]byte, valLength)
	_, err := r.ReadAt(i.Val, offset)
	return err
}

func (o *Store) ItemValWrite(c *Collection, i *Item, w io.WriterAt, offset int64) error {
	if o.callbacks.ItemValWrite != nil {
		return o.callbacks.ItemValWrite(c, i, w, offset)
	}
	_, err := w.WriteAt(i.Val, offset)
	return err
}
