gkvlite
-------

gkvlite is a simple, ordered, key-value persistence library for Go

Overview
========

gkvlite is a library that provides a simple key-value persistence
implementation, inspired by SQLite and CouchDB/Couchstore.

gkvlite has the following features...

* 100% implemented in the Go Language (golang).
* Open source license - MIT.
* Keys are ordered, so range iterations are supported.
* A key is a []byte.
* A value is a []byte.
* On-disk storage for a "Store" is a single file.
* Multiple key-value Collections are supported in a single storage
  file.  That is, one Store can have zero or more Collections.  And, a
  Collection can have zero or more Items (key-value).
* Append-only, copy-on-write design for robustness to
  crashes/power-loss.
* Atomicity - all unpersisted changes from all Collections during a
  Store.Flush() will be persisted atomically.  All changes will either
  be committed or be rolled back.
* Consistency - simple key-value level consistency is supported.
* Isolation - mutations won't affect snapshots.
* Durability - you control when you want to Flush() to disk.
* O(log N) performance for item retrieval, insert, update, delete.
* O(log N) performance to find the smallest or largest items (by key).
* Range iteration performance is same as binary tree traversal
  performance.
* In general, performance is similar to probabilistic balanced
  binary tree performance.
* Non-persistable snapshots are supported, where you can still
  "scribble" on your snapshots with more (non-persistable)
  mutations. These scribbles on snapshots won't affect (are isolated
  from) the original Store.  And, mutations on the original Store
  won't be seen by snapshots.
* Snapshot creation is a fast O(1) operation per Collection.
* In-memory-only mode is supported, when you can use the same API but
  without any persistence.
* Single-threaded.  Users are encouraged to use Go channels or their
  own locking to serialize access to a Store.
* You provide the os.File - this library just uses the os.File you
  provide.
* You provide the os.File.Sync() - if you want to fsync your file,
  call file.Sync() after you do a Flush().
* You control when you want to Flush() changes to disk, so your
  application can address its performance-vs-safety tradeoffs
  appropriately.
* You can retrieve just keys only, to save I/O & memory resources,
  especially when you have many large values and you just need to
  retrieve only keys from disk for some requests.
* You can supply your own KeyCompare function to order items however
  you want.  The default is bytes.Compare().
* You can control item priority to access hotter items faster by
  shuffling them closer to the tops of balanced binary trees (warning:
  intricate/advanced tradeoffs here).
* Errors from file operations are propagated all the way back to your
  code, so your application can respond appropriately.
* Small - the implementation is a single file < 1000 lines of code.
* Tested - "go test" unit tests.

LICENSE
=======

Open source - MIT licensed.

Examples
========

    import (
        "os"
        "github.com/steveyen/gkvlite"
    )
    
	f, err := os.Create("/tmp/test.gkvlite")
	s, err := gkvlite.NewStore(f)
	c := s.SetCollection("cars", nil)
    
    // You can also retrieve the collection, where c == cc.
    cc := s.GetCollection("cars")
    
    // Insert values.
    c.Set([]byte("tesla"), []byte("$$$"))
    c.Set([]byte("mercedes"), []byte("$$"))
    c.Set([]byte("bmw"), []byte("$"))
    
    // Replace values.
    c.Set([]byte("tesla"), []byte("$$$$"))
    
    // Retrieve values.
    mercedesPrice, err := c.Get([]byte("mercedes"))
    thisIsNil, err := c.Get([]byte("the-lunar-rover"))
    
    c.VisitItemsAscend([]byte("ford"), func(i *gkvlite.Item) bool {
        // This visitor callback will be invoked with every item
        // with key "ford" and onwards, in key-sorted order.
        // So: "mercedes", "tesla" are visited, in that ascending order,
        // but not "bmw".
        // If we want to stop visiting, return false;
        // otherwise a true return result means keep visiting items.
        return true
    })
    
    // Let's get a read-only snapshot.
    snap := s.Snapshot()
    
    // The snapshot won't see modifications against the original Store.
    err = c.Delete([]byte("mercedes"))
    mercedesIsNil, err = c.Get([]byte("mercedes"))
    mercedesPriceFromSnaphot, err = snap.Get([]bytes("mercedes"))
    
    // Persist all the changes to disk.  This will not affect the
    // snapshot's "view", though, due to snapshot isolation.
    err := s.Flush()
    
    f.Sync() // Some applications may also want to fsync the underlying file.
    
    // Now, other file readers can see the data, too.
    f2, err := os.Open("/tmp/test.gkvlite")
    s2, err := gkvlite.NewStore(f2)
    c2 := s.GetCollection([]byte("cars"))
    
    bmwPrice := c2.Get([]byte("bmw"))

Implementation / design
=======================

The fundamental datastructure is an immutable treap.  When used with
random item priorities, treaps have probabilistic balanced tree
behavior with the usual O(log N) performance bounds expected of
balanced binary trees.

The persistence design is append-only, using ideas from Apache CouchDB
and Couchstore / Couchbase, providing a simple approach to resilience
and fast restarts in the face of process or machine crashes.  On
re-opening a file, the implementation scans the file backwards looking
for the last good root record and logically "truncates" the file at
that point.  New mutations are appended from that last good root
location.  This follows the "the log is the database" approach of
CouchDB / Couchstore / Couchbase.

TRADEOFF: the append-only persistence design means file sizes will
grow until there's a compaction.  Every mutation (when Flush()'ed)
means the data file will grow.

The immutable, copy-on-write treap plus the append-only persistence
design allows for easy MVCC snapshotting.

TRADEOFF: the immutable, copy-on-write design means more memory
garbage may be created than other designs, meaning more work for the
garbage collector (GC).
