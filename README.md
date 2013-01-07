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
* On-disk storage for a "Store" is a single file.
* ACID properties are supported via a simple, append-only,
  copy-on-write storage design.

Key concepts
============

* Multiple key-value Collections are supported in a single storage
  file (a Store).
* That is, one Store can have zero or more Collections.
* And, a Collection can have zero or more Items (key-value).
* A key is a []byte, max length 64KB.
* A value is a []byte, max length 4GB.

ACID properties
===============

* Atomicity - all unpersisted changes from all Collections during a
  Store.Flush() will be persisted atomically.  All changes will either
  be committed or be rolled back.
* Consistency - simple key-value level consistency is supported.
* Isolation - mutations won't affect snapshots.
* Durability - you control when you want to Flush() to disk, so your
  application can address its performance-vs-safety tradeoffs
  appropriately.

Performance
===========

* O(log N) performance for item retrieval, insert, update, delete.
* O(log N) performance to find the smallest or largest items (by key).
* Range iteration performance is same as binary tree traversal
  performance.
* In general, performance is similar to probabilistic balanced
  binary tree performance.
* You can retrieve just keys only, to save I/O & memory resources,
  especially when you have many large values and you just need to
  retrieve only keys from disk for some requests.

Concurrency
===========

gkvlite is single-threaded.  Users are encouraged to use Go channels
or their own locking to serialize access to a Store.

Snapshots
=========

* Non-persistable snapshots are supported, where you can still
  "scribble" on your snapshots with more (non-persistable)
  updates. These scribbles on snapshots won't affect (are isolated
  from) the original Store.
* And, mutations on the original Store won't be seen by snapshots.
* Snapshot creation is a fast O(1) operation per Collection.

Other features
==============

* In-memory-only mode is supported, where you can use the same API but
  without any persistence.
* You provide the os.File - this library just uses the os.File you
  provide.
* You provide the os.File.Sync() - if you want to fsync your file,
  call file.Sync() after you do a Flush().
* Similar to SQLite's VFS feature, you can supply your own StoreFile
  interface implementation instead of an actual os.File, for your own
  advanced testing or file interposing needs.
* You can supply your own KeyCompare function to order items however
  you want.  The default is bytes.Compare().
* You can control item priority to access hotter items faster by
  shuffling them closer to the top of balanced binary trees (warning:
  intricate/advanced tradeoffs here).
* Errors from file operations are propagated all the way back to your
  code, so your application can respond appropriately.
* Small - the implementation is a single file < 1000 lines of code.
* Tested - "go test" unit tests.
* Docs - "go doc" documentation.

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
    
    // One of the most priceless cars is not in the collection.
    thisIsNil, err := c.Get([]byte("the-apollo-15-moon-buggy"))
    
    // Iterate through items.
    c.VisitItemsAscend([]byte("ford"), func(i *gkvlite.Item) bool {
        // This visitor callback will be invoked with every item
        // with key "ford" and onwards, in key-sorted order.
        // So: "mercedes", "tesla" are visited, in that ascending order,
        // but not "bmw".
        // If we want to stop visiting, return false;
        // otherwise return true to keep visiting.
        return true
    })
    
    // Let's get a snapshot.
    snap := s.Snapshot()
    snapCars := snap.GetCollection("cars")
    
    // The snapshot won't see modifications against the original Store.
    c.Delete([]byte("mercedes"))
    mercedesIsNil, err := c.Get([]byte("mercedes"))
    mercedesPriceFromSnapshot, err := snapCars.Get([]bytes("mercedes"))
    
    // Persist all the changes to disk.
    s.Flush()
    
    f.Sync() // Some applications may also want to fsync the underlying file.
    
    // Now, other file readers can see the data, too.
    f2, err := os.Open("/tmp/test.gkvlite")
    s2, err := gkvlite.NewStore(f2)
    c2 := s.GetCollection("cars")
    
    bmwPrice := c2.Get([]byte("bmw"))

Tips
====

Because all collections are persisted atomically when you flush a
store to disk, you can implement consistent secondary indexes by
maintaining additional collections per store.  For example, a "users"
collection can hold a JSON document per user, keyed by userId.
Another "userEmails" collection can be used like a secondary index,
keyed by "emailAddress:userId", with empty values (e.g., []byte{}).

Implementation / design
=======================

The fundamental data structure is an immutable treap (tree + heap).
When used with random heap item priorities, treaps have probabilistic
balanced tree behavior with the usual O(log N) performance bounds
expected of balanced binary trees.

The persistence design is append-only, using ideas from Apache CouchDB
and Couchstore / Couchbase, providing a simple approach to reaching
ACID properties in the face of process or machine crashes.  On
re-opening a file, the implementation scans the file backwards looking
for the last good root record and logically "truncates" the file at
that point.  New mutations are appended from that last good root
location.  This follows the MVCC (multi-version concurrency control)
and "the log is the database" approach of CouchDB / Couchstore /
Couchbase.

TRADEOFF: the append-only persistence design means file sizes will
grow until there's a compaction.  To get a compacted file, use
CopyTo() with a high "flushEvery" argument.

TRADEOFF: the append-only design means it's possible for an advanced
adversary to corrupt a gkvlite file by cleverly storing the bytes of a
valid gkvlite root record as a value; however, they would need to know
the size of the containing gkvlite database file in order to compute a
valid gkvlite root record and be able to force a process or machine
crash before the next good root record is written/sync'ed.

The immutable, copy-on-write treap plus the append-only persistence
design allows for fast and efficient MVCC snapshots.

TRADEOFF: the immutable, copy-on-write design means more memory
garbage may be created than other designs, meaning more work for the
garbage collector (GC).

TODO / ideas
============

* TODO: Performance: consider splitting item storage from node
  storage, so we're not mixing metadata and data in same cache pages.
  Need to measure how much win this could be in cases like compaction.
  Tradeoff as this could mean no more single file simplicity.

* TODO: Allow snapshots to be concurrent, accessible by separate
  goroutines.

* TODO: Allow mutability for less garbage, perhaps switching to
  immutable only when there are in-use snapshots.  This probably won't
  be a win if there are always active snapshots.

* TODO: Keep stats on misses, disk fetches & writes, tree depth, etc.

* TODO: Provide O(1) collection copying.

* TODO: Provide O(log N) collection spliting.

* TODO: Provide O(1) MidItem() or TopItem() implementation, so that
  users can split collections at decent points.

* TODO: Provide item priority shifting during CopyTo().

* TODO: Allow more fine-grained cached item and node eviction.  Node
  and item objects are current cached in-memory by gkvlite for higher
  retrieval performance, only for the nodes & items that you either
  have updated or have fetched from disk.  Certain applications may
  need that memory instead, though, for more important tasks.  The
  current sledgehammer workaround is to drop all your references to
  any relevant Stores and Collections, let GC reclaim memory, and
  start brand new Store/Collection references.

* See more TODO's throughout codebase / grep.
