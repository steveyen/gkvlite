gkvlite
-------

gkvlite is a simple, ordered, ACID, key-value persistence library for Go.

[![GoDoc](https://godoc.org/github.com/steveyen/gkvlite?status.svg)](https://godoc.org/github.com/steveyen/gkvlite) [![Build Status](https://drone.io/github.com/steveyen/gkvlite/status.png)](https://drone.io/github.com/steveyen/gkvlite/latest) [![Coverage Status](https://coveralls.io/repos/steveyen/gkvlite/badge.png)](https://coveralls.io/r/steveyen/gkvlite)

Overview
========

gkvlite is a library that provides a simple key-value persistence
store, inspired by SQLite and CouchDB/Couchstore.

gkvlite has the following features...

* 100% implemented in the Go Language (golang).
* Open source license - MIT.
* Keys are ordered, so range scans are supported.
* On-disk storage for a "Store" is a single file.
* ACID properties are supported via a simple, append-only,
  copy-on-write storage design.
* Concurrent goroutine support (multiple readers, single mutator).

Key concepts
============

* A Store is held in a single file.
* A Store can have zero or more Collections.
* A Collection can have zero or more Items.
* An Item is a key and value.
* A key is a []byte, max length 64KB (length is uint16).
* A value is a []byte, max length 4GB (length is uint32).

ACID properties
===============

* Atomicity - all unpersisted changes from all Collections during a
  Store.Flush() will be persisted atomically.
* Consistency - simple key-value level consistency is supported.
* Isolation - mutations won't affect concurrent readers or snapshots.
* Durability - you control when you want to Flush() & fsync
  so your application can address its performance-vs-safety tradeoffs
  appropriately.

Performance
===========

* In general, performance is similar to probabilistic balanced
  binary tree performance.
* O(log N) performance for item retrieval, insert, update, delete.
* O(log N) performance to find the smallest or largest items (by key).
* Range iteration performance is same as binary tree traversal
  performance.
* You can optionally retrieve just keys only, to save I/O & memory
  resources.

Snapshots
=========

* Read-only Store snapshots are supported.
* Mutations on the original Store won't be seen by snapshots.
* Snapshot creation is a fast O(1) operation per Collection.

Concurrency
===========

The simplest way to use gkvlite is in single-threaded fashion, such as
by using a go channel or other application-provided locking to
serialize access to a Store.

More advanced users may want to use gkvlite's support for concurrent
goroutines.  The idea is that multiple read-only goroutines, a single
read-write (mutation) goroutine, and a single persistence (flusher)
goroutine do not need to block each other.

More specifically, you should have only a single read-write (or
mutation) goroutine per Store, and should have only a single
persistence goroutine per Store (doing Flush()'s).  And, you may have
multiple, concurrent read-only goroutines per Store (doing read-only
operations like Get()'s, Visit()'s, Snapshot()'s, CopyTo()'s, etc).

A read-only goroutine that performs a long or slow read operation,
like a Get() that must retrieve from disk or a range scan, will see a
consistent, isolated view of the collection.  That is, mutations that
happened after the slow read started will not be seen by the reader.

IMPORTANT: In concurrent usage, the user must provide a StoreFile
implementation that is concurrent safe.

Note that os.File is not a concurrent safe implementation of the
StoreFile interface.  You will need to provide your own implementation
of the StoreFile interface, such as by using a channel to serialize
StoreFile method invocations.

Finally, advanced users may also use a read-write (mutation) goroutine
per Collection instead of per Store.  There should only be, though,
only a single persistence (flusher) goroutine per Store.

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
  advanced testing or I/O interposing needs (e.g., compression,
  checksums, I/O statistics, caching, enabling concurrency, etc).
* You can specify your own KeyCompare function.  The default is
  bytes.Compare().  See also the
  StoreCallbacks.KeyCompareForCollection() callback function.
* Collections are written to file sorted by Collection name.  This
  allows users with advanced concurrency needs to reason about how
  concurrent flushes interact with concurrent mutations.  For example,
  if you have a main data collection and a secondary-index collection,
  with clever collection naming you can know that the main collection
  will always be "ahead of" the secondary-index collection even with
  concurrent flushing.
* A Store can be reverted using the FlushRevert() API to revert the
  last Flush().  This brings the state of a Store back to where it was
  as of the next-to-last Flush().  This allows the application to
  rollback or undo changes on a persisted file.
* Reverted snapshots (calling FlushRevert() on a snapshot) does not
  affect (is isolated from) the original Store and does not affect the
  underlying file.  Calling FlushRevert() on the main Store, however,
  will adversely affect any active snapshots; where the application
  should stop using any snapshots that were created before the
  FlushRevert() invocation on the main Store.
* To evict O(log N) number of items from memory, call
  Collection.EvictSomeItems(), which traverses a random tree branch
  and evicts any clean (already persisted) items found during that
  traversal.  Eviction means clearing out references to those clean
  items, which means those items can be candidates for GC.
* You can control item priority to access hotter items faster by
  shuffling them closer to the top of balanced binary trees (warning:
  intricate/advanced tradeoffs here).
* Tree depth is provided by using the VisitItemsAscendEx() or
  VisitItemsDescendEx() methods.
* You can associate transient, ephemeral (non-persisted) data with
  your items.  If you do use the Item.Transient field, you should use
  sync/atomic pointer functions for concurrency correctness.
  In general, Item's should be treated as immutable, except for the
  Item.Transient field.
* Application-level Item.Val buffer management is possible via the
  optional ItemValAddRef/ItemValDecRef() store callbacks, to help
  reduce garbage memory.  Libraries like github.com/steveyen/go-slab
  may be helpful here.
* Errors from file operations are propagated all the way back to your
  code, so your application can respond appropriately.
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
    
    // One of the most priceless vehicles is not in the collection.
    thisIsNil, err := c.Get([]byte("the-apollo-15-moon-buggy"))
    
    // Iterate through items.
    c.VisitItemsAscend([]byte("ford"), true, func(i *gkvlite.Item) bool {
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
    mercedesPriceFromSnapshot, err := snapCars.Get([]byte("mercedes"))
    
    // Persist all the changes to disk.
    s.Flush()
    
    f.Sync() // Some applications may also want to fsync the underlying file.
    
    // Now, other file readers can see the data, too.
    f2, err := os.Open("/tmp/test.gkvlite")
    s2, err := gkvlite.NewStore(f2)
    c2 := s.GetCollection("cars")
    
    bmwPrice, err := c2.Get([]byte("bmw"))

Tips
====

Because all collections are persisted atomically when you flush a
store to disk, you can implement consistent secondary indexes by
maintaining additional collections per store.  For example, a "users"
collection can hold a JSON document per user, keyed by userId.
Another "userEmails" collection can be used like a secondary index,
keyed by "emailAddress:userId", with empty values (e.g., []byte{}).

Bulk inserts or batched mutations are roughly supported in gkvlite
where your application should only occasionally invoke Flush() after N
mutations or after a given amount of time, as opposed to invoking a
Flush() after every Set/Delete().

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

The append-only file format allows the FlushRevert() API (undo the
changes on a file) to have a simple implementation of scanning
backwards in the file for the next-to-last good root record and
physically truncating the file at that point.

TRADEOFF: the append-only design means it's possible for an advanced
adversary to corrupt a gkvlite file by cleverly storing the bytes of a
valid gkvlite root record as a value; however, they would need to know
the size of the containing gkvlite database file in order to compute a
valid gkvlite root record and be able to force a process or machine
crash after their fake root record is written but before the next good
root record is written/sync'ed.

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

* TODO: Performance: persist items as log, and don't write treap nodes
  on every Flush().

* TODO: Keep stats on misses, disk fetches & writes, etc.

* TODO: Provide public API for O(log N) collection spliting & joining.

* TODO: Provide O(1) MidItem() or TopItem() implementation, so that
  users can split collections at decent points.

* TODO: Provide item priority shifting during CopyTo().

* TODO: Build up perfectly balanced treap from large batch of
  (potentially externally) sorted items.

* TODO: Allow users to retrieve an item's value size (in bytes)
  without having to first fetch the item into memory.

* TODO: Allow more fine-grained cached item and node eviction.  Node
  and item objects are currently cached in-memory by gkvlite for
  higher retrieval performance, only for the nodes & items that you
  either have updated or have fetched from disk.  Certain applications
  may need that memory instead, though, for more important tasks.  The
  current coarse workaround is to drop all your references to any
  relevant Stores and Collections, start brand new Store/Collection
  instances, and let GC reclaim memory.

* See more TODO's throughout codebase / grep.
