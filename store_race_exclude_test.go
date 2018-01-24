// +build !race

package gkvlite

import (
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// This test intentionally accesses the data structure in a non-race safe way
// The intention being to make sure that the concurrent access is safe.
// The downside is, it needs to be in its own file to be run in a race-checked sim
func TestStoreConcurrentInsertDuringVisits(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "gkvlite_")
	if err != nil {
		log.Fatal(err)
	}
	fname := f.Name()
	if useTestParallel {
		t.Parallel()
	}

	s, _ := NewStore(f)
	x := s.SetCollection("x", nil)
	loadCollection(x, []string{"e", "d", "a", "c", "b", "c", "a"})
	visitExpectCollection(t, x, "a", []string{"a", "b", "c", "d", "e"}, nil)
	err = s.Flush()
	if err != nil {
		log.Fatal("Flush error", err)
	}
	err = f.Close()
	if err != nil {
		log.Fatal("Close error", err)
	}

	f1, _ := os.OpenFile(fname, os.O_RDWR, 0666)
	s1, _ := NewStore(f1)
	x1 := s1.GetCollection("x")

	exp := []string{"a", "b", "c", "d", "e"}
	add := []string{"A", "1", "E", "2", "C"}
	toAdd := int32(0)

	// Concurrent mutations like inserts should not affect a visit()
	// that's already inflight.
	// However only a single agent may mutate at a time
	var wg sync.WaitGroup
	var mutate sync.Mutex
	wg.Add(len(exp))
	visitExpectCollection(t, x1, "a", exp, func(i *Item) {
		go func() {
			a := atomic.AddInt32(&toAdd, 1)
			toAddKey := []byte(add[a-1])
			mutate.Lock()
			defer mutate.Unlock()
			if err := x1.Set(toAddKey, toAddKey); err != nil {
				t.Errorf("expected concurrent set to work on key: %v, got: %v",
					toAddKey, err)
			}
			wg.Done()
		}()
		runtime.Gosched() // Yield to test concurrency.
	})
	wg.Wait()
	err = f1.Close()
	if err != nil {
		log.Fatal("Close error", err)
	}
	reportRemove(fname)
}
