package main

// Test integration of gkvlite with go-slab, using gkvlite's optional
// ItemAddRef/ItemDecRef() callbacks to integrate with a slab memory
// allocator.

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/steveyen/gkvlite"
)

func TestSlabStore(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, err := os.Create(fname)
	if err != nil {
		t.Errorf("expected to create file: " + fname)
	}
	defer os.Remove(fname)

	arena, scb := setupStoreArena(t, 256)
	s, err := gkvlite.NewStoreEx(f, scb)
	if err != nil || s == nil {
		t.Errorf("expected NewStoreEx to work")
	}
	s.SetCollection("x", bytes.Compare)
	x := s.GetCollection("x")
	if x == nil {
		t.Errorf("expected SetColl/GetColl to work")
	}

	b := arena.Alloc(5)
	if b == nil {
		t.Errorf("expected buf")
	}
	copy(b, []byte("hello"))
	i := scb.ItemAlloc(x, 1)
	copy(i.Key, []byte("a"))
	i.Val = b
	i.Priority = 100
	x.SetItem(i)
	scb.ItemDecRef(x, i)

	i = scb.ItemAlloc(x, 3)
	copy(i.Key, []byte("big"))
	i.Val = arena.Alloc(1234)
	i.Priority = 100
	x.SetItem(i)
	scb.ItemDecRef(x, i)

	err = s.Flush()
	if err != nil {
		t.Errorf("expected Flush() to error")
	}
	s.Close()
	f.Close()

	f, _ = os.OpenFile(fname, os.O_RDWR, 0666)
	arena, scb = setupStoreArena(t, 64) // Read with a different buf-size.
	s, err = gkvlite.NewStoreEx(f, scb)
	if err != nil || s == nil {
		t.Errorf("expected NewStoreEx to work")
	}
	x = s.SetCollection("x", bytes.Compare)
	if x == nil {
		t.Errorf("expected SetColl/GetColl to work")
	}
	i, err = x.GetItem([]byte("a"), true)
	if err != nil || i == nil {
		t.Errorf("expected no GetItem() err, got: %v", err)
	}
	if string(i.Val) != "hello" {
		t.Errorf("expected hello, got: %#v", i)
	}
	s.ItemDecRef(x, i)
	i, err = x.GetItem([]byte("big"), true)
	if err != nil || i == nil {
		t.Errorf("expected no GetItem() err, got: %v", err)
	}
	if len(i.Val) != 64 {
		t.Errorf("expected 64, got: %d", len(i.Val))
	}
	if scb.ItemValLength(x, i) != 1234 {
		t.Errorf("expected 1234, got: %d", scb.ItemValLength(x, i))
	}
	s.ItemDecRef(x, i)
}

func TestSlabStoreRandom(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, err := os.Create(fname)
	if err != nil {
		t.Errorf("expected to create file: " + fname)
	}
	defer os.Remove(fname)

	arena, scb := setupStoreArena(t, 256)

	start := func(f *os.File) (*gkvlite.Store, *gkvlite.Collection) {
		s, err := gkvlite.NewStoreEx(f, scb)
		if err != nil || s == nil {
			t.Errorf("expected NewStoreEx to work")
		}
		s.SetCollection("x", bytes.Compare)
		x := s.GetCollection("x")
		if x == nil {
			t.Errorf("expected SetColl/GetColl to work")
		}
		return s, x
	}

	s, x := start(f)

	stop := func() {
		s.Flush()
		s.Close()
		f.Close()
		f = nil
	}

	numSets := 0
	numKeys := 10
	for i := 0; i < 100; i++ {
		for j := 0; j < 1000; j++ {
			kr := rand.Int() % numKeys
			ks := fmt.Sprintf("%03d", kr)
			k := []byte(ks)
			r := rand.Int() % 100
			if r < 20 {
				i, err := x.GetItem(k, true)
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
				if i != nil {
					kr4 := kr * kr * kr * kr
					if scb.ItemValLength(x, i) != kr4 {
						t.Errorf("expected len: %d, got %d",
							kr4, scb.ItemValLength(x, i))
					}
					s.ItemDecRef(x, i)
				}
			} else if r < 60 {
				numSets++
				b := arena.Alloc(kr * kr * kr * kr)
				pri := rand.Int31()
				it := scb.ItemAlloc(x, uint16(len(k)))
				copy(it.Key, k)
				it.Val = b
				it.Priority = pri
				err := x.SetItem(it)
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
				scb.ItemDecRef(x, it)
			} else if r < 80 {
				_, err := x.Delete(k)
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			} else if r < 90 {
				x.EvictSomeItems()
			} else {
				// Close and reopen the store.
				stop()
				f, _ = os.OpenFile(fname, os.O_RDWR, 0666)
				s, x = start(f)
			}
		}
		x.EvictSomeItems()
		for k := 0; k < numKeys; k++ {
			_, err := x.Delete([]byte(fmt.Sprintf("%03d", k)))
			if err != nil {
				t.Fatalf("expected nil error, got: %v", err)
			}
		}
	}
}
