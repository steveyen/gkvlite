package slab

// Test integration of gkvlite with go-slab, using gkvlite's optional
// ItemValAddRef/DecRef() callbacks to integrate with a slab memory
// allocator.

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/steveyen/gkvlite"
	"github.com/steveyen/go-slab"
)

// Helper function to read a contiguous byte sequence, splitting
// it up into chained buf's of maxBufSize.  The last buf in the
// chain can have length <= maxBufSize.
func readBufChain(arena *slab.Arena, maxBufSize int, r io.ReaderAt, offset int64,
	valLength uint32) ([]byte, error) {
	n := int(valLength)
	if n > maxBufSize {
		n = maxBufSize
	}
	b := arena.Alloc(n)
	_, err := r.ReadAt(b, offset)
	if err != nil {
		arena.DecRef(b)
		return nil, err
	}
	remaining := valLength - uint32(n)
	if remaining > 0 {
		next, err := readBufChain(arena, maxBufSize, r, offset + int64(n), remaining)
		if err != nil {
			arena.DecRef(b)
			return nil, err
		}
		arena.SetNext(b, next)
	}
	return b, nil
}

func setupStoreArena(t *testing.T, maxBufSize int) (
	*slab.Arena, gkvlite.StoreCallbacks) {
	arena := slab.NewArena(48, // The smallest slab class "chunk size" is 48 bytes.
		1024*1024, // Each slab will be 1MB in size.
		2,         // Power of 2 growth in "chunk sizes".
		nil)       // Use default make([]byte) for slab memory.
	if arena == nil {
		t.Errorf("expected arena")
	}

	itemValLength := func(c *gkvlite.Collection, i *gkvlite.Item) int {
		if i.Val == nil {
			t.Fatalf("itemValLength on nil i.Val, i: %#v", i)
		}
		n := 0
		for b := i.Val; b != nil; b = arena.GetNext(b) {
			n = n + len(b)
		}
		return n
	}
	itemValWrite := func(c *gkvlite.Collection,
		i *gkvlite.Item, w io.WriterAt, offset int64) error {
		n := 0
		for b := i.Val; b != nil; b = arena.GetNext(b) {
			_, err := w.WriteAt(b, offset + int64(n))
			if err != nil {
				return err
			}
			n = n + len(b)
		}
		return nil
	}
	itemValRead := func(c *gkvlite.Collection,
		i *gkvlite.Item, r io.ReaderAt, offset int64, valLength uint32) error {
		b, err := readBufChain(arena, maxBufSize, r, offset, valLength)
		if err != nil {
			return err
		}
		i.Val = b
		return nil
	}
	itemValAddRef := func(c *gkvlite.Collection, i *gkvlite.Item) {
		if i.Val == nil {
			return
		}
		arena.AddRef(i.Val)
	}
	itemValDecRef := func(c *gkvlite.Collection, i *gkvlite.Item) {
		if i.Val == nil {
			return
		}
		arena.DecRef(i.Val)
	}

	scb := gkvlite.StoreCallbacks{
		ItemValLength: itemValLength,
		ItemValWrite:  itemValWrite,
		ItemValRead:   itemValRead,
		ItemValAddRef: itemValAddRef,
		ItemValDecRef: itemValDecRef,
	}
	return arena, scb
}

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
	x.SetItem(&gkvlite.Item{
		Key:      []byte("a"),
		Val:      b,
		Priority: 100,
	})
	x.SetItem(&gkvlite.Item{
		Key:      []byte("big"),
		Val:      arena.Alloc(1234),
		Priority: 100,
	})
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
	i, err := x.GetItem([]byte("a"), true)
	if err != nil || i == nil {
		t.Errorf("expected no GetItem() err, got: %v", err)
	}
	if string(i.Val) != "hello" {
		t.Errorf("expected hello, got: %#v", i)
	}
	s.ItemValDecRef(x, i)
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
	s.ItemValDecRef(x, i)
}
