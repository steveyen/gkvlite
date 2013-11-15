package main

// Test integration of gkvlite with go-slab, using gkvlite's optional
// ItemAddRef/ItemDecRef() callbacks to integrate with a slab memory
// allocator.

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sort"
	"testing"
	"unsafe"

	"github.com/steveyen/gkvlite"
	"github.com/steveyen/go-slab"
)

var maxOps = flag.Int("ops", 0,
	"max number of ops; 0 means run forever")
var maxItems = flag.Int("n", 10000,
	"max number of items")
var maxItemBytes = flag.Int("maxItemBytes", 20000,
	"max bytes for an item")
var pctSets = flag.Int("sets", 50,
	"percentage of sets; 50 means 50% sets")
var pctDeletes = flag.Int("deletes", 5,
	"percentage of deletes; 5 means 5% deletes")
var pctEvicts = flag.Int("evicts", 3,
	"percentage of evicts; 3 means 3% evicts")
var pctReopens = flag.Int("reopens", 0,
	"percentage of reopens; 0 means 0% reopens")
var useSlab = flag.Bool("useSlab", true,
	"whether to use slab allocator")
var flushEvery = flag.Int("flushEvery", 0,
	"flush every N ops; 0 means no flushing")
var slabStats = flag.Bool("slabStats", false,
	"whether to emit slab stats")

func usage() {
	fmt.Fprintf(os.Stderr, "gkvlite slab testing tool\n")
	fmt.Fprintf(os.Stderr, "\nusage: %s [flags] dbFileName\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  dbFileName - file name used during test\n")
	fmt.Fprintf(os.Stderr, "\nexamples:\n")
	fmt.Fprintf(os.Stderr, "  ./slab -n 1000 -sets 30 test.tmp\n")
	fmt.Fprintf(os.Stderr, "\nflags:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "  -h: print this usage/help message\n")
}

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		log.Fatalf("error: missing dbFileName")
	}
	if *pctSets + *pctDeletes + *pctEvicts + *pctReopens > 100 {
		log.Fatalf("error: percentages are > 100%% (%d%%+%d%%+%d%%+%d%%)",
			*pctSets, *pctDeletes, *pctEvicts + *pctReopens)
	}
	run(args[0], *useSlab, *flushEvery, *maxItemBytes,
		*maxOps, *maxItems, *pctSets, *pctDeletes, *pctEvicts, *pctReopens)
}

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

type ItemNode struct {
	refs int
	next *ItemNode
	item gkvlite.Item
}

var freeItemNodes *ItemNode

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
			if t != nil {
				t.Fatalf("itemValLength on nil i.Val, i: %#v", i)
			} else {
				panic(fmt.Sprintf("itemValLength on nil i.Val, i: %#v", i))
			}
		}
		s := 0
		b := i.Val
		for b != nil {
			s = s + len(b)
			n := arena.GetNext(b)
			if s > len(b) {
				arena.DecRef(b)
			}
			b = n
		}
		return s
	}
	itemValWrite := func(c *gkvlite.Collection,
		i *gkvlite.Item, w io.WriterAt, offset int64) error {
		s := 0
		b := i.Val
		for b != nil {
			_, err := w.WriteAt(b, offset + int64(s))
			if err != nil {
				return err
			}
			s = s + len(b)
			n := arena.GetNext(b)
			if s > len(b) {
				arena.DecRef(b)
			}
			b = n
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
	itemAlloc := func(c *gkvlite.Collection, keyLength uint16) *gkvlite.Item {
		var n *ItemNode
		if freeItemNodes != nil {
			n = freeItemNodes
			freeItemNodes = freeItemNodes.next
			n.next = nil
		} else {
			n = &ItemNode{}
			n.item.Transient = unsafe.Pointer(n)
		}
		if n.refs != 0 ||
			n.item.Key != nil || n.item.Val != nil || n.item.Priority != 0 ||
			n.item.Transient != unsafe.Pointer(n) {
			panic("unexpected ItemNode refs or item fields")
		}
		n.refs = 1
		n.next = nil
		n.item.Key = arena.Alloc(int(keyLength))
		return &n.item
	}
	itemAddRef := func(c *gkvlite.Collection, i *gkvlite.Item) {
		n := (*ItemNode)(i.Transient)
		n.refs++
	}
	itemDecRef := func(c *gkvlite.Collection, i *gkvlite.Item) {
		n := (*ItemNode)(i.Transient)
		n.refs--
		if n.refs == 0 {
			if i.Key == nil {
				panic("expected a Key")
			}
			arena.DecRef(i.Key)
			i.Key = nil
			if i.Val != nil {
				arena.DecRef(i.Val)
				i.Val = nil
			}
			i.Priority = 0
			n.next = freeItemNodes
			freeItemNodes = n
		}
	}

	scb := gkvlite.StoreCallbacks{
		ItemValLength: itemValLength,
		ItemValWrite:  itemValWrite,
		ItemValRead:   itemValRead,
		ItemAlloc:     itemAlloc,
		ItemAddRef:    itemAddRef,
		ItemDecRef:    itemDecRef,
	}
	return arena, scb
}

func run(fname string, useSlab bool, flushEvery int, maxItemBytes int,
	maxOps, maxItems, pctSets, pctDeletes, pctEvicts, pctReopens int) {
	fmt.Printf("fname: %s, useSlab: %v, flushEvery: %d" +
		", maxItemBytes: %d, maxOps: %d, maxItems: %d" +
		", pctSets: %d, pctDeletes; %d, pctEvicts: %d, pctReopens: %d\n",
		fname, useSlab, flushEvery, maxItemBytes,
		maxOps, maxItems, pctSets, pctDeletes, pctEvicts, pctReopens)

	os.Remove(fname)
	f, err := os.Create(fname)
	if err != nil {
		panic(fmt.Sprintf("error: could not create file: %s, err: %v", fname, err))
	}

	arenaStats := map[string]int64{}
	arena, scb := setupStoreArena(nil, 256)
	if !useSlab {
		arena = nil
		scb = gkvlite.StoreCallbacks{
			ItemValLength: func(c *gkvlite.Collection, i *gkvlite.Item) int {
				return len(i.Val)
			},
		}
	}

	start := func(f *os.File) (*gkvlite.Store, *gkvlite.Collection) {
		s, err := gkvlite.NewStoreEx(f, scb)
		if err != nil || s == nil {
			panic(fmt.Sprintf("error: expected NewStoreEx to work, err: %v", err))
		}
		x := s.SetCollection("x", bytes.Compare)
		if x == nil {
			panic(fmt.Sprintf("error: expected SetColl/GetColl to work"))
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

	numGets := 0
	numSets := 0
	numDeletes := 0
	numEvicts := 0
	numReopens := 0
	numFlushes := 0

	psd := pctSets + pctDeletes
	psde := pctSets + pctDeletes + pctEvicts
	psder := pctSets + pctDeletes + pctEvicts + pctReopens

	for i := 0; maxOps <= 0 || i < maxOps; i++ {
		kr := rand.Int() % maxItems
		kr4 := kr * kr * kr * kr
		if kr4 > maxItemBytes {
			kr4 = maxItemBytes
		}
		ks := fmt.Sprintf("%03d", kr)
		k := []byte(ks)
		r := rand.Int() % 100
		if r < pctSets {
			numSets++
			var b []byte
			if arena != nil {
				b = arena.Alloc(kr4)
			} else {
				b = make([]byte, kr4)
			}
			pri := rand.Int31()
			var it *gkvlite.Item
			if scb.ItemAlloc != nil {
				it = scb.ItemAlloc(x, uint16(len(k)))
			} else {
				it = &gkvlite.Item{Key: make([]byte, len(k))}
			}
			copy(it.Key, k)
			it.Val = b
			it.Priority = pri
			err := x.SetItem(it)
			if err != nil {
				panic(fmt.Sprintf("error: expected nil error, got: %v", err))
			}
			if scb.ItemDecRef != nil {
				scb.ItemDecRef(x, it)
			}
		} else if r < psd {
			numDeletes++
			_, err := x.Delete(k)
			if err != nil {
				panic(fmt.Sprintf("error: expected nil error, got: %v", err))
			}
		} else if r < psde {
			numEvicts++
			x.EvictSomeItems()
		} else if r < psder {
			numReopens++
			stop()
			f, _ = os.OpenFile(fname, os.O_RDWR, 0666)
			s, x = start(f)
		} else {
			numGets++
			i, err := x.GetItem(k, true)
			if err != nil {
				panic(fmt.Sprintf("error: expected nil error, got: %v", err))
			}
			if i != nil {
				if scb.ItemValLength(x, i) != kr4 {
					panic(fmt.Sprintf("error: expected len: %d, got %d",
						kr4, scb.ItemValLength(x, i)))
				}
				if scb.ItemDecRef != nil {
					scb.ItemDecRef(x, i)
				}
			}
		}

		if flushEvery > 0 && i % flushEvery == 0 {
			numFlushes++
			s.Flush()
		}

		if i % 10000 == 0 {
			if arena != nil && *slabStats == true {
				arena.Stats(arenaStats)
				mk := []string{}
				for k, _ := range arenaStats {
					mk = append(mk, k)
				}
				sort.Strings(mk)
				for _, k := range mk {
					log.Printf("%s = %d", k, arenaStats[k])
				}
			}
			log.Printf("i: %d, numGets: %d, numSets: %d, numDeletes: %d" +
				", numEvicts: %d, numReopens: %d, numFlushes: %d\n",
				i, numGets, numSets, numDeletes, numEvicts, numReopens, numFlushes)
		}
	}
}
