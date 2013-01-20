package gkvlite

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestStoreMem(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	s.SetCollection("x", bytes.Compare)
	x := s.GetCollection("x")
	if x == nil {
		t.Errorf("expected SetColl/GetColl to work")
	}
	x2 := s.GetCollection("x")
	if x2 != x {
		t.Errorf("expected 2nd GetColl to work")
	}

	tests := []struct {
		op  string
		val string
		pri int
		exp string
	}{
		{"get", "not-there", -1, "NIL"},
		{"ups", "a", 100, ""},
		{"get", "a", -1, "a"},
		{"ups", "b", 200, ""},
		{"get", "a", -1, "a"},
		{"get", "b", -1, "b"},
		{"ups", "c", 300, ""},
		{"get", "a", -1, "a"},
		{"get", "b", -1, "b"},
		{"get", "c", -1, "c"},
		{"get", "not-there", -1, "NIL"},
		{"ups", "a", 400, ""},
		{"get", "a", -1, "a"},
		{"get", "b", -1, "b"},
		{"get", "c", -1, "c"},
		{"get", "not-there", -1, "NIL"},
		{"del", "a", -1, ""},
		{"get", "a", -1, "NIL"},
		{"get", "b", -1, "b"},
		{"get", "c", -1, "c"},
		{"get", "not-there", -1, "NIL"},
		{"ups", "a", 10, ""},
		{"get", "a", -1, "a"},
		{"get", "b", -1, "b"},
		{"get", "c", -1, "c"},
		{"get", "not-there", -1, "NIL"},
		{"del", "a", -1, ""},
		{"del", "b", -1, ""},
		{"del", "c", -1, ""},
		{"get", "a", -1, "NIL"},
		{"get", "b", -1, "NIL"},
		{"get", "c", -1, "NIL"},
		{"get", "not-there", -1, "NIL"},
		{"del", "a", -1, ""},
		{"del", "b", -1, ""},
		{"del", "c", -1, ""},
		{"get", "a", -1, "NIL"},
		{"get", "b", -1, "NIL"},
		{"get", "c", -1, "NIL"},
		{"get", "not-there", -1, "NIL"},
		{"ups", "a", 10, ""},
		{"get", "a", -1, "a"},
		{"get", "b", -1, "NIL"},
		{"get", "c", -1, "NIL"},
		{"get", "not-there", -1, "NIL"},
	}

	for testIdx, test := range tests {
		switch test.op {
		case "get":
			i, err := x.GetItem([]byte(test.val), true)
			if err != nil {
				t.Errorf("test: %v, expected get nil error, got: %v",
					testIdx, err)
			}
			if i == nil && test.exp == "NIL" {
				continue
			}
			if string(i.Key) != test.exp {
				t.Errorf("test: %v, on Get, expected key: %v, got: %v",
					testIdx, test.exp, i.Key)
			}
			if string(i.Val) != test.exp {
				t.Errorf("test: %v, on Get, expected val: %v, got: %v",
					testIdx, test.exp, i.Key)
			}
		case "ups":
			err := x.SetItem(&Item{
				Key:      []byte(test.val),
				Val:      []byte(test.val),
				Priority: int32(test.pri),
			})
			if err != nil {
				t.Errorf("test: %v, expected ups nil error, got: %v",
					testIdx, err)
			}
		case "del":
			err := x.Delete([]byte(test.val))
			if err != nil {
				t.Errorf("test: %v, expected del nil error, got: %v",
					testIdx, err)
			}
		}
	}

	xx := s.SetCollection("xx", nil)

	for testIdx, test := range tests {
		switch test.op {
		case "get":
			i, err := xx.Get([]byte(test.val))
			if err != nil {
				t.Errorf("test: %v, expected get nil error, got: %v",
					testIdx, err)
			}
			if i == nil && test.exp == "NIL" {
				continue
			}
			if string(i) != test.exp {
				t.Errorf("test: %v, on Get, expected val: %v, got: %v",
					testIdx, test.exp, test.val)
			}
		case "ups":
			err := xx.Set([]byte(test.val), []byte(test.val))
			if err != nil {
				t.Errorf("test: %v, expected ups nil error, got: %v",
					testIdx, err)
			}
		case "del":
			err := xx.Delete([]byte(test.val))
			if err != nil {
				t.Errorf("test: %v, expected del nil error, got: %v",
					testIdx, err)
			}
		}
	}

	if xx.SetItem(&Item{}) == nil {
		t.Error("expected error on empty item")
	}
	if xx.SetItem(&Item{Key: []byte("hello")}) == nil {
		t.Error("expected error on nil item Val")
	}
	if xx.SetItem(&Item{Val: []byte("hello")}) == nil {
		t.Error("expected error on nil item Key")
	}
	if xx.SetItem(&Item{Key: []byte{}, Val: []byte{}}) == nil {
		t.Error("expected error on zero-length item Key")
	}
	if xx.SetItem(&Item{Key: make([]byte, 0xffff+1), Val: []byte{}}) == nil {
		t.Error("expected error on too long item Key")
	}
	if xx.SetItem(&Item{Key: make([]byte, 0xffff-1), Val: []byte{}}) != nil {
		t.Error("expected success on just under key length")
	}
	if xx.SetItem(&Item{Key: make([]byte, 0xffff), Val: []byte{}}) != nil {
		t.Error("expected success on just perfect key length")
	}
}

func loadCollection(x *Collection, arr []string) {
	for i, s := range arr {
		x.SetItem(&Item{
			Key:      []byte(s),
			Val:      []byte(s),
			Priority: int32(i),
		})
	}
}

func visitExpectCollection(t *testing.T, x *Collection, start string, arr []string) {
	n := 0
	err := x.VisitItemsAscend([]byte(start), true, func(i *Item) bool {
		if string(i.Key) != arr[n] {
			t.Errorf("expected visit item: %v, saw: %v", arr[n], i)
		}
		n++
		return true
	})
	if err != nil {
		t.Errorf("expected no visit error, got: %v", err)
	}
	if n != len(arr) {
		t.Errorf("expected # visit callbacks: %v, saw: %v", len(arr), n)
	}
}

func TestVisitStoreMem(t *testing.T) {
	s, _ := NewStore(nil)
	if s.Flush() == nil {
		t.Errorf("expected in-memory store Flush() error")
	}
	if len(s.GetCollectionNames()) != 0 {
		t.Errorf("expected no coll names on empty store")
	}
	x := s.SetCollection("x", bytes.Compare)
	if s.Flush() == nil {
		t.Errorf("expected in-memory store Flush() error")
	}
	if len(s.GetCollectionNames()) != 1 || s.GetCollectionNames()[0] != "x" {
		t.Errorf("expected 1 coll name x")
	}
	if s.GetCollection("x") != x {
		t.Errorf("expected coll x to be the same")
	}
	if s.GetCollection("y") != nil {
		t.Errorf("expected coll y to be nil")
	}

	visitExpectCollection(t, x, "a", []string{})
	min, err := x.MinItem(true)
	if err != nil || min != nil {
		t.Errorf("expected no min, got: %v, err: %v", min, err)
	}
	max, err := x.MaxItem(true)
	if err != nil || max != nil {
		t.Errorf("expected no max, got: %v, err: %v", max, err)
	}

	loadCollection(x, []string{"e", "d", "a", "c", "b", "c", "a"})
	if s.Flush() == nil {
		t.Errorf("expected in-memory store Flush() error")
	}

	visitX := func() {
		visitExpectCollection(t, x, "a", []string{"a", "b", "c", "d", "e"})
		visitExpectCollection(t, x, "a1", []string{"b", "c", "d", "e"})
		visitExpectCollection(t, x, "b", []string{"b", "c", "d", "e"})
		visitExpectCollection(t, x, "b1", []string{"c", "d", "e"})
		visitExpectCollection(t, x, "c", []string{"c", "d", "e"})
		visitExpectCollection(t, x, "c1", []string{"d", "e"})
		visitExpectCollection(t, x, "d", []string{"d", "e"})
		visitExpectCollection(t, x, "d1", []string{"e"})
		visitExpectCollection(t, x, "e", []string{"e"})
		visitExpectCollection(t, x, "f", []string{})
	}
	visitX()

	min0, err := x.MinItem(false)
	if err != nil || string(min0.Key) != "a" {
		t.Errorf("expected min of a")
	}
	min1, err := x.MinItem(true)
	if err != nil || string(min1.Key) != "a" || string(min1.Val) != "a" {
		t.Errorf("expected min of a")
	}
	max0, err := x.MaxItem(false)
	if err != nil || string(max0.Key) != "e" {
		t.Errorf("expected max0 of e, got: %#v, err: %#v", max0, err)
	}
	max1, err := x.MaxItem(true)
	if err != nil || string(max1.Key) != "e" || string(max1.Val) != "e" {
		t.Errorf("expected max1 of e, got: %#v, err: %#v", max1, err)
	}

	if s.Flush() == nil {
		t.Errorf("expected in-memory store Flush() error")
	}
}

func TestStoreFile(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, err := os.Create(fname)
	if err != nil || f == nil {
		t.Errorf("could not create file: %v", fname)
	}

	s, err := NewStore(f)
	if err != nil || s == nil {
		t.Errorf("expected NewStore(f) to work, err: %v", err)
	}
	if len(s.GetCollectionNames()) != 0 {
		t.Errorf("expected no coll names on empty store")
	}
	x := s.SetCollection("x", bytes.Compare)
	if x == nil {
		t.Errorf("expected SetCollection() to work")
	}

	if err := s.Flush(); err != nil {
		t.Errorf("expected empty Flush() to have no error, err: %v", err)
	}
	finfo, err := f.Stat()
	if err != nil {
		t.Errorf("expected stat to work")
	}
	sizeAfter1stFlush := finfo.Size()
	if sizeAfter1stFlush == 0 {
		t.Errorf("expected non-empty file after 1st Flush(), got: %v",
			sizeAfter1stFlush)
	}

	if len(s.GetCollectionNames()) != 1 || s.GetCollectionNames()[0] != "x" {
		t.Errorf("expected 1 coll name x")
	}
	if s.GetCollection("x") != x {
		t.Errorf("expected coll x to be the same")
	}
	if s.GetCollection("y") != nil {
		t.Errorf("expected coll y to be nil")
	}

	loadCollection(x, []string{"a"})
	if err := s.Flush(); err != nil {
		t.Errorf("expected single key Flush() to have no error, err: %v", err)
	}
	f.Sync()

	i, err := x.GetItem([]byte("a"), true)
	if err != nil {
		t.Errorf("expected s.GetItem(a) to not error, err: %v", err)
	}
	if i == nil {
		t.Errorf("expected s.GetItem(a) to return non-nil item")
	}
	if string(i.Key) != "a" {
		t.Errorf("expected s.GetItem(a) to return key a, got: %v", i.Key)
	}
	if string(i.Val) != "a" {
		t.Errorf("expected s.GetItem(a) to return val a, got: %v", i.Val)
	}

	// ------------------------------------------------

	f2, err := os.Open(fname) // Test reading the file.
	if err != nil || f2 == nil {
		t.Errorf("could not reopen file: %v", fname)
	}
	s2, err := NewStore(f2)
	if err != nil || s2 == nil {
		t.Errorf("expected NewStore(f) to work, err: %v", err)
	}
	if len(s2.GetCollectionNames()) != 1 || s2.GetCollectionNames()[0] != "x" {
		t.Errorf("expected 1 coll name x")
	}
	x2 := s2.GetCollection("x")
	if x2 == nil {
		t.Errorf("expected x2 to be there")
	}
	i, err = x2.GetItem([]byte("a"), true)
	if err != nil {
		t.Errorf("expected s2.GetItem(a) to not error, err: %v", err)
	}
	if i == nil {
		t.Errorf("expected s2.GetItem(a) to return non-nil item")
	}
	if string(i.Key) != "a" {
		t.Errorf("expected s2.GetItem(a) to return key a, got: %v", i.Key)
	}
	if string(i.Val) != "a" {
		t.Errorf("expected s2.GetItem(a) to return val a, got: %+v", i)
	}
	i2, err := x2.GetItem([]byte("not-there"), true)
	if i2 != nil || err != nil {
		t.Errorf("expected miss to miss nicely.")
	}

	// ------------------------------------------------

	loadCollection(x, []string{"c", "b"})

	// x2 has its own snapshot, so should not see the new items.
	i, err = x2.GetItem([]byte("b"), true)
	if i != nil || err != nil {
		t.Errorf("expected b miss to miss nicely.")
	}
	i, err = x2.GetItem([]byte("c"), true)
	if i != nil || err != nil {
		t.Errorf("expected c miss to miss nicely.")
	}

	// Even after Flush().
	if err := s.Flush(); err != nil {
		t.Errorf("expected Flush() to have no error, err: %v", err)
	}
	f.Sync()

	i, err = x2.GetItem([]byte("b"), true)
	if i != nil || err != nil {
		t.Errorf("expected b miss to still miss nicely.")
	}
	i, err = x2.GetItem([]byte("c"), true)
	if i != nil || err != nil {
		t.Errorf("expected c miss to still miss nicely.")
	}

	i, err = x.GetItem([]byte("a"), true)
	if i == nil || err != nil {
		t.Errorf("expected a to be in x.")
	}
	i, err = x.GetItem([]byte("b"), true)
	if i == nil || err != nil {
		t.Errorf("expected b to be in x.")
	}
	i, err = x.GetItem([]byte("c"), true)
	if i == nil || err != nil {
		t.Errorf("expected c to be in x.")
	}

	visitExpectCollection(t, x, "a", []string{"a", "b", "c"})
	visitExpectCollection(t, x2, "a", []string{"a"})

	// ------------------------------------------------

	f3, err := os.Open(fname) // Another file reader.
	if err != nil || f3 == nil {
		t.Errorf("could not reopen file: %v", fname)
	}
	s3, err := NewStore(f3)
	if err != nil || s3 == nil {
		t.Errorf("expected NewStore(f) to work, err: %v", err)
	}
	if len(s3.GetCollectionNames()) != 1 || s3.GetCollectionNames()[0] != "x" {
		t.Errorf("expected 1 coll name x")
	}
	x3 := s3.GetCollection("x")
	if x3 == nil {
		t.Errorf("expected x2 to be there")
	}

	visitExpectCollection(t, x, "a", []string{"a", "b", "c"})
	visitExpectCollection(t, x2, "a", []string{"a"})
	visitExpectCollection(t, x3, "a", []string{"a", "b", "c"})

	i, err = x3.GetItem([]byte("a"), true)
	if i == nil || err != nil {
		t.Errorf("expected a to be in x3.")
	}
	i, err = x3.GetItem([]byte("b"), true)
	if i == nil || err != nil {
		t.Errorf("expected b to be in x3.")
	}
	i, err = x3.GetItem([]byte("c"), true)
	if i == nil || err != nil {
		t.Errorf("expected c to be in x3.")
	}

	// ------------------------------------------------

	// Exercising deletion.
	if err = x.Delete([]byte("b")); err != nil {
		t.Errorf("expected Delete to have no error, err: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Errorf("expected Flush() to have no error, err: %v", err)
	}
	f.Sync()

	f4, err := os.Open(fname) // Another file reader.
	s4, err := NewStore(f4)
	x4 := s4.GetCollection("x")

	visitExpectCollection(t, x, "a", []string{"a", "c"})
	visitExpectCollection(t, x2, "a", []string{"a"})
	visitExpectCollection(t, x3, "a", []string{"a", "b", "c"})
	visitExpectCollection(t, x4, "a", []string{"a", "c"})

	i, err = x4.GetItem([]byte("a"), true)
	if i == nil || err != nil {
		t.Errorf("expected a to be in x4.")
	}
	i, err = x4.GetItem([]byte("b"), true)
	if i != nil || err != nil {
		t.Errorf("expected b to not be in x4.")
	}
	i, err = x4.GetItem([]byte("c"), true)
	if i == nil || err != nil {
		t.Errorf("expected c to be in x4.")
	}

	// ------------------------------------------------

	// Exercising deletion more.
	if err = x.Delete([]byte("c")); err != nil {
		t.Errorf("expected Delete to have no error, err: %v", err)
	}
	loadCollection(x, []string{"d", "c", "b", "b", "c"})
	if err = x.Delete([]byte("b")); err != nil {
		t.Errorf("expected Delete to have no error, err: %v", err)
	}
	if err = x.Delete([]byte("c")); err != nil {
		t.Errorf("expected Delete to have no error, err: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Errorf("expected Flush() to have no error, err: %v", err)
	}
	f.Sync()

	f5, err := os.Open(fname) // Another file reader.
	s5, err := NewStore(f5)
	x5 := s5.GetCollection("x")

	visitExpectCollection(t, x, "a", []string{"a", "d"})
	visitExpectCollection(t, x2, "a", []string{"a"})
	visitExpectCollection(t, x3, "a", []string{"a", "b", "c"})
	visitExpectCollection(t, x4, "a", []string{"a", "c"})
	visitExpectCollection(t, x5, "a", []string{"a", "d"})

	// ------------------------------------------------

	// Exercise MinItem and MaxItem.
	mmTests := []struct {
		coll *Collection
		min  string
		max  string
	}{
		{x, "a", "d"},
		{x2, "a", "a"},
		{x3, "a", "c"},
		{x4, "a", "c"},
		{x5, "a", "d"},
	}

	for mmTestIdx, mmTest := range mmTests {
		i, err = mmTest.coll.MinItem(true)
		if err != nil {
			t.Errorf("mmTestIdx: %v, expected no Min error, but got err: %v",
				mmTestIdx, err)
		}
		if i == nil {
			t.Errorf("mmTestIdx: %v, expected Min item, but got nil",
				mmTestIdx)
		}
		if string(i.Key) != mmTest.min {
			t.Errorf("mmTestIdx: %v, expected Min item key: %v, but got: %v",
				mmTestIdx, mmTest.min, string(i.Key))
		}
		if string(i.Val) != mmTest.min {
			t.Errorf("mmTestIdx: %v, expected Min item val: %v, but got: %v",
				mmTestIdx, mmTest.min, string(i.Val))
		}

		i, err = mmTest.coll.MaxItem(true)
		if err != nil {
			t.Errorf("mmTestIdx: %v, expected no MaxItem error, but got err: %v",
				mmTestIdx, err)
		}
		if i == nil {
			t.Errorf("mmTestIdx: %v, expected MaxItem item, but got nil",
				mmTestIdx)
		}
		if string(i.Key) != mmTest.max {
			t.Errorf("mmTestIdx: %v, expected MaxItem item key: %v, but got: %v",
				mmTestIdx, mmTest.max, string(i.Key))
		}
		if string(i.Val) != mmTest.max {
			t.Errorf("mmTestIdx: %v, expected MaxItem item key: %v, but got: %v",
				mmTestIdx, mmTest.max, string(i.Val))
		}
	}

	// ------------------------------------------------

	// Try some withValue false.
	f5a, err := os.Open(fname) // Another file reader.
	s5a, err := NewStore(f5a)
	x5a := s5a.GetCollection("x")

	i, err = x5a.GetItem([]byte("a"), false)
	if i == nil {
		t.Error("was expecting a item")
	}
	if string(i.Key) != "a" {
		t.Error("was expecting a item has Key a")
	}
	if i.Val != nil {
		t.Error("was expecting a item with nil Val when withValue false")
	}

	i, err = x5a.GetItem([]byte("a"), true)
	if i == nil {
		t.Error("was expecting a item")
	}
	if string(i.Key) != "a" {
		t.Error("was expecting a item has Key a")
	}
	if string(i.Val) != "a" {
		t.Error("was expecting a item has Val a")
	}

	// ------------------------------------------------

	// Exercising delete everything.
	for _, k := range []string{"a", "b", "c", "d"} {
		if err = x.Delete([]byte(k)); err != nil {
			t.Errorf("expected Delete to have no error, err: %v", err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Errorf("expected Flush() to have no error, err: %v", err)
	}
	f.Sync()

	f6, err := os.Open(fname) // Another file reader.
	s6, err := NewStore(f6)
	x6 := s6.GetCollection("x")

	visitExpectCollection(t, x, "a", []string{})
	visitExpectCollection(t, x2, "a", []string{"a"})
	visitExpectCollection(t, x3, "a", []string{"a", "b", "c"})
	visitExpectCollection(t, x4, "a", []string{"a", "c"})
	visitExpectCollection(t, x5, "a", []string{"a", "d"})
	visitExpectCollection(t, x6, "a", []string{})

	// ------------------------------------------------

	ssnap := s.Snapshot()
	if ssnap == nil {
		t.Errorf("expected snapshot to work")
	}
	xsnap := ssnap.GetCollection("x")
	if xsnap == nil {
		t.Errorf("expected snapshot to have x")
	}
	visitExpectCollection(t, xsnap, "a", []string{})
	if ssnap.Flush() == nil {
		t.Errorf("expected snapshot Flush() error")
	}

	s3snap := s3.Snapshot()
	if s3snap == nil {
		t.Errorf("expected snapshot to work")
	}
	x3snap := s3snap.GetCollection("x")
	if x3snap == nil {
		t.Errorf("expected snapshot to have x")
	}
	visitExpectCollection(t, x3snap, "a", []string{"a", "b", "c"})
	if s3snap.Flush() == nil {
		t.Errorf("expected snapshot Flush() error")
	}

	s3snap1 := s3.Snapshot()
	if s3snap1 == nil {
		t.Errorf("expected snapshot to work")
	}
	x3snap1 := s3snap1.GetCollection("x")
	if x3snap1 == nil {
		t.Errorf("expected snapshot to have x")
	}
	visitExpectCollection(t, x3snap1, "a", []string{"a", "b", "c"})
	if s3snap1.Flush() == nil {
		t.Errorf("expected snapshot Flush() error")
	}

	loadCollection(x3snap, []string{"e", "d", "a", "c", "b", "c", "a"})
	visitExpectCollection(t, x3snap1, "a", []string{"a", "b", "c"})
	visitExpectCollection(t, x3, "a", []string{"a", "b", "c"})

	// ------------------------------------------------

	// Exercise CopyTo.
	ccTests := []struct {
		src    *Store
		fevery int
		expect []string
	}{
		{s6, 1, []string{}},
		{s5, 0, []string{"a", "d"}},
		{s5, 2, []string{"a", "d"}},
		{s5, 4, []string{"a", "d"}},
		{s3snap, 4, []string{"a", "b", "c", "d", "e"}},
		{s3snap1, 1, []string{"a", "b", "c"}},
		{s3snap1, 0, []string{"a", "b", "c"}},
		{s3, 1000, []string{"a", "b", "c"}},
	}

	for ccTestIdx, ccTest := range ccTests {
		ccName := fmt.Sprintf("tmpCompactCopy-%v.test", ccTestIdx)
		os.Remove(ccName)
		ccFile, err := os.Create(ccName)
		if err != nil || f == nil {
			t.Errorf("%v: could not create file: %v", ccTestIdx, fname)
		}
		cc, err := ccTest.src.CopyTo(ccFile, ccTest.fevery)
		if err != nil {
			t.Errorf("%v: expected successful CopyTo, got: %v", ccTestIdx, err)
		}
		cx := cc.GetCollection("x")
		if cx == nil {
			t.Errorf("%v: expected successful CopyTo of x coll", ccTestIdx)
		}
		visitExpectCollection(t, cx, "a", ccTest.expect)
		if ccTest.fevery > 100 {
			// Expect file to be more compact when there's less flushing.
			finfoSrc, _ := ccTest.src.file.Stat()
			finfoCpy, _ := cc.file.Stat()
			if finfoSrc.Size() < finfoCpy.Size() {
				t.Error("%v: expected copy to be smaller / compacted"+
					"src size: %v, cpy size: %v", ccTestIdx,
					finfoSrc.Size(), finfoCpy.Size())
			}
		} else {
			err = cc.Flush()
			if err != nil {
				t.Errorf("%v: expect Flush() to work on snapshot, got: %v",
					ccTestIdx, err)
			}
		}
		ccFile.Close()

		// Reopen the snapshot and see that it's right.
		ccFile, err = os.Open(ccName)
		cc, err = NewStore(ccFile)
		if err != nil {
			t.Errorf("%v: expected successful NewStore against snapshot file, got: %v",
				ccTestIdx, err)
		}
		cx = cc.GetCollection("x")
		if cx == nil {
			t.Errorf("%v: expected successful CopyTo of x coll", ccTestIdx)
		}
		visitExpectCollection(t, cx, "a", ccTest.expect)
		if ccTest.fevery > 100 {
			// Expect file to be more compact when there's less flushing.
			finfoSrc, _ := ccTest.src.file.Stat()
			finfoCpy, _ := cc.file.Stat()
			if finfoSrc.Size() < finfoCpy.Size() {
				t.Errorf("%v: expected copy to be smaller / compacted"+
					"src size: %v, cpy size: %v", ccTestIdx,
					finfoSrc.Size(), finfoCpy.Size())
			}
		}
		ccFile.Close()
		os.Remove(ccName)
	}
}

func TestStoreMultipleCollections(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, err := os.Create(fname)
	if err != nil || f == nil {
		t.Errorf("could not create file: %v", fname)
	}

	s, err := NewStore(f)
	if err != nil {
		t.Errorf("expected NewStore to work")
	}
	if len(s.GetCollectionNames()) != 0 {
		t.Errorf("expected no coll names on empty store")
	}
	x := s.SetCollection("x", nil)
	y := s.SetCollection("y", nil)
	z := s.SetCollection("z", nil)

	loadCollection(x, []string{"e", "d", "a", "c", "b", "c", "a"})
	loadCollection(y, []string{"1", "2", "3", "4", "5"})
	loadCollection(z, []string{"D", "C", "B", "A"})

	visitExpectCollection(t, x, "a", []string{"a", "b", "c", "d", "e"})
	visitExpectCollection(t, y, "1", []string{"1", "2", "3", "4", "5"})
	visitExpectCollection(t, z, "A", []string{"A", "B", "C", "D"})

	err = s.Flush()
	if err != nil {
		t.Errorf("expected flush to work")
	}
	f.Close()

	f1, err := os.OpenFile(fname, os.O_RDWR, 0666)
	s1, err := NewStore(f1)
	if err != nil {
		t.Errorf("expected NewStore to work")
	}
	if len(s1.GetCollectionNames()) != 3 {
		t.Errorf("expected 3 colls")
	}

	x1 := s1.GetCollection("x")
	y1 := s1.GetCollection("y")
	z1 := s1.GetCollection("z")

	visitExpectCollection(t, x1, "a", []string{"a", "b", "c", "d", "e"})
	visitExpectCollection(t, y1, "1", []string{"1", "2", "3", "4", "5"})
	visitExpectCollection(t, z1, "A", []string{"A", "B", "C", "D"})

	s1.RemoveCollection("x")

	// Reset the y collection.
	s1.RemoveCollection("y")
	s1.SetCollection("y", nil)

	err = s1.Flush()
	if err != nil {
		t.Errorf("expected flush to work")
	}
	f1.Sync()
	f1.Close()

	f2, err := os.Open(fname)
	s2, err := NewStore(f2)
	if err != nil {
		t.Errorf("expected NewStore to work")
	}
	if len(s2.GetCollectionNames()) != 2 {
		t.Errorf("expected 2 colls, got %v", s2.GetCollectionNames())
	}

	x2 := s2.GetCollection("x")
	y2 := s2.GetCollection("y")
	z2 := s2.GetCollection("z")

	if x2 != nil {
		t.Errorf("expected x coll to be gone")
	}

	visitExpectCollection(t, y2, "1", []string{})
	visitExpectCollection(t, z2, "A", []string{"A", "B", "C", "D"})

	if err = z2.Set([]byte("hi"), []byte("world")); err != nil {
		t.Errorf("expected set to work, got %v", err)
	}

	if err = s2.Flush(); err == nil {
		t.Errorf("expected Flush() to fail on a readonly file")
	}
}
