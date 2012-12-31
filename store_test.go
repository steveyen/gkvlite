package gkvlite

import (
	"bytes"
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
			i, err := x.Get([]byte(test.val), true)
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
			err := x.Upsert(&Item{
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
}

func loadCollection(x *Collection, arr []string) {
	for i, s := range arr {
		x.Upsert(&Item{
			Key:      []byte(s),
			Val:      []byte(s),
			Priority: int32(i),
		})
	}
}

func visitExpectCollection(t *testing.T, x *Collection, start string, arr []string) {
	n := 0
	err := x.VisitAscend([]byte(start), true, func(i *Item) bool {
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
	min, err := x.Min(true)
	if err != nil || min != nil {
		t.Errorf("expected no min, got: %v, err: %v", min, err)
	}
	max, err := x.Max(true)
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

	min0, err := x.Min(false)
	if err != nil || string(min0.Key) != "a" {
		t.Errorf("expected min of a")
	}
	min1, err := x.Min(true)
	if err != nil || string(min1.Key) != "a" || string(min1.Val) != "a" {
		t.Errorf("expected min of a")
	}
	max0, err := x.Max(false)
	if err != nil || string(max0.Key) != "e" {
		t.Errorf("expected max0 of e, got: %#v, err: %#v", max0, err)
	}
	max1, err := x.Max(true)
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
		t.Error("could not create file: %v", fname)
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
	i, err := x2.Get([]byte("a"), true)
	if err != nil {
		t.Errorf("expected s2.Get(a) to not error, err: %v", err)
	}
	if i == nil {
		t.Errorf("expected s2.Get(a) to return non-nil item")
	}
	if string(i.Key) != "a" {
		t.Errorf("expected s2.Get(a) to return key a, got: %v", i.Key)
	}
	if string(i.Val) != "a" {
		t.Errorf("expected s2.Get(a) to return val a, got: %v", i.Val)
	}
	i2, err := x2.Get([]byte("not-there"), true)
	if i2 != nil || err != nil {
		t.Errorf("expected miss to miss nicely.")
	}

	// ------------------------------------------------

	loadCollection(x, []string{"c", "b"})

	// x2 has its own snapshot, so should not see the new items.
	i, err = x2.Get([]byte("b"), true)
	if i != nil || err != nil {
		t.Errorf("expected b miss to miss nicely.")
	}
	i, err = x2.Get([]byte("c"), true)
	if i != nil || err != nil {
		t.Errorf("expected c miss to miss nicely.")
	}

	// Even after Flush().
	if err := s.Flush(); err != nil {
		t.Errorf("expected Flush() to have no error, err: %v", err)
	}
	f.Sync()

	i, err = x2.Get([]byte("b"), true)
	if i != nil || err != nil {
		t.Errorf("expected b miss to still miss nicely.")
	}
	i, err = x2.Get([]byte("c"), true)
	if i != nil || err != nil {
		t.Errorf("expected c miss to still miss nicely.")
	}

	i, err = x.Get([]byte("a"), true)
	if i == nil || err != nil {
		t.Errorf("expected a to be in x.")
	}
	i, err = x.Get([]byte("b"), true)
	if i == nil || err != nil {
		t.Errorf("expected b to be in x.")
	}
	i, err = x.Get([]byte("c"), true)
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

	i, err = x3.Get([]byte("a"), true)
	if i == nil || err != nil {
		t.Errorf("expected a to be in x3.")
	}
	i, err = x3.Get([]byte("b"), true)
	if i == nil || err != nil {
		t.Errorf("expected b to be in x3.")
	}
	i, err = x3.Get([]byte("c"), true)
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

	i, err = x4.Get([]byte("a"), true)
	if i == nil || err != nil {
		t.Errorf("expected a to be in x4.")
	}
	i, err = x4.Get([]byte("b"), true)
	if i != nil || err != nil {
		t.Errorf("expected b to not be in x4.")
	}
	i, err = x4.Get([]byte("c"), true)
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

	// Exercise Min and Max.
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
		i, err = mmTest.coll.Min(true)
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

		i, err = mmTest.coll.Max(true)
		if err != nil {
			t.Errorf("mmTestIdx: %v, expected no Max error, but got err: %v",
				mmTestIdx, err)
		}
		if i == nil {
			t.Errorf("mmTestIdx: %v, expected Max item, but got nil",
				mmTestIdx)
		}
		if string(i.Key) != mmTest.max {
			t.Errorf("mmTestIdx: %v, expected Max item key: %v, but got: %v",
				mmTestIdx, mmTest.max, string(i.Key))
		}
		if string(i.Val) != mmTest.max {
			t.Errorf("mmTestIdx: %v, expected Max item key: %v, but got: %v",
				mmTestIdx, mmTest.max, string(i.Val))
		}
	}

	// ------------------------------------------------

	// Try some withValue false.
	f5a, err := os.Open(fname) // Another file reader.
	s5a, err := NewStore(f5a)
	x5a := s5a.GetCollection("x")

	i, err = x5a.Get([]byte("a"), false)
	if i == nil {
		t.Error("was expecting a item")
	}
	if string(i.Key) != "a" {
		t.Error("was expecting a item has Key a")
	}
	if i.Val != nil {
		t.Error("was expecting a item with nil Val when withValue false")
	}

	i, err = x5a.Get([]byte("a"), true)
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
}
