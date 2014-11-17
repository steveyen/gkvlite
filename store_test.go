package gkvlite

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"unsafe"
)

type mockfile struct {
	f           *os.File
	readat      func(p []byte, off int64) (n int, err error)
	writeat     func(p []byte, off int64) (n int, err error)
	stat        func() (fi os.FileInfo, err error)
	numReadAt   int
	numWriteAt  int
	numStat     int
	numTruncate int
}

func (m *mockfile) ReadAt(p []byte, off int64) (n int, err error) {
	m.numReadAt++
	if m.readat != nil {
		return m.readat(p, off)
	}
	return m.f.ReadAt(p, off)
}

func (m *mockfile) WriteAt(p []byte, off int64) (n int, err error) {
	m.numWriteAt++
	if m.writeat != nil {
		return m.writeat(p, off)
	}
	return m.f.WriteAt(p, off)
}

func (m *mockfile) Stat() (fi os.FileInfo, err error) {
	m.numStat++
	if m.stat != nil {
		return m.stat()
	}
	return m.f.Stat()
}

func (m *mockfile) Truncate(size int64) error {
	m.numTruncate++
	return m.f.Truncate(size)
}

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
	if x.Name() != "x" {
		t.Errorf("expected name to be same")
	}
	x2 := s.GetCollection("x")
	if x2 != x {
		t.Errorf("expected 2nd GetColl to work")
	}
	if x2.Name() != "x" {
		t.Errorf("expected name to be same")
	}
	numItems, numBytes, err := x2.GetTotals()
	if err != nil || numItems != 0 || numBytes != 0 {
		t.Errorf("expected empty memory coll to be empty")
	}

	tests := []struct {
		op  string
		val string
		pri int
		exp string
	}{
		{"get", "not-there", 0, "NIL"},
		{"ups", "a", 100, ""},
		{"get", "a", 0, "a"},
		{"ups", "b", 200, ""},
		{"get", "a", 0, "a"},
		{"get", "b", 0, "b"},
		{"ups", "c", 300, ""},
		{"get", "a", 0, "a"},
		{"get", "b", 0, "b"},
		{"get", "c", 0, "c"},
		{"get", "not-there", 0, "NIL"},
		{"ups", "a", 400, ""},
		{"get", "a", 0, "a"},
		{"get", "b", 0, "b"},
		{"get", "c", 0, "c"},
		{"get", "not-there", 0, "NIL"},
		{"del", "a", 0, ""},
		{"get", "a", 0, "NIL"},
		{"get", "b", 0, "b"},
		{"get", "c", 0, "c"},
		{"get", "not-there", 0, "NIL"},
		{"ups", "a", 10, ""},
		{"get", "a", 0, "a"},
		{"get", "b", 0, "b"},
		{"get", "c", 0, "c"},
		{"get", "not-there", 0, "NIL"},
		{"del", "a", 0, ""},
		{"del", "b", 0, ""},
		{"del", "c", 0, ""},
		{"get", "a", 0, "NIL"},
		{"get", "b", 0, "NIL"},
		{"get", "c", 0, "NIL"},
		{"get", "not-there", 0, "NIL"},
		{"del", "a", 0, ""},
		{"del", "b", 0, ""},
		{"del", "c", 0, ""},
		{"get", "a", 0, "NIL"},
		{"get", "b", 0, "NIL"},
		{"get", "c", 0, "NIL"},
		{"get", "not-there", 0, "NIL"},
		{"ups", "a", 10, ""},
		{"get", "a", 0, "a"},
		{"get", "b", 0, "NIL"},
		{"get", "c", 0, "NIL"},
		{"get", "not-there", 0, "NIL"},
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
			_, err := x.Delete([]byte(test.val))
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
			_, err := xx.Delete([]byte(test.val))
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
	err = xx.SetItem(&Item{
		Key:      make([]byte, 0xffff),
		Val:      []byte{},
		Priority: -1})
	if err == nil {
		t.Error("expected error on -1 Priority")
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

func visitExpectCollection(t *testing.T, x *Collection, start string,
	arr []string, cb func(i *Item)) {
	n := 0
	err := x.VisitItemsAscend([]byte(start), true, func(i *Item) bool {
		if cb != nil {
			cb(i)
		}
		if string(i.Key) != arr[n] {
			t.Fatalf("expected visit item: %v, saw key: %s, item: %#v",
				arr[n], string(i.Key), i)
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

func visitDescendExpectCollection(t *testing.T, x *Collection, tgt string,
	arr []string, cb func(i *Item)) {
	n := 0
	err := x.VisitItemsDescend([]byte(tgt), true, func(i *Item) bool {
		if cb != nil {
			cb(i)
		}
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

	visitExpectCollection(t, x, "a", []string{}, nil)
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
		visitExpectCollection(t, x, "a", []string{"a", "b", "c", "d", "e"}, nil)
		visitExpectCollection(t, x, "a1", []string{"b", "c", "d", "e"}, nil)
		visitExpectCollection(t, x, "b", []string{"b", "c", "d", "e"}, nil)
		visitExpectCollection(t, x, "b1", []string{"c", "d", "e"}, nil)
		visitExpectCollection(t, x, "c", []string{"c", "d", "e"}, nil)
		visitExpectCollection(t, x, "c1", []string{"d", "e"}, nil)
		visitExpectCollection(t, x, "d", []string{"d", "e"}, nil)
		visitExpectCollection(t, x, "d1", []string{"e"}, nil)
		visitExpectCollection(t, x, "e", []string{"e"}, nil)
		visitExpectCollection(t, x, "f", []string{}, nil)
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

	numItems, numBytes, err := x.GetTotals()
	if err != nil || numItems != 5 || numBytes != 10 {
		t.Errorf("mimatched memory coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
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

	numItems, numBytes, err := x.GetTotals()
	if err != nil || numItems != 0 || numBytes != 0 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
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

	numItems, numBytes, err = x.GetTotals()
	if err != nil || numItems != 1 || numBytes != 2 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
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
	numItems, numBytes, err = x2.GetTotals()
	if err != nil || numItems != 1 || numBytes != 2 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
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

	visitExpectCollection(t, x, "a", []string{"a", "b", "c"}, nil)
	visitExpectCollection(t, x2, "a", []string{"a"}, nil)

	numItems, numBytes, err = x.GetTotals()
	if err != nil || numItems != 3 || numBytes != 6 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
	}
	numItems, numBytes, err = x2.GetTotals()
	if err != nil || numItems != 1 || numBytes != 2 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
	}

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

	visitExpectCollection(t, x, "a", []string{"a", "b", "c"}, nil)
	visitExpectCollection(t, x2, "a", []string{"a"}, nil)
	visitExpectCollection(t, x3, "a", []string{"a", "b", "c"}, nil)

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
	if _, err = x.Delete([]byte("b")); err != nil {
		t.Errorf("expected Delete to have no error, err: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Errorf("expected Flush() to have no error, err: %v", err)
	}
	f.Sync()

	f4, err := os.Open(fname) // Another file reader.
	s4, err := NewStore(f4)
	x4 := s4.GetCollection("x")

	visitExpectCollection(t, x, "a", []string{"a", "c"}, nil)
	visitExpectCollection(t, x2, "a", []string{"a"}, nil)
	visitExpectCollection(t, x3, "a", []string{"a", "b", "c"}, nil)
	visitExpectCollection(t, x4, "a", []string{"a", "c"}, nil)

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

	numItems, numBytes, err = x.GetTotals()
	if err != nil || numItems != 2 || numBytes != 4 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
	}
	numItems, numBytes, err = x2.GetTotals()
	if err != nil || numItems != 1 || numBytes != 2 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
	}
	numItems, numBytes, err = x3.GetTotals()
	if err != nil || numItems != 3 || numBytes != 6 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
	}
	numItems, numBytes, err = x4.GetTotals()
	if err != nil || numItems != 2 || numBytes != 4 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
	}

	// ------------------------------------------------

	// Exercising deletion more.
	if _, err = x.Delete([]byte("c")); err != nil {
		t.Errorf("expected Delete to have no error, err: %v", err)
	}
	loadCollection(x, []string{"d", "c", "b", "b", "c"})
	if _, err = x.Delete([]byte("b")); err != nil {
		t.Errorf("expected Delete to have no error, err: %v", err)
	}
	if _, err = x.Delete([]byte("c")); err != nil {
		t.Errorf("expected Delete to have no error, err: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Errorf("expected Flush() to have no error, err: %v", err)
	}
	f.Sync()

	f5, err := os.Open(fname) // Another file reader.
	s5, err := NewStore(f5)
	x5 := s5.GetCollection("x")

	visitExpectCollection(t, x, "a", []string{"a", "d"}, nil)
	visitExpectCollection(t, x2, "a", []string{"a"}, nil)
	visitExpectCollection(t, x3, "a", []string{"a", "b", "c"}, nil)
	visitExpectCollection(t, x4, "a", []string{"a", "c"}, nil)
	visitExpectCollection(t, x5, "a", []string{"a", "d"}, nil)

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
		if _, err = x.Delete([]byte(k)); err != nil {
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

	visitExpectCollection(t, x, "a", []string{}, nil)
	visitExpectCollection(t, x2, "a", []string{"a"}, nil)
	visitExpectCollection(t, x3, "a", []string{"a", "b", "c"}, nil)
	visitExpectCollection(t, x4, "a", []string{"a", "c"}, nil)
	visitExpectCollection(t, x5, "a", []string{"a", "d"}, nil)
	visitExpectCollection(t, x6, "a", []string{}, nil)

	// ------------------------------------------------

	ssnap := s.Snapshot()
	if ssnap == nil {
		t.Errorf("expected snapshot to work")
	}
	xsnap := ssnap.GetCollection("x")
	if xsnap == nil {
		t.Errorf("expected snapshot to have x")
	}
	err = xsnap.SetItem(&Item{
		Key:      []byte("should-be-read-only"),
		Val:      []byte("aaa"),
		Priority: 100,
	})
	if err == nil {
		t.Errorf("expected snapshot SetItem() error")
	}
	wd, err := xsnap.Delete([]byte("should-be-read-only"))
	if err == nil || wd != false {
		t.Errorf("expected snapshot Delete() error")
	}
	visitExpectCollection(t, xsnap, "a", []string{}, nil)
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
	visitExpectCollection(t, x3snap, "a", []string{"a", "b", "c"}, nil)
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
	visitExpectCollection(t, x3snap1, "a", []string{"a", "b", "c"}, nil)
	if s3snap1.Flush() == nil {
		t.Errorf("expected snapshot Flush() error")
	}

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
		{s3snap, 2, []string{"a", "b", "c"}},
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
		visitExpectCollection(t, cx, "a", ccTest.expect, nil)
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
		visitExpectCollection(t, cx, "a", ccTest.expect, nil)
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

	visitExpectCollection(t, x, "a", []string{"a", "b", "c", "d", "e"}, nil)
	visitExpectCollection(t, y, "1", []string{"1", "2", "3", "4", "5"}, nil)
	visitExpectCollection(t, z, "A", []string{"A", "B", "C", "D"}, nil)

	numItems, numBytes, err := x.GetTotals()
	if err != nil || numItems != 5 || numBytes != 10 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
	}
	numItems, numBytes, err = y.GetTotals()
	if err != nil || numItems != 5 || numBytes != 10 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
	}
	numItems, numBytes, err = z.GetTotals()
	if err != nil || numItems != 4 || numBytes != 8 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
	}

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

	visitExpectCollection(t, x1, "a", []string{"a", "b", "c", "d", "e"}, nil)
	visitExpectCollection(t, y1, "1", []string{"1", "2", "3", "4", "5"}, nil)
	visitExpectCollection(t, z1, "A", []string{"A", "B", "C", "D"}, nil)

	numItems, numBytes, err = x1.GetTotals()
	if err != nil || numItems != 5 || numBytes != 10 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
	}
	numItems, numBytes, err = y1.GetTotals()
	if err != nil || numItems != 5 || numBytes != 10 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
	}
	numItems, numBytes, err = z1.GetTotals()
	if err != nil || numItems != 4 || numBytes != 8 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
	}

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

	visitExpectCollection(t, y2, "1", []string{}, nil)
	visitExpectCollection(t, z2, "A", []string{"A", "B", "C", "D"}, nil)

	if err = z2.Set([]byte("hi"), []byte("world")); err != nil {
		t.Errorf("expected set to work, got %v", err)
	}

	if err = s2.Flush(); err == nil {
		t.Errorf("expected Flush() to fail on a readonly file")
	}

	numItems, numBytes, err = y2.GetTotals()
	if err != nil || numItems != 0 || numBytes != 0 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
	}
	numItems, numBytes, err = z2.GetTotals()
	if err != nil || numItems != 5 || numBytes != 8+2+5 {
		t.Errorf("mimatched coll totals, got: %v, %v, %v",
			numItems, numBytes, err)
	}
}

func TestStoreConcurrentVisits(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, _ := os.Create(fname)
	s, _ := NewStore(f)
	x := s.SetCollection("x", nil)
	loadCollection(x, []string{"e", "d", "a", "c", "b", "c", "a"})
	visitExpectCollection(t, x, "a", []string{"a", "b", "c", "d", "e"}, nil)
	s.Flush()
	f.Close()

	f1, _ := os.OpenFile(fname, os.O_RDWR, 0666)
	s1, _ := NewStore(f1)
	x1 := s1.GetCollection("x")

	for i := 0; i < 100; i++ {
		go func() {
			visitExpectCollection(t, x1, "a", []string{"a", "b", "c", "d", "e"},
				func(i *Item) {
					runtime.Gosched() // Yield to test concurrency.
				})
		}()
	}
}

func TestStoreConcurrentDeleteDuringVisits(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, _ := os.Create(fname)
	s, _ := NewStore(f)
	x := s.SetCollection("x", nil)
	loadCollection(x, []string{"e", "d", "a", "c", "b", "c", "a"})
	visitExpectCollection(t, x, "a", []string{"a", "b", "c", "d", "e"}, nil)
	s.Flush()
	f.Close()

	f1, _ := os.OpenFile(fname, os.O_RDWR, 0666)
	s1, _ := NewStore(f1)
	x1 := s1.GetCollection("x")

	exp := []string{"a", "b", "c", "d", "e"}
	toDelete := int32(len(exp))

	// Concurrent mutations like a delete should not affect a visit()
	// that's already inflight.
	visitExpectCollection(t, x1, "a", exp, func(i *Item) {
		d := atomic.AddInt32(&toDelete, -1)
		toDeleteKey := exp[d]
		if _, err := x1.Delete([]byte(toDeleteKey)); err != nil {
			t.Errorf("expected concurrent delete to work on key: %v, got: %v",
				toDeleteKey, err)
		}
	})
}

func TestStoreConcurrentInsertDuringVisits(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, _ := os.Create(fname)
	s, _ := NewStore(f)
	x := s.SetCollection("x", nil)
	loadCollection(x, []string{"e", "d", "a", "c", "b", "c", "a"})
	visitExpectCollection(t, x, "a", []string{"a", "b", "c", "d", "e"}, nil)
	s.Flush()
	f.Close()

	f1, _ := os.OpenFile(fname, os.O_RDWR, 0666)
	s1, _ := NewStore(f1)
	x1 := s1.GetCollection("x")

	exp := []string{"a", "b", "c", "d", "e"}
	add := []string{"A", "1", "E", "2", "C"}
	toAdd := int32(0)

	// Concurrent mutations like inserts should not affect a visit()
	// that's already inflight.
	visitExpectCollection(t, x1, "a", exp, func(i *Item) {
		go func() {
			a := atomic.AddInt32(&toAdd, 1)
			toAddKey := []byte(add[a-1])
			if err := x1.Set(toAddKey, toAddKey); err != nil {
				t.Errorf("expected concurrent set to work on key: %v, got: %v",
					toAddKey, err)
			}
		}()
		runtime.Gosched() // Yield to test concurrency.
	})
}

func TestWasDeleted(t *testing.T) {
	s, _ := NewStore(nil)
	x := s.SetCollection("x", bytes.Compare)
	wasDeleted, err := x.Delete([]byte("notThere"))
	if err != nil {
		t.Errorf("expected no deletion error")
	}
	if wasDeleted {
		t.Errorf("expected wasDeleted false")
	}

	loadCollection(x, []string{"a", "b"})
	wasDeleted, err = x.Delete([]byte("notThere"))
	if err != nil {
		t.Errorf("expected no deletion error")
	}
	if wasDeleted {
		t.Errorf("expected wasDeleted false")
	}
	wasDeleted, err = x.Delete([]byte("a"))
	if err != nil {
		t.Errorf("expected no deletion error")
	}
	if !wasDeleted {
		t.Errorf("expected wasDeleted true")
	}
	wasDeleted, err = x.Delete([]byte("notThere"))
	if err != nil {
		t.Errorf("expected no deletion error")
	}
	if wasDeleted {
		t.Errorf("expected wasDeleted false")
	}
}

func BenchmarkSets(b *testing.B) {
	s, _ := NewStore(nil)
	x := s.SetCollection("x", nil)
	keys := make([][]byte, 20)
	for i := 0; i < len(keys); i++ {
		keys[i] = []byte(strconv.Itoa(i % len(keys)))
	}
	v := []byte("")
	b.ResetTimer() // Ignore time from above.
	for i := 0; i < b.N; i++ {
		x.Set(keys[i%len(keys)], v)
	}
}

func BenchmarkGets(b *testing.B) {
	s, _ := NewStore(nil)
	x := s.SetCollection("x", nil)
	v := []byte("")
	for i := 0; i < b.N; i++ {
		x.Set([]byte(strconv.Itoa(i)), v)
	}
	b.ResetTimer() // Ignore time from above.
	for i := 0; i < b.N; i++ {
		x.Get([]byte(strconv.Itoa(i)))
	}
}

func TestSizeof(t *testing.T) {
	t.Logf("sizeof various structs and types, in bytes...")
	t.Logf("  node: %v", unsafe.Sizeof(node{}))
	t.Logf("  nodeLoc: %v", unsafe.Sizeof(nodeLoc{}))
	t.Logf("  Item: %v", unsafe.Sizeof(Item{}))
	t.Logf("  itemLoc: %v", unsafe.Sizeof(itemLoc{}))
	t.Logf("  ploc: %v", unsafe.Sizeof(ploc{}))
	t.Logf("  []byte: %v", unsafe.Sizeof([]byte{}))
	t.Logf("  uint32: %v", unsafe.Sizeof(uint32(0)))
	t.Logf("  uint64: %v", unsafe.Sizeof(uint64(0)))
}

func TestPrivateCollection(t *testing.T) {
	s, _ := NewStore(nil)
	x := s.MakePrivateCollection(nil)
	if x == nil {
		t.Errorf("expected private collection")
	}
	if len(s.GetCollectionNames()) != 0 {
		t.Errorf("expected private collection to be unlisted")
	}
}

func TestBadStoreFile(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	ioutil.WriteFile(fname, []byte("not a real store file"), 0600)
	defer os.Remove(fname)
	f, err := os.Open(fname)
	if err != nil || f == nil {
		t.Errorf("could not reopen file: %v", fname)
	}
	s, err := NewStore(f)
	if err == nil {
		t.Errorf("expected NewStore(f) to fail")
	}
	if s != nil {
		t.Errorf("expected NewStore(f) to fail with s")
	}
}

func TestEvictSomeItems(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, _ := os.Create(fname)
	s, _ := NewStore(f)
	x := s.SetCollection("x", nil)
	numEvicted := uint64(0)
	loadCollection(x, []string{"e", "d", "a", "c", "b", "c", "a"})
	for i := 0; i < 1000; i++ {
		visitExpectCollection(t, x, "a", []string{"a", "b", "c", "d", "e"}, nil)
		s.Flush()
		numEvicted += x.EvictSomeItems()
	}
	for i := 0; i < 1000; i++ {
		visitExpectCollection(t, x, "a", []string{"a", "b", "c", "d", "e"}, nil)
		loadCollection(x, []string{"e", "d", "a"})
		s.Flush()
		numEvicted += x.EvictSomeItems()
	}
	if numEvicted == 0 {
		t.Errorf("expected some evictions")
	}
}

func TestJoinWithFileErrors(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, _ := os.Create(fname)
	defer os.Remove(fname)

	errAfter := 0x1000000
	numReads := 0

	m := &mockfile{
		f: f,
		readat: func(p []byte, off int64) (n int, err error) {
			numReads++
			if numReads >= errAfter {
				return 0, errors.New("mockfile error")
			}
			return f.ReadAt(p, off)
		},
	}

	sOrig, _ := NewStore(m)
	xOrig := sOrig.SetCollection("x", nil)
	if m.numStat != 1 {
		t.Errorf("expected 1 numStat, got: %#v", m)
	}
	if m.numTruncate != 0 {
		t.Errorf("expected 0 truncates, got: %#v", m)
	}

	loadCollection(xOrig, []string{"a", "b", "c", "d", "e", "f"})
	sOrig.Flush()
	if m.numStat != 1 {
		t.Errorf("expected 1 numStat, got: %#v", m)
	}
	if m.numTruncate != 0 {
		t.Errorf("expected 0 truncates, got: %#v", m)
	}

	fname2 := "tmp2.test"
	os.Remove(fname2)
	f2, _ := os.Create(fname2)
	defer os.Remove(fname2)

	sMore, _ := NewStore(f2)
	xMore := sMore.SetCollection("x", nil)
	loadCollection(xMore, []string{"5", "4", "3", "2", "1", "0"})
	sMore.Flush()

	var res *nodeLoc
	var err error

	// ----------------------------------------

	errAfter = 0x10000000 // Attempt with no errors.
	numReads = 0

	s2, _ := NewStore(m)
	x2 := s2.GetCollection("x")

	rnl := x2.root
	if rnl == nil {
		t.Errorf("expected rnl")
	}
	root := rnl.root
	if root == nil || root.isEmpty() {
		t.Errorf("expected an x2 root")
	}
	if root.Loc().isEmpty() {
		t.Errorf("expected an x2 root to be on disk")
	}
	if root.Node() != nil {
		t.Errorf("expected an x2 root to not be loaded into memory yet")
	}

	errAfter = 0x10000000 // Attempt with no errors.
	numReads = 0

	res, err = s2.join(x2, root, empty_nodeLoc, nil)
	if err != nil {
		t.Errorf("expected no error")
	}
	if res.isEmpty() {
		t.Errorf("expected non-empty res")
	}
	if numReads == 0 {
		t.Errorf("expected some reads, got: %v", numReads)
	}

	// ----------------------------------------

	for i := 1; i < 7; i++ {
		errAfter = 0x10000000 // Attempt with no errors.
		numReads = 0

		s2, err = NewStore(m)
		if err != nil {
			t.Errorf("expected NewStore m to work on loop: %v", i)
		}
		s3, err := NewStore(f2)
		if err != nil {
			t.Errorf("expected NewStore f2 to work on loop: %v", i)
		}

		x2 = s2.GetCollection("x")
		x3 := s3.GetCollection("x")

		rnl2 := x2.root
		root2 := rnl2.root
		if root2 == nil || root2.isEmpty() {
			t.Errorf("expected an x2 root")
		}
		if root2.Loc().isEmpty() {
			t.Errorf("expected an x2 root to be on disk")
		}
		if root2.Node() != nil {
			t.Errorf("expected an x2 root to not be loaded into memory yet")
		}

		rnl3 := x3.root
		root3 := rnl3.root
		if root3 == nil || root3.isEmpty() {
			t.Errorf("expected an x3 root")
		}
		if root3.Loc().isEmpty() {
			t.Errorf("expected an x3 root to be on disk")
		}
		if root3.Node() != nil {
			t.Errorf("expected an x3 root to not be loaded into memory yet")
		}

		errAfter = i
		numReads = 0

		res, err = s2.join(x2, root2, root3, nil)
		if err == nil {
			t.Errorf("expected error due to mockfile errorAfter %v, got nil", errAfter)
		}
		if !res.isEmpty() {
			t.Errorf("expected empty res due to mockfile error")
		}
		if numReads == 0 {
			t.Errorf("expected some reads, got: %v", numReads)
		}
	}
}

func TestStoreStats(t *testing.T) {
	freeNodes = nil

	m := map[string]uint64{}
	n := map[string]uint64{}

	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	x := s.SetCollection("x", bytes.Compare)
	s.Stats(m)
	if m["fileSize"] != 0 {
		t.Errorf("expected 0 fileSize, got: %v", m["fileSize"])
	}
	if m["nodeAllocs"] != 0 {
		t.Errorf("expected 0 nodeAllocs, got: %v", m["nodeAllocs"])
	}

	x.Set([]byte("hello"), []byte("world"))
	s.Stats(n)
	if n["fileSize"] != m["fileSize"] {
		t.Errorf("expected 0 fileSize, got: %#v, %#v", n, m)
	}
	if n["nodeAllocs"] != m["nodeAllocs"]+1 {
		t.Errorf("expected 1 nodeAllocs, got: %#v, %#v", n, m)
	}

	x.Set([]byte("hello"), []byte("there"))
	s.Stats(n)
	if n["fileSize"] != m["fileSize"] {
		t.Errorf("expected 0 fileSize, got: %#v, %#v", n, m)
	}
	if n["nodeAllocs"] != m["nodeAllocs"]+3 {
		t.Errorf("expected 3 nodeAllocs, got: %#v, %#v", n, m)
	}

	x.Delete([]byte("hello"))
	s.Stats(n)
	if n["fileSize"] != m["fileSize"] {
		t.Errorf("expected 0 fileSize, got: %#v, %#v", n, m)
	}
	if n["nodeAllocs"] != m["nodeAllocs"]+3 {
		t.Errorf("expected 3 nodeAllocs, got: %#v, %#v", n, m)
	}
}

func TestVisitItemsAscendEx(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	x := s.SetCollection("x", bytes.Compare)
	n := 40
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%v", i)
		x.Set([]byte(k), []byte(k))
	}
	maxDepth := uint64(0)
	t.Logf("dumping tree for manual balancedness check")
	err = x.VisitItemsAscendEx(nil, true, func(i *Item, depth uint64) bool {
		o := ""
		for i := 0; uint64(i) < depth; i++ {
			o = o + "-"
		}
		t.Logf("=%v%v", o, string(i.Key))
		if maxDepth < depth {
			maxDepth = depth
		}
		return true
	})
	if err != nil {
		t.Errorf("expected visit ex to work, got: %v", err)
	}
	if maxDepth >= uint64(n*3/4) {
		t.Errorf("expected maxDepth to not be so unlucky, got: %v, n: %v",
			maxDepth, n)
	}
}

func TestVisitItemsDescend(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	x := s.SetCollection("x", bytes.Compare)
	loadCollection(x, []string{"e", "d", "a", "c", "b", "c", "a"})

	visitDescendExpectCollection(t, x, "z", []string{"e", "d", "c", "b", "a"}, nil)
	visitDescendExpectCollection(t, x, "e", []string{"d", "c", "b", "a"}, nil)
	visitDescendExpectCollection(t, x, "b", []string{"a"}, nil)
	visitDescendExpectCollection(t, x, "a", []string{}, nil)
	visitDescendExpectCollection(t, x, "", []string{}, nil)
}

func TestKeyCompareForCollectionCallback(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, _ := os.Create(fname)
	s, _ := NewStore(f)
	x := s.SetCollection("x", nil)
	loadCollection(x, []string{"e", "d", "a", "c", "b", "c", "a"})
	visitExpectCollection(t, x, "a", []string{"a", "b", "c", "d", "e"}, nil)
	s.Flush()
	f.Close()

	comparisons := 0
	myKeyCompare := func(a, b []byte) int {
		comparisons++
		return bytes.Compare(a, b)
	}

	f1, _ := os.OpenFile(fname, os.O_RDWR, 0666)
	s1, err := NewStoreEx(f1, StoreCallbacks{
		KeyCompareForCollection: func(collName string) KeyCompare {
			return myKeyCompare
		},
	})
	if err != nil {
		t.Errorf("expected NewStoreEx with non-nil KeyCompareForCollection to work")
	}
	x1 := s1.GetCollection("x")
	visitExpectCollection(t, x1, "a", []string{"a", "b", "c", "d", "e"}, nil)
	x1.Get([]byte("a"))
	x1.Get([]byte("b"))

	if comparisons == 0 {
		t.Errorf("expected invocations of myKeyCompare")
	}
	if x1.Name() != "x" {
		t.Errorf("expected same name after reload")
	}
}

func TestMemoryDeleteEveryItem(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	testDeleteEveryItem(t, s, 10000, 100, func() {})
}

func TestPersistDeleteEveryItem(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	defer os.Remove(fname)
	f, _ := os.Create(fname)
	s, _ := NewStore(f)
	c := 0
	testDeleteEveryItem(t, s, 10000, 100, func() {
		c++
		err := s.Flush()
		if err != nil {
			t.Errorf("expected Flush to work, err: %v", err)
		}
	})
	if c == 0 {
		t.Errorf("expected cb to get invoked")
	}
	err := s.Flush()
	if err != nil {
		t.Errorf("expected last Flush to work, err: %v", err)
	}
	f.Close()

	f2, err := os.Open(fname)
	if err != nil {
		t.Errorf("expected re-Open() to work, err: %v", err)
	}
	s2, err := NewStore(f2)
	if err != nil {
		t.Errorf("expected NewStore to work, err: %v", err)
	}
	x := s2.SetCollection("x", bytes.Compare)
	if x == nil {
		t.Errorf("expected x to be there")
	}
	m := 0
	err = x.VisitItemsAscend(nil, true, func(i *Item) bool {
		m++
		return true
	})
	if err != nil {
		t.Errorf("expected Visit to work, err: %v", err)
	}
	if m != 0 {
		t.Errorf("expected 0 items, got: %v", m)
	}
	f2.Close()
}

func testDeleteEveryItem(t *testing.T, s *Store, n int, every int,
	cb func()) {
	x := s.SetCollection("x", bytes.Compare)
	for i := 0; i < n; i++ {
		err := x.Set([]byte(fmt.Sprintf("%d", i)), []byte{})
		if err != nil {
			t.Errorf("expected SetItem to work, %v", i)
		}
		if i%every == 0 {
			cb()
		}
	}
	m := 0
	x.VisitItemsAscend(nil, true, func(i *Item) bool {
		m++
		return true
	})
	if m != n {
		t.Errorf("expected %v items, got: %v", n, m)
	}
	for i := 0; i < n; i++ {
		wasDeleted, err := x.Delete([]byte(fmt.Sprintf("%d", i)))
		if err != nil {
			t.Errorf("expected Delete to work, %v", i)
		}
		if !wasDeleted {
			t.Errorf("expected Delete to actually delete")
		}
		if i%every == 0 {
			cb()
		}
	}
	m = 0
	x.VisitItemsAscend(nil, true, func(i *Item) bool {
		m++
		return true
	})
	if m != 0 {
		t.Errorf("expected 0 items, got: %v", m)
	}
}

func TestItemCopy(t *testing.T) {
	i := &Item{
		Key:       []byte("hi"),
		Val:       []byte("world"),
		Priority:  1234,
		Transient: unsafe.Pointer(&node{}),
	}
	j := i.Copy()
	if !bytes.Equal(j.Key, i.Key) || !bytes.Equal(j.Val, i.Val) ||
		j.Priority != i.Priority || j.Transient != i.Transient {
		t.Errorf("expected item copy to work")
	}
}

func TestCurFreeNodes(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	x := s.SetCollection("x", bytes.Compare)
	n := x.mkNode(empty_itemLoc, empty_nodeLoc, empty_nodeLoc, 0, 0)
	f := allocStats
	x.freeNode_unlocked(n, nil)
	if f.FreeNodes+1 != allocStats.FreeNodes {
		t.Errorf("expected freeNodes to increment")
	}
	if f.CurFreeNodes+1 != allocStats.CurFreeNodes {
		t.Errorf("expected CurFreeNodes + 1 == allocStats.CurrFreeNodes, got: %v, %v",
			f.CurFreeNodes+1, allocStats.CurFreeNodes)
	}
}

func TestCollectionStats(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	x := s.SetCollection("x", bytes.Compare)
	g := x.AllocStats()
	loadCollection(x, []string{"e", "d", "a", "c", "b", "c", "a"})
	h := x.AllocStats()
	if h.MkNodes <= g.MkNodes {
		t.Errorf("expected MkNodes to be more")
	}
	if h.MkNodeLocs <= g.MkNodeLocs {
		t.Errorf("expected MkNodeLocs to be more")
	}
	if h.MkRootNodeLocs <= g.MkRootNodeLocs {
		t.Errorf("expected MkRootNodeLocs to be more")
	}
}

func TestDump(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	x := s.SetCollection("x", bytes.Compare)
	loadCollection(x, []string{"e", "d", "a", "c", "b", "c", "a"})
	fmt.Printf("testing dump\n")
	dump(s, x.root.root, 1)
}

func TestStoreClose(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	if s.coll == unsafe.Pointer(nil) {
		t.Errorf("expected coll before Close()")
	}
	s.Close()
	if s.coll != unsafe.Pointer(nil) {
		t.Errorf("expected no coll after Close()")
	}
	s.Close()
	if s.coll != unsafe.Pointer(nil) {
		t.Errorf("expected no coll after re-Close()")
	}
	s.Close()
	if s.coll != unsafe.Pointer(nil) {
		t.Errorf("expected no coll after re-Close()")
	}

	// Now, with a collection
	s, _ = NewStore(nil)
	s.SetCollection("x", bytes.Compare)
	if s.coll == unsafe.Pointer(nil) {
		t.Errorf("expected coll before Close()")
	}
	s.Close()
	if s.coll != unsafe.Pointer(nil) {
		t.Errorf("expected no coll after Close()")
	}
	s.Close()
	if s.coll != unsafe.Pointer(nil) {
		t.Errorf("expected no coll after Close()")
	}
}

func TestItemNumValBytes(t *testing.T) {
	var x *Collection
	var h *Item
	s, err := NewStoreEx(nil, StoreCallbacks{
		ItemValLength: func(c *Collection, i *Item) int {
			if c != x {
				t.Errorf("expected colls to be the same")
			}
			if h != i {
				t.Errorf("expected items to be the same")
			}
			return 1313
		},
	})
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	x = s.SetCollection("x", bytes.Compare)
	h = &Item{}
	if 1313 != h.NumValBytes(x) {
		t.Errorf("expected NumValBytes to be 1313")
	}
}

func TestReclaimRootChain(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	x := s.SetCollection("x", bytes.Compare)
	x.SetItem(&Item{
		Key:      []byte("a"),
		Val:      []byte("aaa"),
		Priority: 100,
	})
	x.SetItem(&Item{
		Key:      []byte("b"),
		Val:      []byte("bbb"),
		Priority: 200,
	})
	s2 := s.Snapshot()
	x2 := s2.GetCollection("x")
	x.SetItem(&Item{
		Key:      []byte("b"),
		Val:      []byte("bbbb"),
		Priority: 200,
	})
	x.SetItem(&Item{
		Key:      []byte("a"),
		Val:      []byte("aaaa"),
		Priority: 100,
	})
	v, err := x.Get([]byte("a"))
	if v == nil || !bytes.Equal(v, []byte("aaaa")) {
		t.Errorf("expected aaaa, got: %v\n", v)
	}
	v, err = x2.Get([]byte("a"))
	if v == nil || !bytes.Equal(v, []byte("aaa")) {
		t.Errorf("expected aaa, got: %v\n", v)
	}
}

func TestReclaimRootChainMultipleMutations(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	x := s.SetCollection("x", bytes.Compare)
	x.SetItem(&Item{
		Key:      []byte("a"),
		Val:      []byte("aaa"),
		Priority: 100,
	})
	x.SetItem(&Item{
		Key:      []byte("b"),
		Val:      []byte("bbb"),
		Priority: 200,
	})
	s2 := s.Snapshot()
	x2 := s2.GetCollection("x")
	x.SetItem(&Item{
		Key:      []byte("b"),
		Val:      []byte("bbbb"),
		Priority: 200,
	})
	s3 := s.Snapshot()
	x3 := s3.GetCollection("x")
	x.SetItem(&Item{
		Key:      []byte("a"),
		Val:      []byte("aaaa"),
		Priority: 100,
	})
	v, err := x.Get([]byte("a"))
	if v == nil || !bytes.Equal(v, []byte("aaaa")) {
		t.Errorf("expected aaaa, got: %v\n", v)
	}
	v, err = x2.Get([]byte("a"))
	if v == nil || !bytes.Equal(v, []byte("aaa")) {
		t.Errorf("expected aaa, got: %v\n", v)
	}
	v, err = x3.Get([]byte("a"))
	if v == nil || !bytes.Equal(v, []byte("aaa")) {
		t.Errorf("expected aaaa, got: %v\n", v)
	}
	s2.Close()
	v, err = x.Get([]byte("a"))
	if v == nil || !bytes.Equal(v, []byte("aaaa")) {
		t.Errorf("expected aaaa, got: %v\n", v)
	}
	v, err = x3.Get([]byte("a"))
	if v == nil || !bytes.Equal(v, []byte("aaa")) {
		t.Errorf("expected aaa, got: %v\n", v)
	}
}

func TestMemoryFlushRevert(t *testing.T) {
	s, _ := NewStore(nil)
	err := s.FlushRevert()
	if err == nil {
		t.Errorf("expected memory-only FlushRevert to fail")
	}
	x := s.SetCollection("x", bytes.Compare)
	x.SetItem(&Item{
		Key:      []byte("a"),
		Val:      []byte("aaa"),
		Priority: 100,
	})
	err = s.FlushRevert()
	if err == nil {
		t.Errorf("expected memory-only FlushRevert to fail")
	}
}

func TestFlushRevertEmptyStore(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, _ := os.Create(fname)
	s, _ := NewStore(f)
	err := s.FlushRevert()
	if err != nil {
		t.Errorf("expected flush revert on empty store to work, err: %v", err)
	}
	s.SetCollection("x", nil)
	err = s.FlushRevert()
	if err != nil {
		t.Errorf("expected flush revert on empty store to work, err: %v", err)
	}
	if s.GetCollection("x") != nil {
		t.Errorf("expected flush revert to provide no collections")
	}
	s.Flush()
	err = s.FlushRevert()
	if err != nil {
		t.Errorf("expected flush revert on empty store to work, err: %v", err)
	}
	if s.GetCollection("x") != nil {
		t.Errorf("expected flush revert to provide no collections")
	}
	stat, err := f.Stat()
	if err != nil {
		t.Errorf("expected stat to work")
	}
	if stat.Size() != 0 {
		t.Errorf("expected file size to be 0, got: %v", stat.Size())
	}
}

func TestFlushRevert(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, err := os.Create(fname)
	s, _ := NewStore(f)
	x := s.SetCollection("x", nil)
	x.SetItem(&Item{
		Key:      []byte("a"),
		Val:      []byte("aaa"),
		Priority: 100,
	})
	s.Flush()
	stat0, err := f.Stat()
	if err != nil {
		t.Errorf("expected stat to work")
	}
	if stat0.Size() <= rootsLen {
		t.Errorf("expected file size to be >0, got: %v", stat0.Size())
	}
	x.SetItem(&Item{
		Key:      []byte("b"),
		Val:      []byte("bbb"),
		Priority: 200,
	})
	s.Flush()
	stat1, err := f.Stat()
	if err != nil {
		t.Errorf("expected stat to work")
	}
	if stat1.Size() <= stat0.Size()+rootsLen {
		t.Errorf("expected file size to be larger, got: %v vs %v",
			stat1.Size(), stat0.Size()+rootsLen)
	}

	ss := s.Snapshot()
	err = ss.FlushRevert()
	if err != nil {
		t.Errorf("expected flush revert on empty store to work, err: %v", err)
	}
	x = ss.GetCollection("x")
	if x == nil {
		t.Errorf("expected flush revert to still have collection x")
	}
	sstat3, err := f.Stat()
	if err != nil {
		t.Errorf("expected stat to work")
	}
	if sstat3.Size() != stat1.Size() {
		t.Errorf("expected snapshot post-flush-revert file size to same, got: %v vs %v",
			sstat3.Size(), stat1.Size())
	}
	aval, err := x.Get([]byte("a"))
	if err != nil || aval == nil || string(aval) != "aaa" {
		t.Errorf("expected post-flush-revert a to be there, got: %v, %v", aval, err)
	}
	bval, err := x.Get([]byte("b"))
	if err != nil || bval != nil {
		t.Errorf("expected post-flush-revert b to be gone, got: %v, %v", bval, err)
	}
	visitExpectCollection(t, x, "a", []string{"a"}, nil)
	for i := 0; i < 10; i++ {
		err = ss.FlushRevert()
		if err != nil {
			t.Errorf("expected another flush revert to work, err: %v", err)
		}
		x = ss.GetCollection("x")
		if x != nil {
			t.Errorf("expected flush revert to still have no collection")
		}
		statx, err := f.Stat()
		if err != nil {
			t.Errorf("expected stat to work")
		}
		if statx.Size() != stat1.Size() {
			t.Errorf("expected snapshot flush-revert to have same file size, got: %v",
				statx.Size())
		}
	}

	err = s.FlushRevert()
	if err != nil {
		t.Errorf("expected flush revert on empty store to work, err: %v", err)
	}
	x = s.GetCollection("x")
	if x == nil {
		t.Errorf("expected flush revert to still have collection x")
	}
	stat3, err := f.Stat()
	if err != nil {
		t.Errorf("expected stat to work")
	}
	if stat3.Size() != stat0.Size() {
		t.Errorf("expected post-flush-revert file size to be reverted, got: %v vs %v",
			stat3.Size(), stat0.Size())
	}
	aval, err = x.Get([]byte("a"))
	if err != nil || aval == nil || string(aval) != "aaa" {
		t.Errorf("expected post-flush-revert a to be there, got: %v, %v", aval, err)
	}
	bval, err = x.Get([]byte("b"))
	if err != nil || bval != nil {
		t.Errorf("expected post-flush-revert b to be gone, got: %v, %v", bval, err)
	}
	visitExpectCollection(t, x, "a", []string{"a"}, nil)
	for i := 0; i < 10; i++ {
		err = s.FlushRevert()
		if err != nil {
			t.Errorf("expected another flush revert to work, err: %v", err)
		}
		x = s.GetCollection("x")
		if x != nil {
			t.Errorf("expected flush revert to still have no collection")
		}
		statx, err := f.Stat()
		if err != nil {
			t.Errorf("expected stat to work")
		}
		if statx.Size() != 0 {
			t.Errorf("expected too many flush-revert to bring file size 0, got: %v",
				statx.Size())
		}
	}
}

func TestFlushRevertWithReadError(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, err := os.Create(fname)
	defer os.Remove(fname)

	readShouldErr := false
	m := &mockfile{
		f: f,
		readat: func(p []byte, off int64) (n int, err error) {
			if readShouldErr {
				return 0, errors.New("mockfile error")
			}
			return f.ReadAt(p, off)
		},
	}

	s, _ := NewStore(m)
	x := s.SetCollection("x", nil)
	x.SetItem(&Item{
		Key:      []byte("a"),
		Val:      []byte("aaa"),
		Priority: 100,
	})
	s.Flush()

	readShouldErr = true
	err = s.FlushRevert()
	if err == nil {
		t.Errorf("expected FlushRevert to error")
	}
}

func TestCollectionMisc(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	x := s.SetCollection("x", bytes.Compare)

	e0, e1, e2, err := s.split(x, empty_nodeLoc, nil, nil)
	if err != nil ||
		e0 != empty_nodeLoc ||
		e1 != empty_nodeLoc ||
		e2 != empty_nodeLoc {
		t.Errorf("expected split of empty node loc to be empty")
	}

	b, err := x.MarshalJSON()
	if err != nil {
		t.Errorf("expected MarshalJSON to work")
	}
	c, err := x.rootAddRef().MarshalJSON()
	if err != nil {
		t.Errorf("expected MarshalJSON to work")
	}
	if !bytes.Equal(b, c) {
		t.Errorf("expected MarshalJSON to be same")
	}
	if x.Write() != nil {
		t.Errorf("expected Write to be nil")
	}
	if x.UnmarshalJSON([]byte{0}) == nil {
		t.Errorf("expected UnmarshalJSON to fail")
	}
	if x.UnmarshalJSON([]byte("{}")) == nil {
		t.Errorf("expected UnmarshalJSON to fail")
	}
	i := &Item{
		Key:      []byte("a"),
		Val:      []byte("aaa"),
		Priority: 100,
	}
	x.SetItem(i)
	i.Key = nil // Introduce bad condition.
	_, err = x.Get([]byte("a"))
	if err == nil {
		t.Errorf("expected Get() to err")
	}
	i.Key = []byte("a")
}

func TestDoubleFreeNode(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	x := s.SetCollection("x", bytes.Compare)
	n := x.mkNode(nil, nil, nil, 1, 0)
	c := 0
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic with double free")
		}
		if c != 2 {
			t.Errorf("expected c to be 2")
		}
	}()
	withAllocLocks(func() {
		x.freeNode_unlocked(nil, nil)
		c++
		x.freeNode_unlocked(n, nil)
		c++
		x.freeNode_unlocked(n, nil)
		c++
	})
}

func TestDoubleFreeNodeLoc(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	x := s.SetCollection("x", bytes.Compare)
	n := x.mkNode(nil, nil, nil, 1, 0)
	nl := x.mkNodeLoc(n)
	c := 0
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic with double free")
		}
		if c != 2 {
			t.Errorf("expected c to be 2")
		}
	}()
	x.freeNodeLoc(nil)
	c++
	x.freeNodeLoc(nl)
	c++
	x.freeNodeLoc(nl)
	c++
}

func TestDoubleFreeRootNodeLoc(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	x := s.SetCollection("x", bytes.Compare)
	n := x.mkNode(nil, nil, nil, 1, 0)
	nl := x.mkNodeLoc(n)
	rnl := x.mkRootNodeLoc(nl)
	c := 0
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic with double free")
		}
		if c != 2 {
			t.Errorf("expected c to be 2")
		}
	}()
	x.freeRootNodeLoc(nil)
	c++
	x.freeRootNodeLoc(rnl)
	c++
	x.freeRootNodeLoc(rnl)
	c++
}

func TestNodeLocRead(t *testing.T) {
	var nl *nodeLoc
	n, err := nl.read(nil)
	if n != nil || err != nil {
		t.Errorf("expected nodeLoc.read() err")
	}
}

func TestNumInfo(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, err := os.Create(fname)
	defer os.Remove(fname)

	readShouldErr := false
	m := &mockfile{
		f: f,
		readat: func(p []byte, off int64) (n int, err error) {
			if readShouldErr {
				return 0, errors.New("mockfile error")
			}
			return f.ReadAt(p, off)
		},
	}

	s, _ := NewStore(m)
	x := s.SetCollection("x", nil)
	x.SetItem(&Item{
		Key:      []byte("a"),
		Val:      []byte("aaa"),
		Priority: 100,
	})
	s.Flush()

	readShouldErr = true

	rnl := x.rootAddRef()
	rnl.root.node = unsafe.Pointer(nil) // Evict node from memory to force reading.

	_, _, _, _, err = numInfo(s, rnl.root, empty_nodeLoc)
	if err == nil {
		t.Errorf("expected numInfo to error")
	}
	_, _, _, _, err = numInfo(s, empty_nodeLoc, rnl.root)
	if err == nil {
		t.Errorf("expected numInfo to error")
	}
}

func TestWriteEmptyItemsErr(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, _ := os.Create(fname)
	defer os.Remove(fname)

	writeShouldErr := false
	m := &mockfile{
		f: f,
		writeat: func(p []byte, off int64) (n int, err error) {
			if writeShouldErr {
				return 0, errors.New("mockfile error")
			}
			return f.WriteAt(p, off)
		},
	}

	s, _ := NewStore(m)

	writeShouldErr = true
	if s.Flush() == nil {
		t.Errorf("expected Flush() to error")
	}
}

func TestWriteItemsErr(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, _ := os.Create(fname)
	defer os.Remove(fname)

	writeShouldErr := false
	m := &mockfile{
		f: f,
		writeat: func(p []byte, off int64) (n int, err error) {
			if writeShouldErr {
				return 0, errors.New("mockfile error")
			}
			return f.WriteAt(p, off)
		},
	}

	s, _ := NewStore(m)
	x := s.SetCollection("x", nil)
	x.SetItem(&Item{
		Key:      []byte("b"),
		Val:      []byte("bbb"),
		Priority: 100,
	})
	x.SetItem(&Item{
		Key:      []byte("c"),
		Val:      []byte("ccc"),
		Priority: 10,
	})
	x.SetItem(&Item{
		Key:      []byte("a"),
		Val:      []byte("aaa"),
		Priority: 10,
	})

	writeShouldErr = true
	if s.Flush() == nil {
		t.Errorf("expected Flush() to error")
	}
}

func TestStatErr(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, err := os.Create(fname)
	defer os.Remove(fname)

	s, _ := NewStore(f)
	x := s.SetCollection("x", nil)
	x.SetItem(&Item{
		Key:      []byte("a"),
		Val:      []byte("aaa"),
		Priority: 100,
	})
	s.Flush()

	f2, err := os.Open(fname) // Test reading the file.

	m := &mockfile{
		f: f2,
		stat: func() (fi os.FileInfo, err error) {
			return nil, errors.New("mockfile error")
		},
	}
	s2, err := NewStore(m)
	if err == nil {
		t.Errorf("expected store open to fail due to stat err")
	}
	if s2 != nil {
		t.Errorf("expected store open to fail due to stat err")
	}
}

func TestNodeLocWriteErr(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, _ := os.Create(fname)
	defer os.Remove(fname)

	writeShouldErr := false
	m := &mockfile{
		f: f,
		writeat: func(p []byte, off int64) (n int, err error) {
			if writeShouldErr {
				return 0, errors.New("mockfile error")
			}
			return f.WriteAt(p, off)
		},
	}

	s, _ := NewStore(m)
	x := s.SetCollection("x", nil)
	x.SetItem(&Item{
		Key:      []byte("b"),
		Val:      []byte("bbb"),
		Priority: 100,
	})
	rnl := x.rootAddRef()

	writeShouldErr = true
	if rnl.root.write(s) == nil {
		t.Errorf("expected write node to fail")
	}
	writeShouldErr = false

	rnl.root.node = unsafe.Pointer(nil) // Force a nil node.
	if rnl.root.write(s) != nil {
		t.Errorf("expected write node on nil node to work")
	}
}

func TestStoreRefCount(t *testing.T) {
	counts := map[string]int{}
	nadds := 0
	ndecs := 0
	s, err := NewStoreEx(nil, StoreCallbacks{
		ItemAddRef: func(c *Collection, i *Item) {
			counts[string(i.Key)]++
			if counts[string(i.Key)] <= 0 {
				t.Errorf("in ItemAddRef, count for k: %s was <= 0", string(i.Key))
			}
			nadds++
		},
		ItemDecRef: func(c *Collection, i *Item) {
			counts[string(i.Key)]--
			if counts[string(i.Key)] < 0 {
				t.Errorf("in ItemDecRef, count for k: %s was <= 0", string(i.Key))
			}
			if counts[string(i.Key)] == 0 {
				delete(counts, string(i.Key))
			}
			ndecs++
		},
	})
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStoreEx to work")
	}
	mustJustOneRef := func(msg string) {
		for k, count := range counts {
			if count != 1 {
				t.Errorf("expect count 1 for k: %s, got: %v, msg: %s", k, count, msg)
			}
		}
	}
	mustn := func(ea, ed int, msg string) {
		if ea != nadds {
			t.Errorf("expected nadds: %v, got: %v, msg: %s", ea, nadds, msg)
		}
		if ed != ndecs {
			t.Errorf("expected ndecs: %v, got: %v, msg: %s", ed, ndecs, msg)
		}
	}
	mustn(0, 0, "")
	s.SetCollection("x", bytes.Compare)
	mustn(0, 0, "")
	x := s.GetCollection("x")
	if x == nil {
		t.Errorf("expected SetColl/GetColl to work")
	}
	mustn(0, 0, "")
	if x.Name() != "x" {
		t.Errorf("expected name to be same")
	}
	x2 := s.GetCollection("x")
	if x2 != x {
		t.Errorf("expected 2nd GetColl to work")
	}
	mustn(0, 0, "")
	if x2.Name() != "x" {
		t.Errorf("expected name to be same")
	}
	mustn(0, 0, "")
	numItems, numBytes, err := x2.GetTotals()
	if err != nil || numItems != 0 || numBytes != 0 {
		t.Errorf("expected empty memory coll to be empty")
	}
	mustn(0, 0, "")

	tests := []struct {
		op  string
		val string
		pri int
		exp string
	}{
		{"ups", "a", 100, ""},
		{"ups", "b", 200, ""},
		{"ups", "a", 300, ""},
		{"ups", "c", 200, ""},
		{"ups", "a", 100, ""},
		{"ups", "b", 200, ""},
		{"ups", "a", 300, ""},
		{"ups", "b", 100, ""},
		{"ups", "c", 300, ""},
		{"del", "a", 0, ""},
		{"del", "b", 0, ""},
		{"del", "c", 0, ""},
	}

	for testIdx, test := range tests {
		switch test.op {
		case "get":
			i, err := x.GetItem([]byte(test.val), true)
			if err != nil {
				t.Errorf("test: %v, expected get nil error, got: %v",
					testIdx, err)
			}
			if i != nil {
				s.ItemDecRef(x, i)
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
			_, err := x.Delete([]byte(test.val))
			if err != nil {
				t.Errorf("test: %v, expected del nil error, got: %v",
					testIdx, err)
			}
		}
	}

	mustJustOneRef("final")
}

func TestStoreRefCountRandom(t *testing.T) {
	counts := map[string]int{}
	mustJustOneRef := func(msg string) {
		for k, count := range counts {
			if count != 1 {
				t.Fatalf("expect count 1 for k: %s, got: %v, msg: %s",
					k, count, msg)
			}
		}
	}
	mustJustOneRef("")

	s, err := NewStoreEx(nil, StoreCallbacks{
		ItemAddRef: func(c *Collection, i *Item) {
			k := fmt.Sprintf("%s-%s", i.Key, string(i.Val))
			counts[k]++
			if counts[k] <= 0 {
				t.Errorf("in ItemAddRef, count for k: %s was <= 0", k)
			}
		},
		ItemDecRef: func(c *Collection, i *Item) {
			k := fmt.Sprintf("%s-%s", i.Key, string(i.Val))
			counts[k]--
			if counts[k] < 0 {
				t.Errorf("in ItemDecRef, count for k: %s was <= 0", k)
			}
			if counts[k] == 0 {
				delete(counts, k)
			}
		},
	})
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStoreEx to work")
	}

	x := s.SetCollection("x", bytes.Compare)

	numSets := 0
	numKeys := 10
	for i := 0; i < 100; i++ {
		for j := 0; j < 1000; j++ {
			ks := fmt.Sprintf("%03d", rand.Int()%numKeys)
			k := []byte(ks)
			r := rand.Int() % 2
			switch r {
			case 0:
				numSets++
				v := fmt.Sprintf("%d", numSets)
				pri := rand.Int31()
				err := x.SetItem(&Item{
					Key:      k,
					Val:      []byte(v),
					Priority: pri,
				})
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			case 1:
				_, err := x.Delete(k)
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			}
		}
		for k := 0; k < numKeys; k++ {
			_, err := x.Delete([]byte(fmt.Sprintf("%03d", k)))
			if err != nil {
				t.Fatalf("expected nil error, got: %v", err)
			}
		}
		mustJustOneRef(fmt.Sprintf("i: %d", i))
	}
}

func TestPersistRefCountRandom(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, _ := os.Create(fname)
	defer os.Remove(fname)

	counts := map[string]int{}

	itemAddRef := func(c *Collection, i *Item) {
		if i.Val == nil {
			return
		}
		k := fmt.Sprintf("%s-%s", i.Key, string(i.Val))
		counts[k]++
		if counts[k] <= 0 {
			t.Fatalf("in ItemAddRef, count for k: %s was <= 0", k)
		}
	}

	itemDecRef := func(c *Collection, i *Item) {
		if i.Val == nil {
			return
		}
		k := fmt.Sprintf("%s-%s", i.Key, string(i.Val))
		if counts[k] == 0 {
			t.Fatalf("in ItemDecRef, count for k: %s at 0", k)
		}
		counts[k]--
		if counts[k] == 0 {
			delete(counts, k)
		}
	}

	start := func(f *os.File) (*Store, *Collection, map[string]int) {
		s, err := NewStoreEx(f, StoreCallbacks{
			ItemAddRef: itemAddRef,
			ItemDecRef: itemDecRef,
			ItemValRead: func(c *Collection, i *Item,
				r io.ReaderAt, offset int64, valLength uint32) error {
				i.Val = make([]byte, valLength)
				_, err := r.ReadAt(i.Val, offset)
				itemAddRef(c, i)
				return err
			},
		})
		if err != nil || s == nil {
			t.Errorf("expected memory-only NewStoreEx to work")
		}
		x := s.SetCollection("x", bytes.Compare)
		return s, x, counts
	}

	s, x, counts := start(f)

	stop := func() {
		s.Flush()
		s.Close()
		if len(counts) != 0 {
			t.Errorf("counts not empty after Close(), got: %#v\n", counts)
		}
		f.Close()
	}

	mustJustOneRef := func(msg string) {
		for k, count := range counts {
			if count != 1 {
				t.Fatalf("expect count 1 for k: %s, got: %v, msg: %s, counts: %#v",
					k, count, msg, counts)
			}
		}
	}
	mustJustOneRef("")

	numSets := 0
	numKeys := 10
	for i := 0; i < 100; i++ {
		for j := 0; j < 1000; j++ {
			ks := fmt.Sprintf("%03d", rand.Int()%numKeys)
			k := []byte(ks)
			r := rand.Int() % 100
			if r < 60 {
				numSets++
				v := fmt.Sprintf("%d", numSets)
				pri := rand.Int31()
				err := x.SetItem(&Item{
					Key:      k,
					Val:      []byte(v),
					Priority: pri,
				})
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			} else if r < 90 {
				_, err := x.Delete(k)
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			} else {
				// Close and reopen the store.
				stop()
				f, _ = os.OpenFile(fname, os.O_RDWR, 0666)
				s, x, counts = start(f)
			}
		}
		for k := 0; k < numKeys; k++ {
			_, err := x.Delete([]byte(fmt.Sprintf("%03d", k)))
			if err != nil {
				t.Fatalf("expected nil error, got: %v", err)
			}
		}
		mustJustOneRef(fmt.Sprintf("i: %d", i))
	}
}

func TestEvictRefCountRandom(t *testing.T) {
	fname := "tmp.test"
	os.Remove(fname)
	f, _ := os.Create(fname)
	defer os.Remove(fname)

	start := func(f *os.File) (*Store, *Collection, map[string]int) {
		counts := map[string]int{}
		var s *Store
		var x *Collection
		var err error
		itemAddRef := func(c *Collection, i *Item) {
			k := fmt.Sprintf("%s-%s", i.Key, string(i.Val))
			if i.Val == nil {
				return
			}
			counts[k]++
			if counts[k] <= 0 {
				t.Fatalf("in ItemAddRef, count for k: %s was <= 0", k)
			}
		}
		itemDecRef := func(c *Collection, i *Item) {
			k := fmt.Sprintf("%s-%s", i.Key, string(i.Val))
			if i.Val == nil {
				return
			}
			if counts[k] == 0 {
				if x.root != nil {
					dump(s, x.root.root, 1)
				}
				t.Fatalf("in ItemDecRef, count for k: %s at 0, counts: %#v",
					k, counts)
			}
			counts[k]--
			if counts[k] == 0 {
				delete(counts, k)
			}
		}
		s, err = NewStoreEx(f, StoreCallbacks{
			ItemAddRef: itemAddRef,
			ItemDecRef: itemDecRef,
			ItemValRead: func(c *Collection, i *Item,
				r io.ReaderAt, offset int64, valLength uint32) error {
				i.Val = make([]byte, valLength)
				_, err := r.ReadAt(i.Val, offset)
				itemAddRef(c, i)
				return err
			},
		})
		if err != nil || s == nil {
			t.Errorf("expected memory-only NewStoreEx to work")
		}
		x = s.SetCollection("x", bytes.Compare)
		return s, x, counts
	}

	s, x, counts := start(f)

	stop := func() {
		s.Flush()
		s.Close()
		if len(counts) != 0 {
			t.Errorf("counts not empty after Close(), got: %#v\n", counts)
		}
		f.Close()
		f = nil
	}

	mustJustOneRef := func(msg string) {
		for k, count := range counts {
			if count != 1 {
				t.Fatalf("expect count 1 for k: %s, got: %v, msg: %s, counts: %#v",
					k, count, msg, counts)
			}
		}
	}
	mustJustOneRef("")

	numSets := 0
	numKeys := 10
	for i := 0; i < 10; i++ {
		for j := 0; j < 1000; j++ {
			ks := fmt.Sprintf("%03d", rand.Int()%numKeys)
			k := []byte(ks)
			r := rand.Int() % 100
			if r < 30 {
				i, err := x.GetItem(k, true)
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
				if i != nil {
					s.ItemDecRef(x, i)
				}
			} else if r < 60 {
				numSets++
				v := fmt.Sprintf("%d", numSets)
				pri := rand.Int31()
				err := x.SetItem(&Item{
					Key:      k,
					Val:      []byte(v),
					Priority: pri,
				})
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
			} else if r < 75 {
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
				s, x, counts = start(f)
			}
		}
		x.EvictSomeItems()
		for k := 0; k < numKeys; k++ {
			_, err := x.Delete([]byte(fmt.Sprintf("%03d", k)))
			if err != nil {
				t.Fatalf("expected nil error, got: %v", err)
			}
		}
		mustJustOneRef(fmt.Sprintf("i: %d", i))
	}
}

// perm returns a random permutation of n Int items in the range [0, n).
func perm(n int) (out [][]byte) {
    for _, v := range rand.Perm(n) {
        out = append(out, []byte(strconv.Itoa(v)))
    }
    return out
}

const benchmarkSize = 10000

func BenchmarkRandInsert(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkSize)
	i := 0
	b.StartTimer()
	for i < b.N {
		b.StopTimer()
		tr, _ := NewStore(nil)
		x := tr.SetCollection("x", nil)
		b.StartTimer()
		for _, item := range insertP {
			x.Set(item, item)
			i++
			if i >= b.N {
				return
			}
		}
	}
}

func BenchmarkRandDelete(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkSize)
	removeP := perm(benchmarkSize)
	i := 0
	b.StartTimer()
	for i < b.N {
		b.StopTimer()
		tr, _ := NewStore(nil)
		x := tr.SetCollection("x", nil)
		for _, item := range insertP {
			x.Set(item, item)
		}
		b.StartTimer()
		for _, item := range removeP {
			x.Delete(item)
			i++
			if i >= b.N {
				return
			}
		}
	}
}

func BenchmarkRandGet(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkSize)
	removeP := perm(benchmarkSize)
	i := 0
	b.StartTimer()
	for i < b.N {
		b.StopTimer()
		tr, _ := NewStore(nil)
		x := tr.SetCollection("x", nil)
		for _, item := range insertP {
			x.Set(item, item)
		}
		b.StartTimer()
		for _, item := range removeP {
			x.Get(item)
			i++
			if i >= b.N {
				return
			}
		}
	}
}
