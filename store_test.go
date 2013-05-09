package gkvlite

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"unsafe"
)

type mockfile struct {
	f       *os.File
	readat  func(p []byte, off int64) (n int, err error)
	writeat func(p []byte, off int64) (n int, err error)
}

func (m *mockfile) ReadAt(p []byte, off int64) (n int, err error) {
	if m.readat != nil {
		return m.readat(p, off)
	}
	return m.f.ReadAt(p, off)
}

func (m *mockfile) WriteAt(p []byte, off int64) (n int, err error) {
	if m.writeat != nil {
		return m.writeat(p, off)
	}
	return m.f.WriteAt(p, off)
}

func (m *mockfile) Stat() (fi os.FileInfo, err error) {
	return m.f.Stat()
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

	loadCollection(x3snap, []string{"e", "d", "a", "c", "b", "c", "a"})
	visitExpectCollection(t, x3snap1, "a", []string{"a", "b", "c"}, nil)
	visitExpectCollection(t, x3, "a", []string{"a", "b", "c"}, nil)

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
	v := []byte("")
	for i := 0; i < b.N; i++ {
		x.Set([]byte(strconv.Itoa(i)), v)
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
	loadCollection(xOrig, []string{"a", "b", "c", "d", "e", "f"})
	sOrig.Flush()

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

	rnl := (*rootNodeLoc)(atomic.LoadPointer(&x2.root))
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

	var reclaimable *node
	res, err = s2.join(x2, root, empty_nodeLoc, &reclaimable)
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

		rnl2 := (*rootNodeLoc)(atomic.LoadPointer(&x2.root))
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

		rnl3 := (*rootNodeLoc)(atomic.LoadPointer(&x3.root))
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

		var reclaimable *node
		res, err = s2.join(x2, root2, root3, &reclaimable)
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
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	x := s.SetCollection("x", bytes.Compare)
	m := map[string]uint64{}
	s.Stats(m)
	if m["fileSize"] != 0 {
		t.Errorf("expected 0 fileSize, got: %v", m["fileSize"])
	}
	if m["nodeAllocs"] != 0 {
		t.Errorf("expected 0 nodeAllocs, got: %v", m["nodeAllocs"])
	}

	x.Set([]byte("hello"), []byte("world"))
	s.Stats(m)
	if m["fileSize"] != 0 {
		t.Errorf("expected 0 fileSize, got: %v", m["fileSize"])
	}
	if m["nodeAllocs"] != 1 {
		t.Errorf("expected 1 nodeAllocs, got: %v", m["nodeAllocs"])
	}

	x.Set([]byte("hello"), []byte("there"))
	s.Stats(m)
	if m["fileSize"] != 0 {
		t.Errorf("expected 0 fileSize, got: %v", m["fileSize"])
	}
	if m["nodeAllocs"] != 3 {
		t.Errorf("expected 3 nodeAllocs, got: %v", m["nodeAllocs"])
	}

	x.Delete([]byte("hello"))
	s.Stats(m)
	if m["fileSize"] != 0 {
		t.Errorf("expected 0 fileSize, got: %v", m["fileSize"])
	}
	if m["nodeAllocs"] != 3 {
		t.Errorf("expected 3 nodeAllocs, got: %v", m["nodeAllocs"])
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
