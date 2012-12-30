package gtreap

import (
	"bytes"
	"testing"
)

func TestStoreMem(t *testing.T) {
	s, err := NewStore(nil)
	if err != nil || s == nil {
		t.Errorf("expected memory-only NewStore to work")
	}
	s.AddCollection("x", bytes.Compare)
	x := s.GetCollection("x")
	if x == nil {
		t.Errorf("expected AddColl/GetColl to work")
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
			err := x.Upsert(&PItem{
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

func loadPTreap(x *PTreap, arr []string) {
	for i, s := range arr {
		x.Upsert(&PItem{
			Key:      []byte(s),
			Val:      []byte(s),
			Priority: int32(i),
		})
	}
}

func visitExpectPTreap(t *testing.T, x *PTreap, start string, arr []string) {
	n := 0
	err := x.VisitAscend([]byte(start), true, func(i *PItem) bool {
		if string(i.Key) != arr[n] {
			t.Errorf("expected visit item: %v, saw: %v", arr[n], i)
		}
		n++
		return true
	})
	if err != nil {
		t.Errorf("expected no mem visit error")
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
	x := s.AddCollection("x", bytes.Compare)
	if s.Flush() == nil {
		t.Errorf("expected in-memory store Flush() error")
	}

	visitExpectPTreap(t, x, "a", []string{})
	min, err := x.Min(true)
	if err != nil || min != nil {
		t.Errorf("expected no min, got: %v, err: %v", min, err)
	}
	max, err := x.Max(true)
	if err != nil || max != nil {
		t.Errorf("expected no max, got: %v, err: %v", max, err)
	}

	loadPTreap(x, []string{"e", "d", "a", "c", "b", "c", "a"})
	if s.Flush() == nil {
		t.Errorf("expected in-memory store Flush() error")
	}

	visitX := func() {
		visitExpectPTreap(t, x, "a", []string{"a", "b", "c", "d", "e"})
		visitExpectPTreap(t, x, "a1", []string{"b", "c", "d", "e"})
		visitExpectPTreap(t, x, "b", []string{"b", "c", "d", "e"})
		visitExpectPTreap(t, x, "b1", []string{"c", "d", "e"})
		visitExpectPTreap(t, x, "c", []string{"c", "d", "e"})
		visitExpectPTreap(t, x, "c1", []string{"d", "e"})
		visitExpectPTreap(t, x, "d", []string{"d", "e"})
		visitExpectPTreap(t, x, "d1", []string{"e"})
		visitExpectPTreap(t, x, "e", []string{"e"})
		visitExpectPTreap(t, x, "f", []string{})
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
