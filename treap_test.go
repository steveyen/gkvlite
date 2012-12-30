package gtreap

import (
	"bytes"
	"testing"
)

func stringCompare(a, b interface{}) int {
	return bytes.Compare([]byte(a.(string)), []byte(b.(string)))
}

func TestTreap(t *testing.T) {
	x := NewTreap(stringCompare)
	if x == nil {
		t.Errorf("expected NewTreap to work")
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
			i := x.Get(test.val)
			if i != test.exp && !(i == nil && test.exp == "NIL") {
				t.Errorf("test: %v, on Get, expected: %v, got: %v", testIdx, test.exp, i)
			}
		case "ups":
			x = x.Upsert(test.val, test.pri)
		case "del":
			x = x.Delete(test.val)
		}
	}
}

func load(x *Treap, arr []string) *Treap {
	for i, s := range arr {
		x = x.Upsert(s, i)
	}
	return x
}

func visitExpect(t *testing.T, x *Treap, start string, arr []string) {
	n := 0
	x.VisitAscend(start, func(i Item) bool {
		if i.(string) != arr[n] {
			t.Errorf("expected visit item: %v, saw: %v", arr[n], i)
		}
		n++
		return true
	})
	if n != len(arr) {
		t.Errorf("expected # visit callbacks: %v, saw: %v", len(arr), n)
	}
}

func TestVisit(t *testing.T) {
	x := NewTreap(stringCompare)
	visitExpect(t, x, "a", []string{})

	x = load(x, []string{"e", "d", "c", "c", "a", "b", "a"})

	visitX := func() {
		visitExpect(t, x, "a", []string{"a", "b", "c", "d", "e"})
		visitExpect(t, x, "a1", []string{"b", "c", "d", "e"})
		visitExpect(t, x, "b", []string{"b", "c", "d", "e"})
		visitExpect(t, x, "b1", []string{"c", "d", "e"})
		visitExpect(t, x, "c", []string{"c", "d", "e"})
		visitExpect(t, x, "c1", []string{"d", "e"})
		visitExpect(t, x, "d", []string{"d", "e"})
		visitExpect(t, x, "d1", []string{"e"})
		visitExpect(t, x, "e", []string{"e"})
		visitExpect(t, x, "f", []string{})
	}
	visitX()

	var y *Treap
	y = x.Upsert("f", 1)
	y = y.Delete("a")
	y = y.Upsert("cc", 2)
	y = y.Delete("c")

	visitExpect(t, y, "a", []string{"b", "cc", "d", "e", "f"})
	visitExpect(t, y, "a1", []string{"b", "cc", "d", "e", "f"})
	visitExpect(t, y, "b", []string{"b", "cc", "d", "e", "f"})
	visitExpect(t, y, "b1", []string{"cc", "d", "e", "f"})
	visitExpect(t, y, "c", []string{"cc", "d", "e", "f"})
	visitExpect(t, y, "c1", []string{"cc", "d", "e", "f"})
	visitExpect(t, y, "d", []string{"d", "e", "f"})
	visitExpect(t, y, "d1", []string{"e", "f"})
	visitExpect(t, y, "e", []string{"e", "f"})
	visitExpect(t, y, "f", []string{"f"})
	visitExpect(t, y, "z", []string{})

	// The x treap should be unchanged.
	visitX()

	if x.Min() != "a" {
		t.Errorf("expected min of a")
	}
	if x.Max() != "e" {
		t.Errorf("expected max of d")
	}
	if y.Min() != "b" {
		t.Errorf("expected min of b")
	}
	if y.Max() != "f" {
		t.Errorf("expected max of f")
	}
}
