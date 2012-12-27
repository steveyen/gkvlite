package treap

import (
	"bytes"
	"testing"
)

func stringCompare(a, b Item) int {
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
