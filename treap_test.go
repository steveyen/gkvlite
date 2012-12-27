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
	i := x.Get("not-there")
	if i != nil {
		t.Errorf("expected no item")
	}
	x = x.Upsert("a", 1)
	i = x.Get("a")
	if i != "a" {
		t.Errorf("expected item")
	}
}
