package treap

import (
	"testing"
)

func TestTreap(t *testing.T) {
	x := NewTreap(nil)
	if x == nil {
		t.Errorf("expected NewTreap to work")
	}
	if x.Count() != 0 {
		t.Errorf("expected 0 count")
	}
	i := x.Get("not-there")
	if i != nil {
		t.Errorf("expected no item")
	}
}
