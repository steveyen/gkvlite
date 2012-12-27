package treap

import (
	"testing"
)

func TestTreap(t *testing.T) {
	x := NewTreap(nil)
	if x == nil {
		t.Errorf("expected NewTreap to work")
	}
}
