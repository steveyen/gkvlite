package treap

type Treap struct {
	compare Compare
}

// Compare returns an integer comparing the two items
// lexicographically. The result will be 0 if a==b, -1 if a < b, and
// +1 if a > b.
type Compare func(a, b Item) int

// Item can be anything.
type Item interface{}

