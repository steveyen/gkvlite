package treap

type Treap struct {
	compare    Compare
	prioritize Prioritize
	root       *node
}

// Compare returns an integer comparing the two items
// lexicographically. The result will be 0 if a==b, -1 if a < b, and
// +1 if a > b.
type Compare func(a, b Item) int

type Prioritize func(a Item) int

// Item can be anything.
type Item interface{}

type node struct {
	item     Item
	priority int
	left     *node
	right    *node
}

func NewTreap(c Compare, p Prioritize) *Treap {
	return &Treap{compare: c, prioritize: p, root: nil}
}