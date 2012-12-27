package treap

type Treap struct {
	compare    Compare
	count      int
	root       *node
}

// Compare returns an integer comparing the two items
// lexicographically. The result will be 0 if a==b, -1 if a < b, and
// +1 if a > b.
type Compare func(a, b Item) int

// Key and Item can be anything.
type Key interface{}
type Item interface{}

type node struct {
	key      Key
	item     Item
	priority int
	left     *node
	right    *node
}

func NewTreap(c Compare) *Treap {
	return &Treap{compare: c, root: nil}
}

func (t *Treap) Count() int {
	return t.count
}

func (t *Treap) Get(key Key) Item {
	if t.root == nil {
		return nil
	}
	return t.root.get(key)
}

func (n *node) get(key Key) Item {
	return nil
}