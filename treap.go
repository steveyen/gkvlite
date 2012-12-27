package treap

type Treap struct {
	compare Compare
	count   int
	root    *node
}

// Compare returns an integer comparing the two items
// lexicographically. The result will be 0 if a==b, -1 if a < b, and
// +1 if a > b.
type Compare func(a, b Item) int

// Key and Item can be anything.
type Item interface{}

type node struct {
	item     Item
	priority int
	left     *node
	right    *node
}

func NewTreap(c Compare) *Treap {
	return &Treap{compare: c, count: 0, root: nil}
}

func (t *Treap) Count() int {
	return t.count
}

func (t *Treap) Get(target Item) Item {
	if t.root == nil {
		return nil
	}
	return t.get(t.root, target)
}

func (t *Treap) get(n *node, target Item) Item {
	for {
		if n == nil {
			break
		}
		c := t.compare(target, n.item)
		if c < 0 {
			n = n.left
		} else if c > 0 {
			n = n.right
		} else {
			return n.item
		}
	}
	return nil
}
