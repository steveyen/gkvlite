package gkvlite

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// Item defines A persistable item.
type Item struct {
	Transient interface{} // For any ephemeral data.
	Key, Val  []byte      // Val may be nil if not fetched into memory yet.
	Priority  int32       // Use rand.Int31() for probabilistic balancing.
}

var itemLocGL = sync.RWMutex{}

const itemLocMutex = false

// A persistable item and its persistence location.
type itemLoc struct {
	loc  *ploc // can be nil if item is dirty (not yet persisted).
	item *Item // can be nil if item is not fetched into memory yet.
}

var emptyItemLoc = itemLoc{}

// The Key pointer type
type keyP uint32

const keyPSize = 4 // Number of bytes needed to store above

// NumBytes required by the structure
// Number of Key bytes plus number of Val bytes.
func (i *Item) NumBytes(c *Collection) int {
	return len(i.Key) + i.NumValBytes(c)
}

// NumValBytes returns the bumber of bytes required by the item value
func (i *Item) NumValBytes(c *Collection) int {
	if c.store.callbacks.ItemValLength != nil {
		return c.store.callbacks.ItemValLength(c, i)
	}
	return len(i.Val)
}

// Copy returns a copy of the item
// The returned Item will not have been allocated through the optional
// StoreCallbacks.ItemAlloc() callback.
func (i *Item) Copy() *Item {
	return &Item{
		Key:       i.Key,
		Val:       i.Val,
		Priority:  i.Priority,
		Transient: i.Transient,
	}
}

// Loc return the location of the item
func (iloc *itemLoc) Loc() *ploc {
	if itemLocMutex {
		itemLocGL.RLock()
		defer itemLocGL.RUnlock()
	}
	return iloc.loc
}

func (iloc *itemLoc) setLoc(n *ploc) {
	if itemLocMutex {
		itemLocGL.Lock()
		defer itemLocGL.Unlock()
	}
	iloc.loc = n
}

// Item returns an item from its location
func (iloc *itemLoc) Item() *Item {
	if itemLocMutex {
		itemLocGL.RLock()
		defer itemLocGL.RUnlock()
	}
	return iloc.item
}

func (iloc *itemLoc) casItem(o, n *Item) bool {
	if itemLocMutex {
		itemLocGL.Lock()
		defer itemLocGL.Unlock()
	}
	if iloc.item == o {
		iloc.item = n
		return true
	}
	return false
}

// Copy returns a copy of the items location
func (iloc *itemLoc) Copy(src *itemLoc) {
	if src == nil {
		iloc.Copy(&emptyItemLoc)
		return
	}

	if itemLocMutex {
		itemLocGL.Lock()
		defer itemLocGL.Unlock()
	}
	// NOTE: This trick only works because of the global lock. No reason to lock
	// src independently of i.
	iloc.loc = src.loc
	iloc.item = src.item
}

const itemLocHdrLength int = 4 + keyPSize + 4 + 4

func (iloc *itemLoc) write(c *Collection) (err error) {
	if iloc.Loc().isEmpty() {
		iItem := iloc.Item()
		if iItem == nil {
			return errors.New("itemLoc.write with nil item")
		}
		if c.store.callbacks.BeforeItemWrite != nil {
			iItem, err = c.store.callbacks.BeforeItemWrite(c, iItem)
			if err != nil {
				return err
			}
		}
		offset := atomic.LoadInt64(&c.store.size)
		hlength := itemLocHdrLength + len(iItem.Key)
		vlength := iItem.NumValBytes(c)
		ilength := hlength + vlength

		ds := itemBa{
			length:    uint32(ilength),
			keyLength: keyP(len(iItem.Key)),
			valLength: uint32(vlength),
			priority:  iItem.Priority,
		}
		b := ds.render(hlength)
		var pos int
		pos = priSz
		pos += copy(b[priSz:], iItem.Key)
		if pos != hlength {
			return fmt.Errorf("itemLoc.write() pos: %v didn't match hlength: %v",
				pos, hlength)
		}
		if _, err := c.store.file.WriteAt(b, offset); err != nil {
			return err
		}
		err := c.store.ItemValWrite(c, iItem, c.store.file, offset+int64(pos))
		if err != nil {
			return err
		}
		atomic.StoreInt64(&c.store.size, offset+int64(ilength))
		iloc.setLoc(&ploc{Offset: offset, Length: uint32(ilength)})
	}
	return nil
}

func (iloc *itemLoc) read(c *Collection, withValue bool) (icur *Item, err error) {
	if iloc == nil {
		return nil, nil
	}
	icur = iloc.Item()
	if icur == nil || (icur.Val == nil && withValue) {
		loc := iloc.Loc()
		if loc.isEmpty() {
			return nil, nil
		}
		if loc.Length < uint32(itemLocHdrLength) {
			return nil, fmt.Errorf("unexpected item loc.Length: %v < %v",
				loc.Length, itemLocHdrLength)
		}
		b := make([]byte, itemLocHdrLength)
		if _, err := c.store.file.ReadAt(b, loc.Offset); err != nil {
			return nil, err
		}
		var ds itemBa
		ds.populate(b)
		keyLength := ds.getKeyLength()

		i := c.store.ItemAlloc(c, keyLength)
		if i == nil {
			return nil, errors.New("ItemAlloc() failed")
		}
		i.Priority = ds.getPriority()

		valLength := ds.getValLength()
		if ds.getLength() != uint32(itemLocHdrLength)+uint32(keyLength)+valLength {
			c.store.ItemDecRef(c, i)
			return nil, errors.New("mismatched itemLoc lengths")
		}
		if priSz != itemLocHdrLength {
			c.store.ItemDecRef(c, i)
			return nil, fmt.Errorf("read pos != itemLoc_hdrLength, %v != %v",
				priSz, itemLocHdrLength)
		}
		if _, err := c.store.file.ReadAt(i.Key,
			loc.Offset+int64(itemLocHdrLength)); err != nil {
			c.store.ItemDecRef(c, i)
			return nil, err
		}
		if withValue {
			err := c.store.ItemValRead(c, i, c.store.file,
				loc.Offset+int64(itemLocHdrLength)+int64(keyLength), valLength)
			if err != nil {
				c.store.ItemDecRef(c, i)
				return nil, err
			}
		}
		if c.store.callbacks.AfterItemRead != nil {
			i, err = c.store.callbacks.AfterItemRead(c, i)
			if err != nil {
				c.store.ItemDecRef(c, i)
				return nil, err
			}
		}
		if !iloc.casItem(icur, i) {
			c.store.ItemDecRef(c, i)
			return iloc.read(c, withValue)
		}
		if icur != nil {
			c.store.ItemDecRef(c, icur)
		}
		icur = i
	}
	return icur, nil
}

// NumBytes return the number of bytes needed for the collection
func (iloc *itemLoc) NumBytes(c *Collection) int {
	loc := iloc.Loc()
	if loc.isEmpty() {
		i := iloc.Item()
		if i == nil {
			return 0
		}
		return i.NumBytes(c)
	}
	return int(loc.Length) - itemLocHdrLength
}
