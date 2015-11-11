package gkvlite

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// A persistable item.
type Item struct {
	Transient interface{} // For any ephemeral data.
	Key, Val  []byte      // Val may be nil if not fetched into memory yet.
	Priority  int32       // Use rand.Int31() for probabilistic balancing.
}

var itemLocGL = sync.RWMutex{}

// A persistable item and its persistence location.
type itemLoc struct {
	loc  *ploc // can be nil if item is dirty (not yet persisted).
	item *Item // can be nil if item is not fetched into memory yet.
}

var empty_itemLoc = &itemLoc{}

// Number of Key bytes plus number of Val bytes.
func (i *Item) NumBytes(c *Collection) int {
	return len(i.Key) + i.NumValBytes(c)
}

func (i *Item) NumValBytes(c *Collection) int {
	if c.store.callbacks.ItemValLength != nil {
		return c.store.callbacks.ItemValLength(c, i)
	}
	return len(i.Val)
}

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

func (i *itemLoc) Loc() *ploc {
	itemLocGL.RLock()
	defer itemLocGL.RUnlock()
	return i.loc
}

func (i *itemLoc) setLoc(n *ploc) {
	itemLocGL.Lock()
	defer itemLocGL.Unlock()
	i.loc = n
}

func (i *itemLoc) Item() *Item {
	itemLocGL.RLock()
	defer itemLocGL.RUnlock()
	return i.item
}

func (i *itemLoc) casItem(o, n *Item) bool {
	itemLocGL.Lock()
	defer itemLocGL.Unlock()
	if i.item == o {
		i.item = n
		return true
	}
	return false
}

func (i *itemLoc) Copy(src *itemLoc) {
	if src == nil {
		i.Copy(empty_itemLoc)
		return
	}

	itemLocGL.Lock()
	defer itemLocGL.Unlock()
	// NOTE: This trick only works because of the global lock. No reason to lock
	// src independently of i.
	i.loc = src.loc
	i.item = src.item
}

const itemLoc_hdrLength int = 4 + 4 + 4 + 4

func (i *itemLoc) write(c *Collection) (err error) {
	if i.Loc().isEmpty() {
		iItem := i.Item()
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
		hlength := itemLoc_hdrLength + len(iItem.Key)
		vlength := iItem.NumValBytes(c)
		ilength := hlength + vlength
		b := make([]byte, hlength)
		pos := 0
		binary.BigEndian.PutUint32(b[pos:pos+4], uint32(ilength))
		pos += 4
		binary.BigEndian.PutUint32(b[pos:pos+4], uint32(len(iItem.Key)))
		pos += 4
		binary.BigEndian.PutUint32(b[pos:pos+4], uint32(vlength))
		pos += 4
		binary.BigEndian.PutUint32(b[pos:pos+4], uint32(iItem.Priority))
		pos += 4
		pos += copy(b[pos:], iItem.Key)
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
		i.setLoc(&ploc{Offset: offset, Length: uint32(ilength)})
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
		if loc.Length < uint32(itemLoc_hdrLength) {
			return nil, fmt.Errorf("unexpected item loc.Length: %v < %v",
				loc.Length, itemLoc_hdrLength)
		}
		b := make([]byte, itemLoc_hdrLength)
		if _, err := c.store.file.ReadAt(b, loc.Offset); err != nil {
			return nil, err
		}
		pos := 0
		length := binary.BigEndian.Uint32(b[pos : pos+4])
		pos += 4
		keyLength := binary.BigEndian.Uint32(b[pos : pos+4])
		pos += 4
		valLength := binary.BigEndian.Uint32(b[pos : pos+4])
		pos += 4
		i := c.store.ItemAlloc(c, keyLength)
		if i == nil {
			return nil, errors.New("ItemAlloc() failed")
		}
		i.Priority = int32(binary.BigEndian.Uint32(b[pos : pos+4]))
		pos += 4
		if length != uint32(itemLoc_hdrLength)+uint32(keyLength)+valLength {
			c.store.ItemDecRef(c, i)
			return nil, errors.New("mismatched itemLoc lengths")
		}
		if pos != itemLoc_hdrLength {
			c.store.ItemDecRef(c, i)
			return nil, fmt.Errorf("read pos != itemLoc_hdrLength, %v != %v",
				pos, itemLoc_hdrLength)
		}
		if _, err := c.store.file.ReadAt(i.Key,
			loc.Offset+int64(itemLoc_hdrLength)); err != nil {
			c.store.ItemDecRef(c, i)
			return nil, err
		}
		if withValue {
			err := c.store.ItemValRead(c, i, c.store.file,
				loc.Offset+int64(itemLoc_hdrLength)+int64(keyLength), valLength)
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

func (iloc *itemLoc) NumBytes(c *Collection) int {
	loc := iloc.Loc()
	if loc.isEmpty() {
		i := iloc.Item()
		if i == nil {
			return 0
		}
		return i.NumBytes(c)
	}
	return int(loc.Length) - itemLoc_hdrLength
}
