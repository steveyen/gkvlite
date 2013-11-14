package gkvlite

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"
)

// A persistable item.
type Item struct {
	Transient unsafe.Pointer // For any ephemeral data; atomic CAS recommended.
	Key, Val  []byte         // Val may be nil if not fetched into memory yet.
	Priority  int32          // Use rand.Int31() for probabilistic balancing.
}

// A persistable item and its persistence location.
type itemLoc struct {
	loc  unsafe.Pointer // *ploc - can be nil if item is dirty (not yet persisted).
	item unsafe.Pointer // *Item - can be nil if item is not fetched into memory yet.
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
	return (*ploc)(atomic.LoadPointer(&i.loc))
}

func (i *itemLoc) Item() *Item {
	return (*Item)(atomic.LoadPointer(&i.item))
}

func (i *itemLoc) Copy(src *itemLoc) {
	if src == nil {
		i.Copy(empty_itemLoc)
		return
	}
	atomic.StorePointer(&i.loc, unsafe.Pointer(src.Loc()))
	atomic.StorePointer(&i.item, unsafe.Pointer(src.Item()))
}

const itemLoc_hdrLength int = 4 + 2 + 4 + 4

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
		binary.BigEndian.PutUint16(b[pos:pos+2], uint16(len(iItem.Key)))
		pos += 2
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
		atomic.StorePointer(&i.loc,
			unsafe.Pointer(&ploc{Offset: offset, Length: uint32(ilength)}))
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
		keyLength := binary.BigEndian.Uint16(b[pos : pos+2])
		pos += 2
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
		if !atomic.CompareAndSwapPointer(&iloc.item,
			unsafe.Pointer(icur), unsafe.Pointer(i)) {
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
