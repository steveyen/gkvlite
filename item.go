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

// Offset/location of a persisted range of bytes.
type ploc struct {
	Offset int64  `json:"o"` // Usable for os.ReadAt/WriteAt() at file offset 0.
	Length uint32 `json:"l"` // Number of bytes.
}

const ploc_length int = 8 + 4

var ploc_empty *ploc = &ploc{} // Sentinel.

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
		hlength := 4 + 2 + 4 + 4 + len(iItem.Key)
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
		if c.store.callbacks.ItemValWrite != nil {
			err := c.store.callbacks.ItemValWrite(c, iItem, c.store.file,
				offset+int64(pos))
			if err != nil {
				return err
			}
		} else {
			_, err := c.store.file.WriteAt(iItem.Val, offset+int64(pos))
			if err != nil {
				return err
			}
		}
		atomic.StoreInt64(&c.store.size, offset+int64(ilength))
		atomic.StorePointer(&i.loc,
			unsafe.Pointer(&ploc{Offset: offset, Length: uint32(ilength)}))
	}
	return nil
}

func (iloc *itemLoc) read(c *Collection, withValue bool) (i *Item, err error) {
	if iloc == nil {
		return nil, nil
	}
	i = iloc.Item()
	if i == nil || (i.Val == nil && withValue) {
		loc := iloc.Loc()
		if loc.isEmpty() {
			return nil, nil
		}
		hdrLength := 4 + 2 + 4 + 4
		if loc.Length < uint32(hdrLength) {
			return nil, fmt.Errorf("unexpected item loc.Length: %v < %v",
				loc.Length, hdrLength)
		}
		b := make([]byte, hdrLength)
		if _, err := c.store.file.ReadAt(b, loc.Offset); err != nil {
			return nil, err
		}
		i = &Item{}
		pos := 0
		length := binary.BigEndian.Uint32(b[pos : pos+4])
		pos += 4
		keyLength := binary.BigEndian.Uint16(b[pos : pos+2])
		pos += 2
		valLength := binary.BigEndian.Uint32(b[pos : pos+4])
		pos += 4
		i.Priority = int32(binary.BigEndian.Uint32(b[pos : pos+4]))
		pos += 4
		if length != uint32(hdrLength)+uint32(keyLength)+valLength {
			return nil, errors.New("mismatched itemLoc lengths")
		}
		if pos != hdrLength {
			return nil, fmt.Errorf("read pos != hdrLength, %v != %v", pos, hdrLength)
		}
		i.Key = make([]byte, keyLength)
		if _, err := c.store.file.ReadAt(i.Key,
			loc.Offset+int64(hdrLength)); err != nil {
			return nil, err
		}
		if withValue {
			if c.store.callbacks.ItemValRead != nil {
				err := c.store.callbacks.ItemValRead(c, i, c.store.file,
					loc.Offset+int64(hdrLength)+int64(keyLength), valLength)
				if err != nil {
					return nil, err
				}
			} else {
				i.Val = make([]byte, valLength)
				if _, err := c.store.file.ReadAt(i.Val,
					loc.Offset+int64(hdrLength)+int64(keyLength)); err != nil {
					return nil, err
				}
			}
		}
		if c.store.callbacks.AfterItemRead != nil {
			i, err = c.store.callbacks.AfterItemRead(c, i)
			if err != nil {
				return nil, err
			}
		}
		atomic.StorePointer(&iloc.item, unsafe.Pointer(i))
	}
	return i, nil
}

func (p *ploc) isEmpty() bool {
	return p == nil || (p.Offset == int64(0) && p.Length == uint32(0))
}

func (p *ploc) write(b []byte, pos int) int {
	if p == nil {
		return ploc_empty.write(b, pos)
	}
	binary.BigEndian.PutUint64(b[pos:pos+8], uint64(p.Offset))
	pos += 8
	binary.BigEndian.PutUint32(b[pos:pos+4], p.Length)
	pos += 4
	return pos
}

func (p *ploc) read(b []byte, pos int) (*ploc, int) {
	p.Offset = int64(binary.BigEndian.Uint64(b[pos : pos+8]))
	pos += 8
	p.Length = binary.BigEndian.Uint32(b[pos : pos+4])
	pos += 4
	if p.isEmpty() {
		return nil, pos
	}
	return p, pos
}
