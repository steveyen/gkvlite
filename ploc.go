package gkvlite

import (
	"encoding/binary"
)

// Offset/location of a persisted range of bytes.
type ploc struct {
	Offset int64  `json:"o"` // Usable for os.ReadAt/WriteAt() at file offset 0.
	Length uint32 `json:"l"` // Number of bytes.
}

const ploc_length int = 8 + 4

var ploc_empty *ploc = &ploc{} // Sentinel.

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
