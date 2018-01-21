package gkvlite

import "encoding/binary"

const (
	lenLoc = 0
	keyLoc = lenLoc + 4
	valLoc = keyLoc + keyPSize
	priLoc = valLoc + 4
	priSz  = priLoc + 4
)
// itemBa structure allows you to populate to and from a byte array
// This ba can then be written to the disk
type itemBa struct {
	length    uint32
	keyLength keyP
	valLength uint32
	priority  int32
}
// populate the structure from a byte array
func (ds *itemBa) populate(b []byte) {
	ds.length = binary.BigEndian.Uint32(b[lenLoc : keyLoc])
	if keyPSize == 2 {
		ds.keyLength = keyP(binary.BigEndian.Uint16(b[keyLoc : valLoc]))
	} else {
		ds.keyLength = keyP(binary.BigEndian.Uint32(b[keyLoc : valLoc]))
	}

	ds.valLength = binary.BigEndian.Uint32(b[valLoc : priLoc])
	ds.priority = int32(binary.BigEndian.Uint32(b[priLoc : priSz]))
}
func (ds itemBa) getLength() uint32 {
	return ds.length
}
func (ds itemBa) getKeyLength() keyP {
	return ds.keyLength
}
func (ds itemBa) getValLength() uint32 {
	return ds.valLength
}
func (ds itemBa) getPriority() int32 {
	return ds.priority
}
// Render the structure to a byte array
func (ds itemBa) render(hlength int) []byte {
	b := make([]byte, hlength)
	binary.BigEndian.PutUint32(b[lenLoc:keyLoc], uint32(ds.length))
	if keyPSize == 2 {
		binary.BigEndian.PutUint16(b[keyLoc:valLoc], uint16(ds.keyLength))
	} else {
		binary.BigEndian.PutUint32(b[keyLoc:valLoc], uint32(ds.keyLength))
	}
	binary.BigEndian.PutUint32(b[valLoc:priLoc], uint32(ds.valLength))
	binary.BigEndian.PutUint32(b[priLoc:priSz], uint32(ds.priority))

	return b
}
