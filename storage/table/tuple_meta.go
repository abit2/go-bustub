package table

import (
	"encoding/binary"
)

// InvalidTS is the invalid timestamp value (BusTub INVALID_TS).
const InvalidTS int64 = -1

// TupleMetaSize is the size of TupleMeta in bytes (BusTub TUPLE_META_SIZE).
const TupleMetaSize = 16

// TupleMeta holds visibility and deletion metadata for a tuple (BusTub TupleMeta).
type TupleMeta struct {
	TS        int64
	IsDeleted bool
	// padding to 16 bytes
	_ [7]byte
}

// Size returns the size of TupleMeta in bytes.
func (TupleMeta) Size() int {
	return TupleMetaSize
}

// SerializeTo writes meta into the buffer (little-endian).
func (m *TupleMeta) SerializeTo(buf []byte) {
	if len(buf) < TupleMetaSize {
		return
	}
	binary.LittleEndian.PutUint64(buf[0:8], uint64(m.TS))
	if m.IsDeleted {
		buf[8] = 1
	} else {
		buf[8] = 0
	}
}

// DeserializeFrom reads meta from the buffer.
func (m *TupleMeta) DeserializeFrom(buf []byte) {
	if len(buf) < TupleMetaSize {
		return
	}
	m.TS = int64(binary.LittleEndian.Uint64(buf[0:8]))
	m.IsDeleted = buf[8] != 0
}
