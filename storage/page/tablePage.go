// Copyright (c) 2021 Qitian Zeng
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package page

import (
	"encoding/binary"
	"goostub/common"
	"goostub/storage/table"
)

// TablePageHeaderSize is the size of the table page header in bytes (BusTub TABLE_PAGE_HEADER_SIZE).
const TablePageHeaderSize = 8

// TupleInfoSize is the size of one slot in the tuple info array (BusTub TUPLE_INFO_SIZE).
const TupleInfoSize = 24

// Offsets into the 8-byte header.
const (
	offsetNextPageID      = 0
	offsetNumTuples       = 4
	offsetNumDeletedTuples = 6
)

// TupleInfo starts at offset 8; each entry is 24 bytes: offset(2), size(2), TupleMeta(16).
const offsetTupleInfo = 8

/**
 * Slotted page format (BusTub layout):
 *  ---------------------------------------------------------
 *  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 *  ---------------------------------------------------------
 *
 *  Header format (size in bytes):
 *  ----------------------------------------------------------------------------
 *  | NextPageId (4)| NumTuples(2) | NumDeletedTuples(2) |
 *  ----------------------------------------------------------------------------
 *  ----------------------------------------------------------------
 *  | Tuple_1 offset+size+meta (24) | Tuple_2 (24) | ... |
 *  ----------------------------------------------------------------
 *
 * Tuple format: | meta | data |
 */

// TablePage is the interface for a slotted table page (BusTub TablePage).
type TablePage interface {
	Init()
	GetNumTuples() uint32
	GetNextPageId() common.PageID
	SetNextPageId(common.PageID)
	GetNextTupleOffset(meta *table.TupleMeta, tuple *table.Tuple) (uint16, bool)
	InsertTuple(meta *table.TupleMeta, tuple *table.Tuple) (uint16, bool)
	UpdateTupleMeta(meta *table.TupleMeta, rid common.RID)
	GetTuple(rid common.RID) (table.TupleMeta, *table.Tuple)
	GetTupleMeta(rid common.RID) table.TupleMeta
	UpdateTupleInPlaceUnsafe(meta *table.TupleMeta, tuple *table.Tuple, rid common.RID)
}

// TablePageView is a view over raw page data implementing TablePage.
type TablePageView struct {
	Data []byte
}

// NewTablePageView creates a view over the given page data (must be at least TablePageHeaderSize bytes).
func NewTablePageView(data []byte) *TablePageView {
	return &TablePageView{Data: data}
}

// Init zeroes the header and sets next_page_id to invalid.
func (p *TablePageView) Init() {
	if len(p.Data) < TablePageHeaderSize {
		return
	}
	invalidPID := common.PageID(common.InvalidPageID)
	binary.LittleEndian.PutUint32(p.Data[offsetNextPageID:], uint32(invalidPID))
	binary.LittleEndian.PutUint16(p.Data[offsetNumTuples:], 0)
	binary.LittleEndian.PutUint16(p.Data[offsetNumDeletedTuples:], 0)
}

// GetNumTuples returns the number of tuples in this page.
func (p *TablePageView) GetNumTuples() uint32 {
	if len(p.Data) < offsetNumTuples+2 {
		return 0
	}
	return uint32(binary.LittleEndian.Uint16(p.Data[offsetNumTuples:]))
}

// GetNextPageId returns the page ID of the next table page.
func (p *TablePageView) GetNextPageId() common.PageID {
	if len(p.Data) < offsetNextPageID+4 {
		return common.InvalidPageID
	}
	return common.PageID(binary.LittleEndian.Uint32(p.Data[offsetNextPageID:]))
}

// SetNextPageId sets the next page ID.
func (p *TablePageView) SetNextPageId(next common.PageID) {
	if len(p.Data) >= offsetNextPageID+4 {
		binary.LittleEndian.PutUint32(p.Data[offsetNextPageID:], uint32(next))
	}
}

// GetNextTupleOffset returns the next free offset for the given tuple, or (0, false).
func (p *TablePageView) GetNextTupleOffset(meta *table.TupleMeta, tuple *table.Tuple) (uint16, bool) {
	_ = meta
	_ = tuple
	// TODO: implement
	return 0, false
}

// InsertTuple inserts a tuple and returns its slot number, or (0, false).
func (p *TablePageView) InsertTuple(meta *table.TupleMeta, tuple *table.Tuple) (uint16, bool) {
	_ = meta
	_ = tuple
	// TODO: implement
	return 0, false
}

// UpdateTupleMeta updates the tuple metadata at the given RID.
func (p *TablePageView) UpdateTupleMeta(meta *table.TupleMeta, rid common.RID) {
	if len(p.Data) < offsetTupleInfo+int(rid.GetSlotNum()+1)*TupleInfoSize {
		return
	}
	slotOff := offsetTupleInfo + int(rid.GetSlotNum())*TupleInfoSize
	meta.SerializeTo(p.Data[slotOff+4 : slotOff+4+table.TupleMetaSize])
}

// GetTuple returns the tuple metadata and tuple at the given RID.
func (p *TablePageView) GetTuple(rid common.RID) (table.TupleMeta, *table.Tuple) {
	var meta table.TupleMeta
	if len(p.Data) < offsetTupleInfo+int(rid.GetSlotNum()+1)*TupleInfoSize {
		return meta, nil
	}
	slotOff := offsetTupleInfo + int(rid.GetSlotNum())*TupleInfoSize
	meta.DeserializeFrom(p.Data[slotOff+4 : slotOff+4+table.TupleMetaSize])
	off := binary.LittleEndian.Uint16(p.Data[slotOff:])
	size := binary.LittleEndian.Uint16(p.Data[slotOff+2:])
	o, sz := int(off), int(size)
	if o+sz > len(p.Data) {
		return meta, nil
	}
	return meta, table.NewTupleFromData(rid, p.Data[o:o+sz])
}

// GetTupleMeta returns the tuple metadata at the given RID.
func (p *TablePageView) GetTupleMeta(rid common.RID) table.TupleMeta {
	var meta table.TupleMeta
	if len(p.Data) < offsetTupleInfo+int(rid.GetSlotNum()+1)*TupleInfoSize {
		return meta
	}
	slotOff := offsetTupleInfo + int(rid.GetSlotNum())*TupleInfoSize
	meta.DeserializeFrom(p.Data[slotOff+4 : slotOff+4+table.TupleMetaSize])
	return meta
}

// UpdateTupleInPlaceUnsafe updates the tuple in place at the given RID.
func (p *TablePageView) UpdateTupleInPlaceUnsafe(meta *table.TupleMeta, tuple *table.Tuple, rid common.RID) {
	_ = meta
	_ = tuple
	_ = rid
	// TODO: implement
}
