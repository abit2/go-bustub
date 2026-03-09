package table

import (
	"goostub/common"
)

// TableIterator iterates over tuples in a TableHeap.
type TableIterator struct {
	table *TableHeap
	// TODO: add cursor state (current page, slot, etc.)
}

// GetTuple returns the tuple metadata and tuple at the current position.
// Returns (zero meta, nil) when exhausted or error.
func (it *TableIterator) GetTuple() (TupleMeta, *Tuple) {
	_ = it.table
	// TODO: implement
	return TupleMeta{}, nil
}

// Valid returns whether the iterator is positioned at a valid tuple.
func (it *TableIterator) Valid() bool {
	// TODO: implement
	return false
}

// Next advances the iterator to the next tuple.
func (it *TableIterator) Next() {
	// TODO: implement
}

// RID returns the RID of the current tuple, or InvalidPageID/0 when invalid.
func (it *TableIterator) RID() common.RID {
	// TODO: implement
	return common.DefaultRID()
}
