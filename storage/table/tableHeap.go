package table

import (
	"goostub/buffer"
	"goostub/common"
	"goostub/concurrency"
	"goostub/recovery"
	"sync"
)

// TableHeap represents a physical table on disk (doubly-linked list of pages).
type TableHeap struct {
	bpm         *buffer.BufferPoolManager
	firstPageID common.PageID
	lastPageID  common.PageID
	latch       sync.Mutex
}

// NewTableHeap creates a table heap (create table).
func NewTableHeap(bpm *buffer.BufferPoolManager, lockM *concurrency.LockManager, logM *recovery.LogManager, txn common.Transaction) *TableHeap {
	_ = lockM
	_ = logM
	_ = txn
	return &TableHeap{
		bpm:         bpm,
		firstPageID: common.InvalidPageID,
		lastPageID:  common.InvalidPageID,
	}
}

// GetFirstPageId returns the id of the first page of this table.
func (t *TableHeap) GetFirstPageId() common.PageID {
	return t.firstPageID
}

// InsertTuple inserts a tuple with the given metadata; returns RID and true on success.
func (t *TableHeap) InsertTuple(meta TupleMeta, tuple *Tuple, lockMgr *concurrency.LockManager, txn common.Transaction, tableOid common.TableOID) (common.RID, bool) {
	_ = meta
	_ = tuple
	_ = lockMgr
	_ = txn
	_ = tableOid
	// TODO: implement
	return common.DefaultRID(), false
}

// GetTuple returns the tuple metadata and tuple at the given RID.
func (t *TableHeap) GetTuple(rid common.RID) (TupleMeta, *Tuple) {
	_ = rid
	// TODO: implement
	return TupleMeta{}, nil
}

// GetTupleMeta returns the tuple metadata at the given RID.
func (t *TableHeap) GetTupleMeta(rid common.RID) TupleMeta {
	_ = rid
	// TODO: implement
	return TupleMeta{}
}

// UpdateTupleMeta updates the tuple metadata at the given RID.
func (t *TableHeap) UpdateTupleMeta(meta TupleMeta, rid common.RID) {
	_ = meta
	_ = rid
	// TODO: implement
}

// UpdateTupleInPlace updates the tuple in place; check is an optional predicate.
func (t *TableHeap) UpdateTupleInPlace(meta TupleMeta, tuple *Tuple, rid common.RID, check func(TupleMeta, *Tuple, common.RID) bool) bool {
	_ = meta
	_ = tuple
	_ = rid
	_ = check
	// TODO: implement
	return false
}

// MakeIterator returns an iterator over the table.
func (t *TableHeap) MakeIterator() *TableIterator {
	return &TableIterator{table: t}
}

// MakeEagerIterator returns an eager iterator (loads all pages up front if needed).
func (t *TableHeap) MakeEagerIterator() *TableIterator {
	return &TableIterator{table: t}
}

// ApplyDelete marks the tuple at rid as deleted (used on commit).
func (t *TableHeap) ApplyDelete(rid common.RID, txn common.Transaction) {
	_ = rid
	_ = txn
	// TODO: implement
}

// RollbackDelete unmarks the tuple at rid as deleted (used on abort).
func (t *TableHeap) RollbackDelete(rid common.RID, txn common.Transaction) {
	_ = rid
	_ = txn
	// TODO: implement
}

// UpdateTuple updates the tuple at rid (used on abort for update rollback).
func (t *TableHeap) UpdateTuple(tup *Tuple, rid common.RID, txn common.Transaction) {
	_ = tup
	_ = rid
	_ = txn
	// TODO: implement
}
