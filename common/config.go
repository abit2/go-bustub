package common

import (
	"time"
)

var CycleDetectionInterval time.Duration
var EnableLogging bool
var LogTimeout time.Duration

const (
	// invalid frame id
	InvalidFrameID = -1
	// invalid page id
	InvalidPageID = -1
	// invalid transaction id
	InvalidTxnID = -1
	// invalid log sequence number
	InvalidLSN = -1
	// the header page id
	HeaderPageID = 0
	// size of a data page in byte (BusTub BUSTUB_PAGE_SIZE)
	PageSize = 8192
	// size of buffer pool (BusTub BUFFER_POOL_SIZE)
	BufferPoolSize = 128
	// size of a log buffer in byte
	LogBufferSize = (BufferPoolSize + 1) * PageSize
	// size of extendible hash bucket
	BucketSize = 50
	// default size of file on disk
	DefaultDBIOSize = 16
	// backward k-distance for LRU-K replacer
	LRUKReplacerK = 10
	// default length for varchar when constructing the column
	VarcharDefaultLength = 128
)

// TxnStartID is the first txn id (BusTub TXN_START_ID)
const TxnStartID int64 = 1 << 62

type FrameID int32      // frame id type
type PageID int32       // page id type
type TxnID int64        // transaction id type (BusTub txn_id_t)
type LSN int32          // log sequence number
type SlotOffset uintptr // slot offset type
type OID uint16
type TableOID uint32
type IndexOID uint32
type ColumnOID uint32
