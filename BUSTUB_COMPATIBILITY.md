# Compatibility: go-bustub vs BusTub (C++)

This document compares **go-bustub** with the reference [CMU-DB BusTub](https://github.com/cmu-db/bustub) C++ codebase (e.g. `../bustub`) so you can follow the course and reuse concepts/assignments where possible.

**Scaffolding has been aligned with BusTub** (constants, APIs, page layouts, and types). Remaining simplifications: no DiskScheduler (BPM uses DiskManager directly), no B+ tree, and lock manager / replacer logic remain stubs.

---

## Summary

| Area | Compatible | Notes |
|------|------------|--------|
| **Concepts & layer order** | ✅ | Same layers: disk → buffer pool → pages → table heap → catalog → indexes → concurrency → recovery |
| **Replacer API** | ✅ | Same interface: `Victim`, `Pin`, `Unpin`, `Size`; **ArcReplacer** added with `Evict`, `RecordAccess`, `SetEvictable`, `Remove`, `Size`. |
| **Replacer policy** | ✅ | Go: **ArcReplacer** (stub) + Clock; BusTub: **ARC**. BPM uses ArcReplacer. |
| **Page / Table page** | ✅ | Table page header: `NextPageId (4)`, `NumTuples (2)`, `NumDeletedTuples (2)`; **TupleMeta** (TS, IsDeleted, 16 bytes). |
| **Extendible hash** | ✅ | Directory: MaxDepth, GlobalDepth, LocalDepths[512], BucketPageIds[512]. Bucket: CurrentSize, MaxSize, then KV array. |
| **Page size** | ✅ | Go: **8192** (aligned with BusTub). |
| **Buffer pool API** | ✅ | `NewPage`, `ReadPage`/`WritePage` (return **ReadPageGuard**/**WritePageGuard**), `DeletePage`, `FlushPage`, `GetPinCount`. |
| **Lock manager** | ✅ | **LockTable**/UnlockTable, **LockRow**/UnlockRow; modes: Shared, Exclusive, IS, IX, SIX. Stub implementation. |
| **Tuple / TableHeap** | ✅ | **TupleMeta**; TableHeap: InsertTuple(meta, tuple, ...), GetTuple, GetTupleMeta, UpdateTupleMeta, MakeIterator. |
| **B+ tree** | ❌ | Go has no B+ tree; BusTub has full B+ tree index. |

---

## 1. Constants & config

| Constant | go-bustub | BusTub |
|----------|-----------|--------|
| Page size | `8192` (`common.PageSize`) | `8192` (`BUSTUB_PAGE_SIZE`) |
| Buffer pool size | `128` | `128` |
| Bucket size (hash) | `50` | `50` |
| Invalid page/txn/LSN | `-1` | `-1` |
| TxnID type | `int64` | `int64` |
| InvalidFrameID, TxnStartID, etc. | present | present |

**Compatibility:** Constants and types are aligned; database files use the same page size.

---

## 2. Buffer pool & replacer

**Replacer interface** — Same in spirit and method names:

- `Victim(frame_id) -> bool`
- `Pin(frame_id)`
- `Unpin(frame_id)`
- `Size() -> count`

**Policy:**

- **go-bustub:** ClockReplacer (Replacer interface) and ArcReplacer (BusTub-style); BPM uses ArcReplacer.
- **BusTub:** ARC with `ArcReplacer`, `AccessType`, `FrameStatus`.

So the *interface* is compatible for implementing “a” replacer; the *algorithm* you implement differs (Clock vs ARC).

**Buffer pool manager:**

- **BusTub:** `NewPage()`, `ReadPage`/`WritePage` (returning `ReadPageGuard`/`WritePageGuard`), `DeletePage`, `FlushPage`.
- **go-bustub:** Same API: `NewBufferPoolManager(numFrames, diskManager, logManager)`, `NewPage()`, `ReadPage`/`WritePage` (return guards), `DeletePage`, `FlushPage`, `FlushAllPages`, `GetPinCount`. Guards have `GetData()`/`GetDataMut()`, `Drop()`, `Flush()`. `UnpinPage` kept for backward compatibility (used internally by guards).

---

## 3. Storage: page & table page

**Page (base):**

- Both: `GetData()`, `GetPageId()`, pin count, dirty flag, LSN, read/write latch. **Compatible conceptually.**

**Table page (slotted page):**

- **BusTub:** Header = `NextPageId (4)`, `NumTuples (2)`, `NumDeletedTuples (2)`; slots store `TupleInfo` (offset, size, `TupleMeta`). `TupleMeta`: `ts_`, `is_deleted_`.
- **go-bustub:** Aligned: `TablePageHeaderSize = 8`; `NextPageId (4)`, `NumTuples (2)`, `NumDeletedTuples (2)`; `TupleInfoSize = 24` (offset, size, `TupleMeta`). **TupleMeta** (TS int64, IsDeleted bool, 16 bytes). `TablePageView` implements the interface over raw page data.

---

## 4. Extendible hash table

**Directory page:**

- **BusTub:** Layout: MaxDepth (4), GlobalDepth (4), LocalDepths (512), BucketPageIds (2048). Methods: `Init`, `HashToBucketIndex`, `GetBucketPageId`, `SetBucketPageId`, `GetSplitImageIndex`, depth getters/setters, `VerifyIntegrity`, `PrintDirectory`.
- **go-bustub:** Aligned: `HashTableDirectoryPage` is a view over raw data (`NewHashTableDirectoryPage(data)`). Same layout and method names; getters/setters read/write at offsets. `HTableDirectoryMaxDepth`, `HTableDirectoryArraySize` constants.

**Bucket page:**

- **BusTub:** Metadata = `CurrentSize (4)`, `MaxSize (4)`; then array of (key, value) pairs.
- **go-bustub:** Aligned: `HashTableBucketPage` view with `HTableBucketPageMetadataSize = 8`; `Init(maxSize)`, `GetValue`/`Insert`/`Remove`/`RemoveAt`, `KeyAt`/`ValueAt`, `Size`/`IsFull`/`IsEmpty`. `PageAsBucketPage` and `NewBucketPageView` construct from page data.

---

## 5. Disk manager

Both provide:

- `ReadPage(page_id, page_data)`
- `WritePage(page_id, page_data)`
- `WriteLog` / `ReadLog`
- Allocation of new pages (BusTub: internal `AllocatePage()`; Go: `AllocatePage()` on `DiskManager`)

Both support page reuse: BusTub has `DeletePage`; Go has `DeallocatePage(pageID)` (appends to a free list). `AllocatePage` returns a reused page ID when the free list is non-empty. Go also has `GetNumDeletes()`.

---

## 6. Catalog

Both have:

- **TableInfo:** schema, name, table heap, table OID.
- **IndexInfo:** key schema, name, index, index OID, table name, key size.

Go’s **IndexInfo** now includes `IsPrimaryKey bool` and `IndexType` (BPlusTreeIndex, HashTableIndex, STLOrderedIndex, STLUnorderedIndex). `CreateIndex` signature includes `isPrimaryKey` and `indexType`. B+ tree index type exists in the enum but is not implemented.

---

## 7. Concurrency

**Lock manager:**

- **BusTub:** Table and row locks; modes: Shared, Exclusive, Intention Shared/Exclusive, SIX; proper 2PL, wait queues, deadlock handling.
- **go-bustub:** Aligned API: **LockTable**(txn, tableOid, mode), **UnlockTable**(txn, tableOid), **LockRow**(txn, tableOid, rid, mode), **UnlockRow**(txn, tableOid, rid). Lock modes: Shared, Exclusive, IntentionShared, IntentionExclusive, SharedIntentionExclusive. Transaction has **GetTableLockSet()** map[TableOID]uint8. Implementation is a stub (grants immediately).

**Transaction / TransactionManager:**

- Both: Begin, Commit, Abort; write set for table (and index) updates; release locks on commit/abort.
- BusTub: richer isolation and tuple visibility (TupleMeta, timestamps).
- Go: ApplyDelete / RollbackDelete on table; index rollback is stubbed.

Compatible enough to implement “transaction lifecycle and write set” in Go while reading BusTub material.

---

## 8. Recovery

- **BusTub:** `LogManager`, log records, checkpoint manager (can be disabled).
- **go-bustub:** `LogManager` is an empty struct.

Compatible at the “there is a log manager” level; implementation in Go is left to you.

---

## 9. What BusTub has that go-bustub does not

- **B+ tree index** (and related pages: header, internal, leaf).
- **Planner / optimizer** (expression, plan nodes, rules).
- **Executor** (full set of plan nodes and execution engine).
- **Parser / SQL shell** (so no `main`/shell in Go yet).
- **DiskScheduler** (Go BPM uses DiskManager directly).
- **Full ARC / 2PL implementation** (Go has scaffolding and stubs only).

---

## 10. Recommendations

1. **Use BusTub as the reference for concepts and order of implementation** (buffer pool → table heap → indexes → concurrency → recovery). The Go clone matches that layering and scaffolding.
2. **Constants and APIs are aligned**; you can follow BusTub writeups and map them directly to Go (same page size, guard-based BPM, TupleMeta, directory/bucket layouts, lock modes).
3. **Implement the stubs** (ArcReplacer eviction, BPM eviction logic, TableHeap/TablePage insert/get, extendible hash split/merge, lock manager 2PL) using the BusTub logic; the scaffolding is in place.
4. **Replacer:** Both Clock (legacy) and ArcReplacer exist; BPM uses ArcReplacer. Implement ArcReplacer for full parity.

---

*Scaffolding aligned with BusTub (C++) in `../bustub`. Last updated after scaffolding match.*
