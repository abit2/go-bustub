// Copyright (c) 2021-2022 Qitian Zeng
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package htable

import (
	"encoding/binary"
	"goostub/common"
	"goostub/storage/page"
	"unsafe"
)

// HTableBucketPageMetadataSize is the size of bucket page metadata (BusTub HTABLE_BUCKET_PAGE_METADATA_SIZE).
const HTableBucketPageMetadataSize = 8

/**
 * Bucket page format (BusTub):
 *  ----------------------------------------------------------------------------
 * | METADATA | KEY(1) + VALUE(1) | KEY(2) + VALUE(2) | ... | KEY(n) + VALUE(n)
 *  ----------------------------------------------------------------------------
 *
 * Metadata: CurrentSize (4), MaxSize (4)
 */

// HashTableBucketPage is a view over raw page data for a hash table bucket.
type HashTableBucketPage struct {
	data    []byte
	keySize uint32
}

// HTableBucketArraySize returns the max number of entries that fit in a page.
func HTableBucketArraySize(mappingTypeSize uint64) uint64 {
	return (uint64(common.PageSize) - HTableBucketPageMetadataSize) / mappingTypeSize
}

// PageAsBucketPage returns a bucket page view over the given page data.
func PageAsBucketPage(p page.Page, keySize uint32) *HashTableBucketPage {
	d := p.GetData()
	return &HashTableBucketPage{data: d, keySize: keySize}
}

// NewBucketPageView creates a view over raw data (e.g. from a page guard).
func NewBucketPageView(data []byte, keySize uint32) *HashTableBucketPage {
	return &HashTableBucketPage{data: data, keySize: keySize}
}

func (p *HashTableBucketPage) kvSize() uint32 {
	return p.keySize + uint32(unsafe.Sizeof(common.RID{}))
}

func (p *HashTableBucketPage) maxSlots() uint32 {
	return uint32((int(common.PageSize) - HTableBucketPageMetadataSize) / int(p.kvSize()))
}

func (p *HashTableBucketPage) slotOffset(idx uint32) int {
	return HTableBucketPageMetadataSize + int(idx)*int(p.kvSize())
}

// Init initializes the bucket with the given max size (or computed from page/keySize).
func (p *HashTableBucketPage) Init(maxSize uint32) {
	if len(p.data) < HTableBucketPageMetadataSize {
		return
	}
	if maxSize == 0 {
		maxSize = p.maxSlots()
	}
	binary.LittleEndian.PutUint32(p.data[0:], 0)
	binary.LittleEndian.PutUint32(p.data[4:], maxSize)
}

// GetValue (Lookup) scans the bucket and appends all RIDs with the matching key to result.
func (p *HashTableBucketPage) GetValue(key []byte, result *[]common.RID) bool {
	if result == nil {
		return false
	}
	size := binary.LittleEndian.Uint32(p.data[0:])
	for i := uint32(0); i < size; i++ {
		off := p.slotOffset(i)
		if off+int(p.kvSize()) > len(p.data) {
			break
		}
		slotKey := p.data[off : off+int(p.keySize)]
		if len(slotKey) == len(key) && bytesEqual(slotKey, key) {
			var rid common.RID
			ridBytes := p.data[off+int(p.keySize) : off+int(p.kvSize())]
			rid = ridFromBytes(ridBytes)
			*result = append(*result, rid)
		}
	}
	return len(*result) > 0
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func ridFromBytes(b []byte) common.RID {
	if len(b) < 8 {
		return common.DefaultRID()
	}
	pageID := common.PageID(binary.LittleEndian.Uint32(b[0:4]))
	slotNum := binary.LittleEndian.Uint32(b[4:8])
	return common.NewRID(pageID, slotNum)
}

func ridToBytes(r common.RID, b []byte) {
	if len(b) < 8 {
		return
	}
	binary.LittleEndian.PutUint32(b[0:4], uint32(r.GetPageId()))
	binary.LittleEndian.PutUint32(b[4:8], r.GetSlotNum())
}

// Insert inserts a key-value pair; returns false if duplicate or full.
func (p *HashTableBucketPage) Insert(key []byte, value common.RID) bool {
	size := binary.LittleEndian.Uint32(p.data[0:])
	maxSize := binary.LittleEndian.Uint32(p.data[4:])
	if size >= maxSize {
		return false
	}
	// Check duplicate
	for i := uint32(0); i < size; i++ {
		off := p.slotOffset(i)
		slotKey := p.data[off : off+int(p.keySize)]
		if bytesEqual(slotKey, key) {
			ridBytes := p.data[off+int(p.keySize) : off+int(p.kvSize())]
			existing := ridFromBytes(ridBytes)
			if existing.GetPageId() == value.GetPageId() && existing.GetSlotNum() == value.GetSlotNum() {
				return false
			}
		}
	}
	off := p.slotOffset(size)
	if off+int(p.kvSize()) > len(p.data) {
		return false
	}
	copy(p.data[off:off+int(p.keySize)], key)
	ridToBytes(value, p.data[off+int(p.keySize):off+int(p.kvSize())])
	binary.LittleEndian.PutUint32(p.data[0:], size+1)
	return true
}

// Remove removes one (key, value) pair; returns true if found and removed.
func (p *HashTableBucketPage) Remove(key []byte, value common.RID) bool {
	size := binary.LittleEndian.Uint32(p.data[0:])
	for i := uint32(0); i < size; i++ {
		off := p.slotOffset(i)
		slotKey := p.data[off : off+int(p.keySize)]
		if bytesEqual(slotKey, key) {
			ridBytes := p.data[off+int(p.keySize) : off+int(p.kvSize())]
			existing := ridFromBytes(ridBytes)
			if existing.GetPageId() == value.GetPageId() && existing.GetSlotNum() == value.GetSlotNum() {
				p.RemoveAt(i)
				return true
			}
		}
	}
	return false
}

// RemoveAt removes the entry at bucketIdx (swap with last and decrement size).
func (p *HashTableBucketPage) RemoveAt(bucketIdx uint32) {
	size := binary.LittleEndian.Uint32(p.data[0:])
	if bucketIdx >= size {
		return
	}
	lastIdx := size - 1
	if bucketIdx != lastIdx {
		offIdx := p.slotOffset(bucketIdx)
		offLast := p.slotOffset(lastIdx)
		kvSize := int(p.kvSize())
		for j := 0; j < kvSize; j++ {
			p.data[offIdx+j] = p.data[offLast+j]
		}
	}
	binary.LittleEndian.PutUint32(p.data[0:], lastIdx)
}

// KeyAt returns a copy of the key at bucketIdx (caller must not modify the page through this).
func (p *HashTableBucketPage) KeyAt(bucketIdx uint32) []byte {
	off := p.slotOffset(bucketIdx)
	if off+int(p.keySize) > len(p.data) {
		return nil
	}
	out := make([]byte, p.keySize)
	copy(out, p.data[off:off+int(p.keySize)])
	return out
}

// ValueAt returns the RID at bucketIdx.
func (p *HashTableBucketPage) ValueAt(bucketIdx uint32) common.RID {
	off := p.slotOffset(bucketIdx) + int(p.keySize)
	if off+8 > len(p.data) {
		return common.DefaultRID()
	}
	return ridFromBytes(p.data[off : off+8])
}

// Size returns the number of entries in the bucket.
func (p *HashTableBucketPage) Size() uint32 {
	if len(p.data) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(p.data[0:])
}

// IsFull returns true if the bucket has no free slots.
func (p *HashTableBucketPage) IsFull() bool {
	return p.Size() >= binary.LittleEndian.Uint32(p.data[4:])
}

// IsEmpty returns true if the bucket has no entries.
func (p *HashTableBucketPage) IsEmpty() bool {
	return p.Size() == 0
}
