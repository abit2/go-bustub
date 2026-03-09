package htable

import (
	"encoding/binary"
	"fmt"
	"goostub/common"
	"os"

	"github.com/go-kit/log/level"
)

const (
	directoryArraySize = 512
	// HTableDirectoryMaxDepth is the max global depth (BusTub HTABLE_DIRECTORY_MAX_DEPTH).
	HTableDirectoryMaxDepth = 9
	// HTableDirectoryArraySize is the number of directory entries (1 << MaxDepth).
	HTableDirectoryArraySize = 1 << HTableDirectoryMaxDepth
)

// Directory layout in page bytes (BusTub): MaxDepth(4), GlobalDepth(4), LocalDepths(512), BucketPageIds(2048).
const (
	dirOffsetMaxDepth      = 0
	dirOffsetGlobalDepth   = 4
	dirOffsetLocalDepths   = 8
	DirOffsetBucketPageIds = 8 + 512
)

/**
 * Directory Page for extendible hash table.
 *
 * Directory format (size in byte):
 * --------------------------------------------------------------------------------------
 * | MaxDepth (4) | GlobalDepth (4) | LocalDepths (512) | BucketPageIds(2048) | Free(1528)
 * --------------------------------------------------------------------------------------
 */

// HashTableDirectoryPage is a view over raw page data for the directory.
type HashTableDirectoryPage struct {
	data   []byte
	pageId common.PageID
	lsn    common.LSN
}

// NewHashTableDirectoryPage creates a directory page view over the given data (page.GetData()).
func NewHashTableDirectoryPage(data []byte) *HashTableDirectoryPage {
	return &HashTableDirectoryPage{data: data}
}

func (p *HashTableDirectoryPage) GetPageId() common.PageID {
	return p.pageId
}

func (p *HashTableDirectoryPage) SetPageId(pid common.PageID) {
	p.pageId = pid
}

func (p *HashTableDirectoryPage) GetLSN() common.LSN {
	return p.lsn
}

func (p *HashTableDirectoryPage) SetLSN(lsn common.LSN) {
	p.lsn = lsn
}

// Init initializes the directory with the given max depth (default HTABLE_DIRECTORY_MAX_DEPTH).
func (p *HashTableDirectoryPage) Init(maxDepth uint32) {
	if len(p.data) < DirOffsetBucketPageIds+512*4 {
		return
	}
	if maxDepth == 0 {
		maxDepth = HTableDirectoryMaxDepth
	}
	binary.LittleEndian.PutUint32(p.data[dirOffsetMaxDepth:], maxDepth)
	binary.LittleEndian.PutUint32(p.data[dirOffsetGlobalDepth:], 0)
	for i := uint32(0); i < directoryArraySize; i++ {
		p.data[dirOffsetLocalDepths+i] = 0
	}
	for i := 0; i < 512*4; i++ {
		p.data[DirOffsetBucketPageIds+i] = 0
	}
}

// HashToBucketIndex maps hash to directory index using global depth mask.
func (p *HashTableDirectoryPage) HashToBucketIndex(hash uint32) uint32 {
	return hash & p.GetGlobalDepthMask()
}

func (p *HashTableDirectoryPage) GetBucketPageId(bucketIdx uint32) common.PageID {
	if bucketIdx >= directoryArraySize || len(p.data) < DirOffsetBucketPageIds+int(bucketIdx+1)*4 {
		return common.InvalidPageID
	}
	return common.PageID(binary.LittleEndian.Uint32(p.data[DirOffsetBucketPageIds+bucketIdx*4:]))
}

func (p *HashTableDirectoryPage) SetBucketPageId(bucketIdx uint32, bucketPageId common.PageID) {
	if bucketIdx < directoryArraySize && len(p.data) >= DirOffsetBucketPageIds+int(bucketIdx+1)*4 {
		binary.LittleEndian.PutUint32(p.data[DirOffsetBucketPageIds+bucketIdx*4:], uint32(bucketPageId))
	}
}

// GetSplitImageIndex returns the directory index of the split image of bucket_idx.
func (p *HashTableDirectoryPage) GetSplitImageIndex(bucketIdx uint32) uint32 {
	ld := p.GetLocalDepth(bucketIdx)
	highBit := uint32(1) << ld
	return bucketIdx ^ highBit
}

func (p *HashTableDirectoryPage) GetGlobalDepthMask() uint32 {
	gd := p.GetGlobalDepth()
	if gd >= 32 {
		return 0xFFFFFFFF
	}
	return (uint32(1) << gd) - 1
}

func (p *HashTableDirectoryPage) GetLocalDepthMask(bucketIdx uint32) uint32 {
	ld := p.GetLocalDepth(bucketIdx)
	if ld >= 32 {
		return 0xFFFFFFFF
	}
	return (uint32(1) << ld) - 1
}

func (p *HashTableDirectoryPage) GetGlobalDepth() uint32 {
	if len(p.data) < dirOffsetGlobalDepth+4 {
		return 0
	}
	return binary.LittleEndian.Uint32(p.data[dirOffsetGlobalDepth:])
}

func (p *HashTableDirectoryPage) GetMaxDepth() uint32 {
	if len(p.data) < dirOffsetMaxDepth+4 {
		return 0
	}
	return binary.LittleEndian.Uint32(p.data[dirOffsetMaxDepth:])
}

func (p *HashTableDirectoryPage) IncrGlobalDepth() {
	if len(p.data) >= dirOffsetGlobalDepth+4 {
		gd := binary.LittleEndian.Uint32(p.data[dirOffsetGlobalDepth:])
		binary.LittleEndian.PutUint32(p.data[dirOffsetGlobalDepth:], gd+1)
	}
}

func (p *HashTableDirectoryPage) DecrGlobalDepth() {
	if len(p.data) >= dirOffsetGlobalDepth+4 {
		gd := binary.LittleEndian.Uint32(p.data[dirOffsetGlobalDepth:])
		if gd > 0 {
			binary.LittleEndian.PutUint32(p.data[dirOffsetGlobalDepth:], gd-1)
		}
	}
}

func (p *HashTableDirectoryPage) CanShrink() bool {
	gd := p.GetGlobalDepth()
	if gd == 0 {
		return false
	}
	size := uint32(1) << gd
	for i := uint32(0); i < size; i++ {
		if p.GetLocalDepth(i) >= uint8(gd) {
			return false
		}
	}
	return true
}

func (p *HashTableDirectoryPage) Size() uint32 {
	return uint32(1) << p.GetGlobalDepth()
}

func (p *HashTableDirectoryPage) MaxSize() uint32 {
	return uint32(1) << p.GetMaxDepth()
}

func (p *HashTableDirectoryPage) GetLocalDepth(bucketIdx uint32) uint8 {
	if bucketIdx >= directoryArraySize || len(p.data) < dirOffsetLocalDepths+int(bucketIdx)+1 {
		return 0
	}
	return p.data[dirOffsetLocalDepths+bucketIdx]
}

func (p *HashTableDirectoryPage) SetLocalDepth(bucketIdx uint32, localDepth uint8) {
	if bucketIdx < directoryArraySize && len(p.data) >= dirOffsetLocalDepths+int(bucketIdx)+1 {
		p.data[dirOffsetLocalDepths+bucketIdx] = localDepth
	}
}

func (p *HashTableDirectoryPage) IncrLocalDepth(bucketIdx uint32) {
	if bucketIdx < directoryArraySize {
		ld := p.GetLocalDepth(bucketIdx)
		p.SetLocalDepth(bucketIdx, ld+1)
	}
}

func (p *HashTableDirectoryPage) DecrLocalDepth(bucketIdx uint32) {
	if bucketIdx < directoryArraySize {
		ld := p.GetLocalDepth(bucketIdx)
		if ld > 0 {
			p.SetLocalDepth(bucketIdx, ld-1)
		}
	}
}

func (p *HashTableDirectoryPage) GetLocalHighBit(bucketIdx uint32) uint32 {
	ld := p.GetLocalDepth(bucketIdx)
	return uint32(1) << ld
}

func (p *HashTableDirectoryPage) VerifyIntegrity() {
	pageId2Count := make(map[common.PageID]uint32)
	pageId2Ld := make(map[common.PageID]uint8)
	gd := p.GetGlobalDepth()

	for curIdx := uint32(0); curIdx < directoryArraySize; curIdx++ {
		curPageId := p.GetBucketPageId(curIdx)
		curLd := p.GetLocalDepth(curIdx)
		if curLd > uint8(gd) {
			level.Warn(common.Logger).Log(fmt.Sprintf("Verify Integrity: LD %d > GD %d", curLd, gd))
			p.PrintDirectory()
			os.Exit(1)
		}
		pageId2Count[curPageId]++
		if oldLd, ok := pageId2Ld[curPageId]; ok && oldLd != curLd {
			level.Warn(common.Logger).Log(fmt.Sprintf("Verify Integrity: cur local depth: %d, old local depth %d, for page id: %d", curLd, oldLd, curPageId))
			p.PrintDirectory()
			os.Exit(1)
		} else {
			pageId2Ld[curPageId] = curLd
		}
	}

	for curPageId, curCount := range pageId2Count {
		curLd := uint32(pageId2Ld[curPageId])
		requiredCount := uint32(1) << (gd - curLd)
		if curCount != requiredCount {
			level.Warn(common.Logger).Log(fmt.Sprintf("Verify Integrity: cur count: %d, required count: %d, for page id: %d", curCount, requiredCount, curPageId))
			p.PrintDirectory()
			os.Exit(1)
		}
	}
}

func (p *HashTableDirectoryPage) PrintDirectory() {
	gd := p.GetGlobalDepth()
	level.Debug(common.Logger).Log(fmt.Sprintf("======== DIRECTORY (global depth: %d) ========", gd))
	level.Debug(common.Logger).Log("| bucket idx | page id | local depth |")
	for idx := 0; idx < (1 << gd); idx++ {
		level.Debug(common.Logger).Log(fmt.Sprintf("|     %d     |     %d     |     %d     |", idx, p.GetBucketPageId(uint32(idx)), p.GetLocalDepth(uint32(idx))))
	}
	level.Debug(common.Logger).Log("================ END DIRECTORY ================")
}
