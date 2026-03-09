package buffer

import (
	"goostub/common"
	"goostub/recovery"
	"goostub/storage/disk"
	"sync"
	"sync/atomic"
)

// CallbackType is used for buffer pool callbacks.
type CallbackType int

const (
	BEFORE CallbackType = iota
	AFTER
)

// BufferpoolCallback is an optional callback when pinning/unpinning.
type BufferpoolCallback func(t CallbackType, pid common.PageID)

// frameHeader holds one frame's data and metadata (BusTub FrameHeader).
type frameHeader struct {
	frameID  common.FrameID
	pinCount int32
	isDirty  bool
	data     []byte
	rwlatch  common.ReaderWriterLatch
}

// BufferPoolManager manages a pool of page frames and replaces pages via the replacer.
type BufferPoolManager struct {
	numFrames   int
	nextPageID  int32
	bpmLatch    sync.Mutex
	frames      []*frameHeader
	pageTable   map[common.PageID]common.FrameID
	freeFrames  []common.FrameID
	replacer    *ArcReplacer
	diskManager *disk.DiskManager
	logManager  *recovery.LogManager
}

// NewBufferPoolManager creates a buffer pool manager with the given number of frames.
func NewBufferPoolManager(numFrames int, diskManager *disk.DiskManager, logManager *recovery.LogManager) *BufferPoolManager {
	bpm := &BufferPoolManager{
		numFrames:   numFrames,
		nextPageID:  0,
		frames:      make([]*frameHeader, numFrames),
		pageTable:   make(map[common.PageID]common.FrameID),
		freeFrames:  make([]common.FrameID, 0, numFrames),
		replacer:    NewArcReplacer(numFrames),
		diskManager: diskManager,
		logManager:  logManager,
	}
	for i := 0; i < numFrames; i++ {
		fid := common.FrameID(i)
		bpm.frames[i] = &frameHeader{
			frameID:  fid,
			pinCount: 0,
			isDirty:  false,
			data:     make([]byte, common.PageSize),
			rwlatch:  common.NewRWLatch(),
		}
		bpm.freeFrames = append(bpm.freeFrames, fid)
	}
	return bpm
}

// Size returns the number of frames.
func (m *BufferPoolManager) Size() int {
	return m.numFrames
}

// NewPage allocates a new page and returns its id and a write guard, or (InvalidPageID, nil) on failure.
func (m *BufferPoolManager) NewPage() (common.PageID, *WritePageGuard) {
	m.bpmLatch.Lock()
	defer m.bpmLatch.Unlock()
	fid, ok := m.getFreeOrEvictFrame()
	if !ok {
		return common.InvalidPageID, nil
	}
	pid := common.PageID(m.diskManager.AllocatePage())
	m.frames[fid].pinCount = 1
	m.frames[fid].isDirty = false
	for i := range m.frames[fid].data {
		m.frames[fid].data[i] = 0
	}
	m.pageTable[pid] = fid
	m.replacer.RecordAccess(fid, pid, AccessTypeUnknown)
	m.replacer.SetEvictable(fid, false)
	return pid, &WritePageGuard{bpm: m, pageID: pid, valid: true}
}

// DeletePage removes the page from the buffer pool and deallocates it on disk.
func (m *BufferPoolManager) DeletePage(pageID common.PageID) bool {
	m.bpmLatch.Lock()
	defer m.bpmLatch.Unlock()
	fid, ok := m.pageTable[pageID]
	if !ok {
		m.diskManager.DeallocatePage(pageID)
		return true
	}
	frame := m.frames[fid]
	if frame.pinCount > 0 {
		return false
	}
	m.replacer.Remove(fid)
	delete(m.pageTable, pageID)
	m.diskManager.DeallocatePage(pageID)
	m.freeFrames = append(m.freeFrames, fid)
	frame.pinCount = 0
	frame.isDirty = false
	return true
}

// ReadPage fetches the page for reading; returns (guard, true) or (nil, false).
func (m *BufferPoolManager) ReadPage(pageID common.PageID) (*ReadPageGuard, bool) {
	m.bpmLatch.Lock()
	defer m.bpmLatch.Unlock()
	fid, ok := m.pageTable[pageID]
	if ok {
		m.frames[fid].pinCount++
		m.replacer.RecordAccess(fid, pageID, AccessTypeUnknown)
		m.replacer.SetEvictable(fid, false)
		return &ReadPageGuard{bpm: m, pageID: pageID, valid: true}, true
	}
	fid, got := m.getFreeOrEvictFrame()
	if !got {
		return nil, false
	}
	m.diskManager.ReadPage(pageID, m.frames[fid].data)
	m.frames[fid].pinCount = 1
	m.frames[fid].isDirty = false
	m.pageTable[pageID] = fid
	m.replacer.RecordAccess(fid, pageID, AccessTypeUnknown)
	m.replacer.SetEvictable(fid, false)
	return &ReadPageGuard{bpm: m, pageID: pageID, valid: true}, true
}

// WritePage fetches the page for writing; returns (guard, true) or (nil, false).
func (m *BufferPoolManager) WritePage(pageID common.PageID) (*WritePageGuard, bool) {
	m.bpmLatch.Lock()
	defer m.bpmLatch.Unlock()
	fid, ok := m.pageTable[pageID]
	if ok {
		m.frames[fid].pinCount++
		m.replacer.RecordAccess(fid, pageID, AccessTypeUnknown)
		m.replacer.SetEvictable(fid, false)
		return &WritePageGuard{bpm: m, pageID: pageID, valid: true}, true
	}
	fid, got := m.getFreeOrEvictFrame()
	if !got {
		return nil, false
	}
	m.diskManager.ReadPage(pageID, m.frames[fid].data)
	m.frames[fid].pinCount = 1
	m.frames[fid].isDirty = false
	m.pageTable[pageID] = fid
	m.replacer.RecordAccess(fid, pageID, AccessTypeUnknown)
	m.replacer.SetEvictable(fid, false)
	return &WritePageGuard{bpm: m, pageID: pageID, valid: true}, true
}

// FlushPage writes the page to disk if dirty.
func (m *BufferPoolManager) FlushPage(pageID common.PageID) bool {
	m.bpmLatch.Lock()
	fid, ok := m.pageTable[pageID]
	if !ok {
		m.bpmLatch.Unlock()
		return false
	}
	frame := m.frames[fid]
	if !frame.isDirty {
		m.bpmLatch.Unlock()
		return true
	}
	data := make([]byte, common.PageSize)
	copy(data, frame.data)
	m.bpmLatch.Unlock()
	m.diskManager.WritePage(pageID, data)
	m.bpmLatch.Lock()
	m.frames[fid].isDirty = false
	m.bpmLatch.Unlock()
	return true
}

// FlushAllPages flushes every page in the buffer pool.
func (m *BufferPoolManager) FlushAllPages() {
	m.bpmLatch.Lock()
	for pid := range m.pageTable {
		fid := m.pageTable[pid]
		if m.frames[fid].isDirty {
			data := make([]byte, common.PageSize)
			copy(data, m.frames[fid].data)
			m.bpmLatch.Unlock()
			m.diskManager.WritePage(pid, data)
			m.bpmLatch.Lock()
			m.frames[fid].isDirty = false
		}
	}
	m.bpmLatch.Unlock()
}

// GetPinCount returns the pin count for the page, or (0, false) if not present.
func (m *BufferPoolManager) GetPinCount(pageID common.PageID) (int, bool) {
	m.bpmLatch.Lock()
	defer m.bpmLatch.Unlock()
	fid, ok := m.pageTable[pageID]
	if !ok {
		return 0, false
	}
	return int(m.frames[fid].pinCount), true
}

// UnpinPage unpins the page and marks it dirty if specified. Kept for backward compatibility.
func (m *BufferPoolManager) UnpinPage(pid common.PageID, isDirty bool, fn BufferpoolCallback) bool {
	m.unpinPage(pid, isDirty)
	if fn != nil {
		fn(AFTER, pid)
	}
	return true
}

// unpinPage is called by guards on Drop.
func (m *BufferPoolManager) unpinPage(pageID common.PageID, isDirty bool) {
	m.bpmLatch.Lock()
	defer m.bpmLatch.Unlock()
	fid, ok := m.pageTable[pageID]
	if !ok {
		return
	}
	frame := m.frames[fid]
	if isDirty {
		frame.isDirty = true
	}
	n := atomic.AddInt32(&frame.pinCount, -1)
	if n == 0 {
		m.replacer.SetEvictable(fid, true)
	}
}

func (m *BufferPoolManager) getFrameData(pageID common.PageID) []byte {
	m.bpmLatch.Lock()
	defer m.bpmLatch.Unlock()
	fid, ok := m.pageTable[pageID]
	if !ok {
		return nil
	}
	return m.frames[fid].data
}

func (m *BufferPoolManager) getFrameDataMut(pageID common.PageID) []byte {
	m.bpmLatch.Lock()
	defer m.bpmLatch.Unlock()
	fid, ok := m.pageTable[pageID]
	if !ok {
		return nil
	}
	return m.frames[fid].data
}

func (m *BufferPoolManager) isFrameDirty(pageID common.PageID) bool {
	m.bpmLatch.Lock()
	defer m.bpmLatch.Unlock()
	fid, ok := m.pageTable[pageID]
	if !ok {
		return false
	}
	return m.frames[fid].isDirty
}

// getFreeOrEvictFrame returns a frame ID; caller holds bpmLatch.
func (m *BufferPoolManager) getFreeOrEvictFrame() (common.FrameID, bool) {
	if len(m.freeFrames) > 0 {
		n := len(m.freeFrames) - 1
		fid := m.freeFrames[n]
		m.freeFrames = m.freeFrames[:n]
		return fid, true
	}
	evicted, ok := m.replacer.Evict()
	if !ok {
		return 0, false
	}
	// Find page in this frame and evict it
	var pid common.PageID
	for p, f := range m.pageTable {
		if f == evicted {
			pid = p
			break
		}
	}
	frame := m.frames[evicted]
	if frame.isDirty {
		m.diskManager.WritePage(pid, frame.data)
	}
	delete(m.pageTable, pid)
	m.replacer.Remove(evicted)
	frame.pinCount = 0
	frame.isDirty = false
	return evicted, true
}
