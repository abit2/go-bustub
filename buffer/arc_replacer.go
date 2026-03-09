package buffer

import (
	"goostub/common"
	"sync"
)

// AccessType indicates how a page was accessed (for ARC policy).
type AccessType int

const (
	AccessTypeUnknown AccessType = iota
	AccessTypeLookup
	AccessTypeScan
	AccessTypeIndex
)

// ArcStatus is the list/bucket a frame belongs to in ARC.
type ArcStatus int

const (
	ArcStatusMRU ArcStatus = iota
	ArcStatusMFU
	ArcStatusMRUGhost
	ArcStatusMFUGhost
)

// FrameStatus holds per-frame state for the ARC replacer.
type FrameStatus struct {
	PageID    common.PageID
	FrameID   common.FrameID
	Evictable bool
	Status    ArcStatus
}

// ArcReplacer implements the ARC (Adaptive Replacement Cache) replacement policy.
type ArcReplacer struct {
	replacerSize int
	latch        sync.Mutex
	// Stub fields; full implementation would add mru_, mfu_, ghost lists, maps, etc.
}

// NewArcReplacer creates a new ARC replacer with the given capacity.
func NewArcReplacer(numFrames int) *ArcReplacer {
	return &ArcReplacer{
		replacerSize: numFrames,
	}
}

// Evict selects a victim frame and removes it from the replacer.
// Returns (frameID, true) if a victim was found, (0, false) otherwise.
func (r *ArcReplacer) Evict() (common.FrameID, bool) {
	r.latch.Lock()
	defer r.latch.Unlock()
	// TODO: implement ARC eviction
	return 0, false
}

// RecordAccess records that the given frame was accessed with the given page and access type.
func (r *ArcReplacer) RecordAccess(frameID common.FrameID, pageID common.PageID, accessType AccessType) {
	r.latch.Lock()
	defer r.latch.Unlock()
	_ = accessType
	_, _ = frameID, pageID
	// TODO: implement
}

// SetEvictable marks the frame as evictable or not.
func (r *ArcReplacer) SetEvictable(frameID common.FrameID, evictable bool) {
	r.latch.Lock()
	defer r.latch.Unlock()
	_, _ = frameID, evictable
	// TODO: implement
}

// Remove removes the frame from the replacer (e.g. when page is evicted).
func (r *ArcReplacer) Remove(frameID common.FrameID) {
	r.latch.Lock()
	defer r.latch.Unlock()
	_ = frameID
	// TODO: implement
}

// Size returns the number of evictable frames in the replacer.
func (r *ArcReplacer) Size() int {
	r.latch.Lock()
	defer r.latch.Unlock()
	// TODO: implement
	return 0
}
