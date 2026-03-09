package buffer

import (
	"goostub/common"
)

// ReadPageGuard grants read access to a page; release with Drop() to unpin.
// Do not copy; use by reference.
type ReadPageGuard struct {
	bpm    *BufferPoolManager
	pageID common.PageID
	valid  bool
}

// GetPageId returns the page ID of the guarded page.
func (g *ReadPageGuard) GetPageId() common.PageID {
	if !g.valid {
		return common.InvalidPageID
	}
	return g.pageID
}

// GetData returns a read-only view of the page data (caller must not modify).
func (g *ReadPageGuard) GetData() []byte {
	if !g.valid {
		return nil
	}
	return g.bpm.getFrameData(g.pageID)
}

// IsDirty returns whether the page is marked dirty.
func (g *ReadPageGuard) IsDirty() bool {
	if !g.valid {
		return false
	}
	return g.bpm.isFrameDirty(g.pageID)
}

// Flush writes the page to disk if dirty.
func (g *ReadPageGuard) Flush() {
	if g.valid {
		g.bpm.FlushPage(g.pageID)
	}
}

// Drop releases the guard and unpins the page. Idempotent.
func (g *ReadPageGuard) Drop() {
	if !g.valid {
		return
	}
	g.valid = false
	g.bpm.unpinPage(g.pageID, false)
}

// WritePageGuard grants write access to a page; release with Drop() to unpin and mark dirty.
type WritePageGuard struct {
	bpm    *BufferPoolManager
	pageID common.PageID
	valid  bool
}

// GetPageId returns the page ID of the guarded page.
func (g *WritePageGuard) GetPageId() common.PageID {
	if !g.valid {
		return common.InvalidPageID
	}
	return g.pageID
}

// GetData returns the page data for reading.
func (g *WritePageGuard) GetData() []byte {
	return g.GetDataMut()
}

// GetDataMut returns the page data for reading and writing.
func (g *WritePageGuard) GetDataMut() []byte {
	if !g.valid {
		return nil
	}
	return g.bpm.getFrameDataMut(g.pageID)
}

// IsDirty returns whether the page is marked dirty.
func (g *WritePageGuard) IsDirty() bool {
	if !g.valid {
		return false
	}
	return g.bpm.isFrameDirty(g.pageID)
}

// Flush writes the page to disk if dirty.
func (g *WritePageGuard) Flush() {
	if g.valid {
		g.bpm.FlushPage(g.pageID)
	}
}

// Drop releases the guard, marks the page dirty, and unpins. Idempotent.
func (g *WritePageGuard) Drop() {
	if !g.valid {
		return
	}
	g.valid = false
	g.bpm.unpinPage(g.pageID, true)
}
