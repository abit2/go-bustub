// Copyright (c) 2021 Qitian Zeng
// 
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package buffer

import (
	"goostub/common"
)

type ClockReplacer struct {
	// Student's code: add fields for clock replacement policy
}

// Student: implement everything below

func NewClockReplacer(numPages int64) *ClockReplacer {
	// TODO: implement
	return &ClockReplacer{}
}

func (r *ClockReplacer) Victim(frameId *common.FrameID) bool {
	// TODO: implement
	return false
}

func (r *ClockReplacer) Pin(frameId common.FrameID) {
	// TODO: implement
}

func (r *ClockReplacer) Unpin(frameId common.FrameID) {
	// TODO: implement
}

func (r *ClockReplacer) Size() int64 {
	// TODO: implement
	return 0
}
