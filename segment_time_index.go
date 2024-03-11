package diskq

import (
	"time"
)

func newSegmentTimeIndex(offset uint64, timestamp time.Time) segmentTimeIndex {
	return segmentTimeIndex{offset, uint64(timestamp.UnixNano())}
}

type segmentTimeIndex [2]uint64

func (sti segmentTimeIndex) GetOffset() uint64 {
	return sti[0]
}

func (sti segmentTimeIndex) GetTimestampUTC() time.Time {
	return time.Unix(0, int64(sti[1])).UTC()
}

// var segmentTimeIndexSize = binary.Size(segmentTimeIndex{})
