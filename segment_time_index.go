package diskq

import (
	"encoding/binary"
	"time"
)

// NewSegmentTimeIndex returns a new segment time index struct.
func NewSegmentTimeIndex(offset uint64, timestamp time.Time) SegmentTimeIndex {
	return SegmentTimeIndex{offset, uint64(timestamp.UnixNano())}
}

// SegmentTimeIndex is a fixed with element of a time index file.
type SegmentTimeIndex [2]uint64

// GetOffset gets the offset the time index entry corresponds to.
func (sti SegmentTimeIndex) GetOffset() uint64 {
	return sti[0]
}

// GetTimestampUTC gets the timestamp (as recorded as nanos)
// from the index entry.
func (sti SegmentTimeIndex) GetTimestampUTC() time.Time {
	return time.Unix(0, int64(sti[1])).UTC()
}

// SegmentTimeIndexSizeBytes is the size in bytes of a segment
// time index element.
var SegmentTimeIndexSizeBytes = binary.Size(SegmentTimeIndex{})
