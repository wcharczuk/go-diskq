package diskq

import (
	"encoding/binary"
	"io"
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

// writeSegmentIndex writes a segment to a given writer with a reused
// intermediate buffer.
func writeSegmentTimeIndex(wr io.Writer, buf []byte, seg SegmentTimeIndex) (n int, err error) {
	buf[0] = byte(seg[0])
	buf[1] = byte(seg[0] >> 8)
	buf[2] = byte(seg[0] >> 16)
	buf[3] = byte(seg[0] >> 24)
	buf[4] = byte(seg[0] >> 32)
	buf[5] = byte(seg[0] >> 40)
	buf[6] = byte(seg[0] >> 48)
	buf[7] = byte(seg[0] >> 56)

	buf[8] = byte(seg[1])
	buf[9] = byte(seg[1] >> 8)
	buf[10] = byte(seg[1] >> 16)
	buf[11] = byte(seg[1] >> 24)
	buf[12] = byte(seg[1] >> 32)
	buf[13] = byte(seg[1] >> 40)
	buf[14] = byte(seg[1] >> 48)
	buf[15] = byte(seg[1] >> 56)

	n, err = wr.Write(buf)
	return
}
