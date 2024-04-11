package diskq

import (
	"encoding/binary"
	"io"
)

// NewSegmentIndex returns a new segment index element.
func NewSegmentIndex(offset uint64, offsetBytes uint64, sizeBytes uint64) SegmentIndex {
	return SegmentIndex{offset, offsetBytes, sizeBytes}
}

// SegmentIndex is an individual element of an index file.
type SegmentIndex [3]uint64

// GetOffset gets the offset this entry corresponds to.
func (si SegmentIndex) GetOffset() uint64 {
	return si[0]
}

// GetOffsetBytes gets the offset from the start of the data
// file in bytes this entry appears in the data file.
func (si SegmentIndex) GetOffsetBytes() uint64 {
	return si[1]
}

// GetSizeBytes returns the size in bytes of the data file entry
// that corresponds to this offset.
//
// It is used when reading out the data file, specifically we will
// allocate this many bytes to read from for the data file entry.
func (si SegmentIndex) GetSizeBytes() uint64 {
	return si[2]
}

// SegmentIndexSizeBytes is the size in bytes of an entry in
// the segment index.
var SegmentIndexSizeBytes = binary.Size(SegmentIndex{})

// writeSegmentIndex writes a segment to a given writer with a reused
// intermediate buffer.
func writeSegmentIndex(wr io.Writer, buf []byte, seg SegmentIndex) (n int, err error) {
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

	buf[16] = byte(seg[2])
	buf[17] = byte(seg[2] >> 8)
	buf[18] = byte(seg[2] >> 16)
	buf[19] = byte(seg[2] >> 24)
	buf[20] = byte(seg[2] >> 32)
	buf[21] = byte(seg[2] >> 40)
	buf[22] = byte(seg[2] >> 48)
	buf[23] = byte(seg[2] >> 56)

	n, err = wr.Write(buf)
	return
}

// readSegmentIndex reads a segment from a given writer with a reused intermediate buffer.
func readSegmentIndex(r io.Reader, b []byte, seg *SegmentIndex) (err error) {
	var n int
	n, err = r.Read(b)
	if err != nil {
		return
	}

	if n < SegmentIndexSizeBytes {
		err = io.ErrUnexpectedEOF
		return
	}

	(*seg)[0] = uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56

	(*seg)[1] = uint64(b[8]) | uint64(b[9])<<8 | uint64(b[10])<<16 | uint64(b[11])<<24 |
		uint64(b[12])<<32 | uint64(b[13])<<40 | uint64(b[14])<<48 | uint64(b[15])<<56

	(*seg)[2] = uint64(b[16]) | uint64(b[17])<<8 | uint64(b[18])<<16 | uint64(b[19])<<24 |
		uint64(b[20])<<32 | uint64(b[21])<<40 | uint64(b[22])<<48 | uint64(b[23])<<56

	return
}
