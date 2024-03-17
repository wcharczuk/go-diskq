package diskq

import "encoding/binary"

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
