package diskq

import "encoding/binary"

func newSegmentIndex(offset uint64, offsetBytes uint64, sizeBytes uint64) segmentIndex {
	return segmentIndex{offset, offsetBytes, sizeBytes}
}

type segmentIndex [3]uint64

func (si segmentIndex) GetOffset() uint64 {
	return si[0]
}

func (si segmentIndex) GetOffsetBytes() uint64 {
	return si[1]
}

func (si segmentIndex) GetSizeBytes() uint64 {
	return si[2]
}

var segmentIndexSize = binary.Size(segmentIndex{})
