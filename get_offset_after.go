package diskq

import (
	"encoding/binary"
	"io"
	"os"
	"time"
)

// GetOffsetAfter returns the next offset after a given timestamp.
//
// If no offet was found after that timestamp, typically because the timestamp is after the "newest" offset
// the found bool will be false.
//
// GetOffsetAfter first will iterate over the individual partion segments until it finds one whose newest timestamp is before
// the given after timestamp value, then will iterate over the individual timestamps in the time index for the segment
// until the correct offset is found.
//
// A possible future optimization will be to binary search the timeindex file but the thinking currently is that
// seeks can be slightly expensive within a file handle, and it's best to just read the file sequentially.
func GetOffsetAfter(path string, partitionIndex uint32, after time.Time) (offset uint64, found bool, err error) {
	var segments []uint64
	segments, err = getPartitionSegmentOffsets(path, partitionIndex)
	if err != nil {
		return
	}

	var targetSegmentStartOffset uint64
	var newest time.Time
	for x := 0; x < len(segments); x++ {
		targetSegmentStartOffset = segments[x]
		newest, err = getSegmentNewestTimestamp(path, partitionIndex, targetSegmentStartOffset)
		if err != nil {
			return
		}
		if after.Before(newest) {
			found = true
			break
		}
	}
	if !found {
		return
	}

	var segmentTimeIndexHandle *os.File
	segmentTimeIndexHandle, err = openSegmentFileForRead(path, partitionIndex, targetSegmentStartOffset, extTimeIndex)
	if err != nil {
		return
	}
	defer segmentTimeIndexHandle.Close()

	var sti segmentTimeIndex
	for {
		err = binary.Read(segmentTimeIndexHandle, binary.LittleEndian, &sti)
		if err == io.EOF {
			err = nil
			found = false
			return
		}
		if sti.GetTimestampUTC().After(after) {
			offset = sti.GetOffset()
			return
		}
	}
}
