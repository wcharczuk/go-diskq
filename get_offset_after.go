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
// the given after timestamp value, then will use a binary search over the individual timestamps in the time index for the segment
// until the correct offset is found.
func GetOffsetAfter(path string, partitionIndex uint32, after time.Time) (offset uint64, found bool, err error) {
	var segments []uint64
	segments, err = GetPartitionSegmentStartOffsets(path, partitionIndex)
	if err != nil {
		return
	}

	var targetSegmentStartOffset uint64
	var newest time.Time
	for x := 0; x < len(segments); x++ {
		targetSegmentStartOffset = segments[x]
		newest, err = GetSegmentNewestTimestamp(path, partitionIndex, targetSegmentStartOffset)
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
	segmentTimeIndexHandle, err = OpenSegmentFileForRead(path, partitionIndex, targetSegmentStartOffset, ExtTimeIndex)
	if err != nil {
		return
	}
	defer func() { _ = segmentTimeIndexHandle.Close() }()

	oldestOffset, newestOffset, err := GetSegmentNewestOldestOffsetFromTimeIndexHandle(segmentTimeIndexHandle)
	if err != nil {
		return
	}
	offsets := newestOffset - oldestOffset

	var searchTimeIndex SegmentTimeIndex
	offset, err = searchOffsets(offsets, func(relativeOffset uint64) (bool, error) {
		seekBytes := int64(relativeOffset) * int64(SegmentTimeIndexSizeBytes)
		_, searchErr := segmentTimeIndexHandle.Seek(seekBytes, io.SeekStart)
		if searchErr != nil {
			return false, searchErr
		}
		searchErr = binary.Read(segmentTimeIndexHandle, binary.LittleEndian, &searchTimeIndex)
		if searchErr != nil {
			return false, searchErr
		}
		return searchTimeIndex.GetTimestampUTC().After(after), nil
	})
	offset = offset + oldestOffset
	return
}

func searchOffsets(n uint64, f func(uint64) (bool, error)) (uint64, error) {
	i, j := uint64(0), n
	for i < j {
		h := uint64(i+j) >> 1
		less, err := f(h)
		if err != nil {
			return 0, err
		}
		if !less {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, nil
}
