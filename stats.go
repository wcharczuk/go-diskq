package diskq

import (
	"os"
	"time"
)

// GetStats returns stats about a stream rooted at a given path.
func GetStats(path string) (*Stats, error) {
	partitions, err := getPartitions(path)
	if err != nil {
		return nil, err
	}

	output := Stats{
		Path: path,
	}
	if _, err := os.Stat(formatPathForSentinel(path)); err == nil {
		output.InUse = true
	}
	for _, partitionIndex := range partitions {
		partitionStats, err := getPartitionStats(path, partitionIndex)
		if err != nil {
			return nil, err
		}
		output.SizeBytes += partitionStats.SizeBytes
	}
	return &output, nil
}

func getPartitionStats(path string, partitionIndex uint32) (*PartitionStats, error) {
	output := PartitionStats{
		PartitionIndex: partitionIndex,
	}
	sizeBytes, err := getPartitionSizeBytes(path, partitionIndex)
	if err != nil {
		return nil, err
	}
	output.SizeBytes = uint64(sizeBytes)

	segments, err := getPartitionSegmentOffsets(path, partitionIndex)
	if err != nil {
		return nil, err
	}

	if len(segments) > 0 {
		output.OldestOffset = segments[0]
		output.OldestOffsetActive = segments[len(segments)-1]
		output.OldestTimestamp, err = getSegmentOldestTimestamp(path, partitionIndex, segments[0])
		if err != nil {
			return nil, err
		}
		output.OldestTimestampActive, err = getSegmentOldestTimestamp(path, partitionIndex, segments[len(segments)-1])
		if err != nil {
			return nil, err
		}
		output.NewestTimestamp, err = getSegmentNewestTimestamp(path, partitionIndex, segments[len(segments)-1])
		if err != nil {
			return nil, err
		}
		output.NewestOffset, err = getSegmentNewestOffset(path, partitionIndex, segments[len(segments)-1])
		if err != nil {
			return nil, err
		}
	}

	output.Segments = len(segments)
	return &output, nil
}

// Stats holds information about a diskq stream.
type Stats struct {
	Path       string
	SizeBytes  uint64
	InUse      bool
	Partitions []PartitionStats
}

// PartitionStats holds information about a diskq stream partition.
type PartitionStats struct {
	PartitionIndex uint32

	OldestOffset       uint64
	OldestOffsetActive uint64
	NewestOffset       uint64

	OldestTimestamp       time.Time
	OldestTimestampActive time.Time
	NewestTimestamp       time.Time

	Segments  int
	SizeBytes uint64
}
