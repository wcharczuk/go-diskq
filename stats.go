package diskq

import (
	"fmt"
	"os"
	"time"
)

// GetStats returns stats about a stream rooted at a given path.
func GetStats(path string) (*Stats, error) {
	partitions, err := GetPartitions(path)
	if err != nil {
		return nil, fmt.Errorf("diskq; get stats; cannot get partitions: %w", err)
	}

	output := Stats{
		Path: path,
	}
	if _, err := os.Stat(FormatPathForSentinel(path)); err == nil {
		output.InUse = true
	}
	for _, partitionIndex := range partitions {
		partitionStats, err := getPartitionStats(path, partitionIndex)
		if err != nil {
			return nil, err
		}
		output.SizeBytes += partitionStats.SizeBytes
		output.TotalOffsets += (partitionStats.NewestOffset - partitionStats.OldestOffset)
		output.Partitions = append(output.Partitions, *partitionStats)

		partitionAge := partitionStats.NewestTimestamp.Sub(partitionStats.OldestTimestamp)
		if partitionAge > output.Age {
			output.Age = partitionAge
		}
	}
	return &output, nil
}

func getPartitionStats(path string, partitionIndex uint32) (*PartitionStats, error) {
	output := PartitionStats{
		PartitionIndex: partitionIndex,
	}
	sizeBytes, err := GetPartitionSizeBytes(path, partitionIndex)
	if err != nil {
		return nil, fmt.Errorf("diskq; get stats; cannot get partition size bytes: %w", err)
	}
	output.SizeBytes = uint64(sizeBytes)

	segments, err := GetPartitionSegmentStartOffsets(path, partitionIndex)
	if err != nil {
		return nil, fmt.Errorf("diskq; get stats; cannot get partition segment offsets: %w", err)
	}

	if len(segments) > 0 {
		output.OldestOffset = segments[0]
		output.OldestOffsetActive = segments[len(segments)-1]
		output.NewestOffset, err = GetSegmentNewestOffset(path, partitionIndex, segments[len(segments)-1])
		if err != nil {
			return nil, fmt.Errorf("diskq; get stats; cannot get partition newest offset: %w", err)
		}

		output.OldestTimestamp, err = GetSegmentOldestTimestamp(path, partitionIndex, segments[0])
		if err != nil {
			return nil, fmt.Errorf("diskq; get stats; cannot get partition oldest timestamp: %w", err)
		}
		output.OldestTimestampActive, err = GetSegmentOldestTimestamp(path, partitionIndex, segments[len(segments)-1])
		if err != nil {
			return nil, fmt.Errorf("diskq; get stats; cannot get partition oldest timestamp active: %w", err)
		}
		output.NewestTimestamp, err = GetSegmentNewestTimestamp(path, partitionIndex, segments[len(segments)-1])
		if err != nil {
			return nil, fmt.Errorf("diskq; get stats; cannot get partition newest timestamp: %w", err)
		}
	}

	output.Segments = len(segments)
	return &output, nil
}

// Stats holds information about a diskq stream.
type Stats struct {
	Path         string
	SizeBytes    uint64
	InUse        bool
	TotalOffsets uint64
	Age          time.Duration
	Partitions   []PartitionStats
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
