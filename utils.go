package diskq

import (
	"fmt"
	"path/filepath"
)

func formatPathForPartition(cfg Config, partitionIndex uint32) string {
	return filepath.Join(
		cfg.Path,
		formatPartitionIndexForPath(partitionIndex),
	)
}

func formatPathForSegment(cfg Config, partitionIndex uint32, startOffset uint64) string {
	return filepath.Join(
		cfg.Path,
		formatPartitionIndexForPath(partitionIndex),
		formatStartOffsetForPath(startOffset),
	)
}

func formatPartitionIndexForPath(partitionIndex uint32) string {
	return fmt.Sprintf("%06d", partitionIndex)
}

func formatStartOffsetForPath(startOffset uint64) string {
	return fmt.Sprintf("%020d", startOffset)
}
