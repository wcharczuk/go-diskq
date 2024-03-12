package diskq

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

func formatPathForPartition(path string, partitionIndex uint32) string {
	return filepath.Join(
		path,
		formatPartitionIndexForPath(partitionIndex),
	)
}

func formatPathForSegment(path string, partitionIndex uint32, startOffset uint64) string {
	return filepath.Join(
		path,
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

func parseSegmentOffsetFromPath(path string) (uint64, error) {
	pathBase := filepath.Base(path)
	rawStartOffset := strings.TrimSuffix(pathBase, filepath.Ext(pathBase))
	return strconv.ParseUint(rawStartOffset, 10, 64)
}
