package diskq

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func formatPathForPartitions(path string) string {
	return filepath.Join(
		path,
		"parts",
	)
}

func formatPathForPartition(path string, partitionIndex uint32) string {
	return filepath.Join(
		formatPathForPartitions(path),
		formatPartitionIndexForPath(partitionIndex),
	)
}

func formatPathForSegment(path string, partitionIndex uint32, startOffset uint64) string {
	return filepath.Join(
		formatPathForPartition(path, partitionIndex),
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

func getSegmentEndTimestamp(path string, partitionIndex uint32, startOffset uint64) (ts time.Time, err error) {
	var segment segmentTimeIndex
	var f *os.File
	f, err = os.Open(formatPathForSegment(path, partitionIndex, startOffset) + extTimeIndex)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return
	}
	err = binary.Read(f, binary.LittleEndian, &segment)
	if err != nil {
		return
	}
	ts = segment.GetTimestampUTC()
	return
}

func getSegmentEndOffset(path string, partitionIndex uint32, startOffset uint64) (endOffset uint64, err error) {
	var segment segmentIndex
	var f *os.File
	f, err = os.Open(formatPathForSegment(path, partitionIndex, startOffset) + extIndex)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()

	var fstat fs.FileInfo
	fstat, err = f.Stat()
	if err != nil {
		return
	}

	fsize := fstat.Size()
	if fsize < int64(segmentIndexSize) {
		endOffset = startOffset
		return
	}

	if _, err = f.Seek(-int64(segmentIndexSize), io.SeekEnd); err != nil {
		return
	}
	err = binary.Read(f, binary.LittleEndian, &segment)
	if err != nil {
		return
	}
	endOffset = segment.GetOffset()
	return
}

func getSegmentStartOffsetForOffset(entries []uint64, offset uint64) (uint64, bool) {
	for x := len(entries) - 1; x >= 0; x-- {
		startOffset := entries[x]
		if startOffset <= offset {
			return startOffset, true
		}
	}
	return 0, false
}

func getPartitionSegmentOffsets(path string, partitionIndex uint32) (output []uint64, err error) {
	entries, err := os.ReadDir(formatPathForPartition(path, partitionIndex))
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(e.Name(), extData) {
			continue
		}
		segmentStartOffset, err := parseSegmentOffsetFromPath(e.Name())
		if err != nil {
			return nil, err
		}
		output = append(output, segmentStartOffset)
	}
	return
}

func getPartitions(path string) ([]uint32, error) {
	dirEntries, err := os.ReadDir(formatPathForPartitions(path))
	if err != nil {
		return nil, err
	}
	output := make([]uint32, 0, len(dirEntries))
	for _, de := range dirEntries {
		if !de.IsDir() {
			continue
		}
		identifier, err := strconv.ParseUint(filepath.Base(de.Name()), 10, 32)
		if err != nil {
			return nil, err
		}
		output = append(output, uint32(identifier))
	}
	return output, nil
}

func getPartitionsLookup(path string) (map[uint32]struct{}, error) {
	partitionIndexes, err := getPartitions(path)
	if err != nil {
		return nil, err
	}
	output := make(map[uint32]struct{})
	for _, partitionIndex := range partitionIndexes {
		output[partitionIndex] = struct{}{}
	}
	return output, nil
}
