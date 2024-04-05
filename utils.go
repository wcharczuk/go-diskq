package diskq

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// FormatPathForSentinel formats a path string for a sentinel
// (or "owner") file for a given stream path.
func FormatPathForSentinel(path string) string {
	return filepath.Join(path, "owner")
}

// FormatPathForPartitions formats a path string for the partitions
// directory within a stream directory.
func FormatPathForPartitions(path string) string {
	return filepath.Join(
		path,
		"parts",
	)
}

// FormatPathForPartition formats a path string for an individual partition
// within the partitions directory of a stream path.
func FormatPathForPartition(path string, partitionIndex uint32) string {
	return filepath.Join(
		FormatPathForPartitions(path),
		FormatPartitionIndexForPath(partitionIndex),
	)
}

// FormatPathForSegment formats a path string for a specific segment of a given partition.
func FormatPathForSegment(path string, partitionIndex uint32, startOffset uint64) string {
	return filepath.Join(
		FormatPathForPartition(path, partitionIndex),
		formatStartOffsetForPath(startOffset),
	)
}

// FormatPartitionIndexForPath returns a partition index as a string.
func FormatPartitionIndexForPath(partitionIndex uint32) string {
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

// GetSegmentNewestTimestamp opens a segment timeindex file as indicated by the path, partitionIndex and startOffset for the file
// and returns the newest (or last) timestamp in the index.
func GetSegmentNewestTimestamp(path string, partitionIndex uint32, startOffset uint64) (ts time.Time, err error) {
	var segment SegmentTimeIndex
	var f *os.File
	f, err = os.Open(FormatPathForSegment(path, partitionIndex, startOffset) + ExtTimeIndex)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()

	var stat fs.FileInfo
	stat, err = f.Stat()
	if err != nil {
		return
	}
	if stat.Size() < int64(SegmentTimeIndexSizeBytes) {
		return
	}

	if _, err = f.Seek(-int64(SegmentTimeIndexSizeBytes), io.SeekEnd); err != nil {
		return
	}
	err = binary.Read(f, binary.LittleEndian, &segment)
	if err != nil {
		return
	}
	ts = segment.GetTimestampUTC()
	return
}

// GetSegmentOldestTimestamp opens a segment timeindex file as indicated by the path, partitionIndex and startOffset for the file
// and returns the oldest (or first) timestamp in the index.
func GetSegmentOldestTimestamp(path string, partitionIndex uint32, startOffset uint64) (oldest time.Time, err error) {
	var segment SegmentTimeIndex
	var f *os.File
	f, err = os.Open(FormatPathForSegment(path, partitionIndex, startOffset) + ExtTimeIndex)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()

	var stat fs.FileInfo
	stat, err = f.Stat()
	if err != nil {
		return
	}
	if stat.Size() < int64(SegmentTimeIndexSizeBytes) {
		return
	}
	err = binary.Read(f, binary.LittleEndian, &segment)
	if err != nil {
		return
	}
	oldest = segment.GetTimestampUTC()
	return
}

// GetSegmentOldestTimestamp opens a segment timeindex file as indicated by the path, partitionIndex and startOffset for the file
// and returns both the oldest (or first) and the newest (or last) timestamp in the index.
func GetSegmentOldestNewestTimestamps(path string, partitionIndex uint32, startOffset uint64) (oldest, newest time.Time, err error) {
	var segment SegmentTimeIndex
	var f *os.File
	f, err = os.Open(FormatPathForSegment(path, partitionIndex, startOffset) + ExtTimeIndex)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()

	var stat fs.FileInfo
	stat, err = f.Stat()
	if err != nil {
		return
	}
	if stat.Size() < int64(SegmentTimeIndexSizeBytes) {
		return
	}
	err = binary.Read(f, binary.LittleEndian, &segment)
	if err != nil {
		return
	}
	oldest = segment.GetTimestampUTC()
	if _, err = f.Seek(-int64(SegmentTimeIndexSizeBytes), io.SeekEnd); err != nil {
		return
	}
	err = binary.Read(f, binary.LittleEndian, &segment)
	if err != nil {
		return
	}
	newest = segment.GetTimestampUTC()
	return
}

// GetSegmentOldestTimestamp opens a segment index file as indicated by the path, partitionIndex and startOffset for the file
// and returns the newest (or last) offset in the index.
func GetSegmentNewestOffset(path string, partitionIndex uint32, startOffset uint64) (newestOffset uint64, err error) {
	var segment SegmentIndex
	var f *os.File
	f, err = os.Open(FormatPathForSegment(path, partitionIndex, startOffset) + ExtIndex)
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
	if fsize < int64(SegmentIndexSizeBytes) {
		newestOffset = startOffset
		return
	}

	if _, err = f.Seek(-int64(SegmentIndexSizeBytes), io.SeekEnd); err != nil {
		return
	}
	err = binary.Read(f, binary.LittleEndian, &segment)
	if err != nil {
		return
	}
	newestOffset = segment.GetOffset()
	return
}

// GetSegmentNewestOldestOffsetFromTimeIndexHandle returns the oldest (or first) and newest (or last) offsets
// from an already open file handle to a timeindex by seeking to the 0th byte and reading a timeindex segment, then
// seeking to the -SegmentTimeIndexSizeBytes-th byte from the end of the file, and reading the last timeindex segment.
func GetSegmentNewestOldestOffsetFromTimeIndexHandle(f *os.File) (oldestOffset, newestOffset uint64, err error) {
	var fstat fs.FileInfo
	fstat, err = f.Stat()
	if err != nil {
		return
	}
	fsize := fstat.Size()
	if fsize < int64(SegmentIndexSizeBytes) {
		return
	}

	var segment SegmentTimeIndex
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return
	}
	err = binary.Read(f, binary.LittleEndian, &segment)
	if err != nil {
		return
	}
	oldestOffset = segment.GetOffset()

	if _, err = f.Seek(-int64(SegmentTimeIndexSizeBytes), io.SeekEnd); err != nil {
		return
	}
	err = binary.Read(f, binary.LittleEndian, &segment)
	if err != nil {
		return
	}
	newestOffset = segment.GetOffset()
	return
}

// GetSegmentStartOffsetForOffset searches a given list of entries for a given offset such that the entry returned
// would correspond to the startoffset of the segment file that _would_ contain that offset.
func GetSegmentStartOffsetForOffset(entries []uint64, offset uint64) (uint64, bool) {
	for x := len(entries) - 1; x >= 0; x-- {
		startOffset := entries[x]
		if startOffset <= offset {
			return startOffset, true
		}
	}
	return 0, false
}

// GetPartitions returns the partition indices of the partitions in a given stream by path.
func GetPartitions(path string) ([]uint32, error) {
	dirEntries, err := os.ReadDir(FormatPathForPartitions(path))
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
	partitionIndexes, err := GetPartitions(path)
	if err != nil {
		return nil, err
	}
	output := make(map[uint32]struct{})
	for _, partitionIndex := range partitionIndexes {
		output[partitionIndex] = struct{}{}
	}
	return output, nil
}

// GetPartitionSizeBytes gets the size in bytes of a partition by path and partition index.
//
// It does this by iterating over the segment files for the partition and stat-ing the files.
func GetPartitionSizeBytes(path string, partitionIndex uint32) (sizeBytes int64, err error) {
	var offsets []uint64
	offsets, err = GetPartitionSegmentStartOffsets(path, partitionIndex)
	if err != nil {
		return
	}
	var info fs.FileInfo
	for _, offset := range offsets {
		segmentRoot := FormatPathForSegment(path, partitionIndex, offset)
		info, err = os.Stat(segmentRoot + ExtData)
		if err != nil {
			return
		}
		sizeBytes += info.Size()
	}
	return
}

// GetPartitionSegmentStartOffsets gets the start offsets of a given partition as derrived by the filenames
// of the segments within a partition's data directory.
func GetPartitionSegmentStartOffsets(path string, partitionIndex uint32) (output []uint64, err error) {
	entries, err := os.ReadDir(FormatPathForPartition(path, partitionIndex))
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
		if !strings.HasSuffix(e.Name(), ExtData) {
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

// internal utils

func hashIndexForMessage(m Message, partitions int) int {
	h := fnv.New32()
	_, _ = h.Write([]byte(m.PartitionKey))
	return int(h.Sum32()) % partitions
}

func maybeSync(wr io.Writer) error {
	if typed, ok := wr.(*os.File); ok {
		return typed.Sync()
	}
	return nil
}
