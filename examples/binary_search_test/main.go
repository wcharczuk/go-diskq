package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wcharczuk/go-diskq"
)

func main() {
	tempPath, done := tempDir()
	defer done()

	cfg := diskq.Config{
		Path:             tempPath,
		PartitionCount:   1,
		SegmentSizeBytes: 1 << 33, // 16gb per segment
	}

	dq, err := diskq.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	defer func() { _ = dq.Close() }()

	testStart := time.Now()

	for x := 0; x < 65536; x++ {
		_, _, _ = dq.Push(diskq.Message{
			PartitionKey: fmt.Sprintf("message-%d", x),
			TimestampUTC: testStart.Add(-time.Duration(x) * time.Minute),
			Data:         []byte(strings.Repeat("a", 2048)),
		})
	}

	var linearTimes []time.Duration
	for x := 0; x < 32; x++ {
		start := time.Now()
		_, _, _ = getOffsetAfterLinear(tempPath, 0, testStart.Add(-4096*time.Minute))
		linearTimes = append(linearTimes, time.Since(start))
	}

	var binaryTimes []time.Duration
	for x := 0; x < 32; x++ {
		start := time.Now()
		_, _, _ = diskq.GetOffsetAfter(tempPath, 0, testStart.Add(-4096*time.Minute))
		binaryTimes = append(binaryTimes, time.Since(start))
	}

	fmt.Println("test results")
	fmt.Println("------------")
	fmt.Printf("linear: %v\n", avgDurations(linearTimes).Round(time.Microsecond))
	fmt.Printf("binary: %v\n", avgDurations(binaryTimes).Round(time.Microsecond))
}

func avgDurations(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	accum := new(big.Int)
	for _, d := range durations {
		accum.Add(accum, big.NewInt(int64(d)))
	}
	accum.Div(accum, big.NewInt(int64(len(durations))))
	return time.Duration(accum.Int64())
}

func tempDir() (string, func()) {
	dir := filepath.Join(os.TempDir(), diskq.UUIDv4().String())
	_ = os.Mkdir(dir, 0755)
	return dir, func() {
		fmt.Println("removing temp dir", dir)
		_ = os.RemoveAll(dir)
	}
}

func getOffsetAfterLinear(path string, partitionIndex uint32, after time.Time) (offset uint64, found bool, err error) {
	var segments []uint64
	segments, err = diskq.GetPartitionSegmentStartOffsets(path, partitionIndex)
	if err != nil {
		return
	}

	var targetSegmentStartOffset uint64
	var newest time.Time
	for x := 0; x < len(segments); x++ {
		targetSegmentStartOffset = segments[x]
		newest, err = diskq.GetSegmentNewestTimestamp(path, partitionIndex, targetSegmentStartOffset)
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
	segmentTimeIndexHandle, err = diskq.OpenSegmentFileForRead(path, partitionIndex, targetSegmentStartOffset, diskq.ExtTimeIndex)
	if err != nil {
		return
	}
	defer func() { _ = segmentTimeIndexHandle.Close() }()

	var sti diskq.SegmentTimeIndex
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
