package diskq

import (
	"bytes"
	"os"
	"testing"
	"time"
)

func Test_Partition_writeToNewActiveSegment(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	m := Message{
		PartitionKey: "aaa",
		TimestampUTC: time.Date(2024, 01, 02, 12, 11, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	}
	expectPartitionKey := hashIndexForMessage(m, defaultPartitionCount)
	messageSize := messageSizeBytes(m)

	cfg := Config{
		Path:             testPath,
		SegmentSizeBytes: 3 * messageSize,
	}

	dq, err := New(cfg)
	assert_noerror(t, err)

	_, _, _ = dq.Push(m)
	_, _, _ = dq.Push(m)
	_, _, _ = dq.Push(m)
	_, _, _ = dq.Push(m)

	// expect a new active segment
	assert_equal(t, 4, dq.partitions[expectPartitionKey].activeSegment.startOffset)

	_, _, _ = dq.Push(m)
	_, _, _ = dq.Push(m)
	_, _, _ = dq.Push(m)
	_, _, _ = dq.Push(m)

	// now we need to tear apart the individual partition files

	index00, err := os.ReadFile(formatPathForSegment(cfg.Path, uint32(expectPartitionKey), 0) + extIndex)
	assert_noerror(t, err)

	entries := readIndexEntries(bytes.NewReader(index00))
	assert_equal(t, 0, entries[0].GetOffset())
	assert_equal(t, 0, entries[0].GetOffsetBytes())
	assert_equal(t, messageSize, entries[0].GetSizeBytes())

	assert_equal(t, 1, entries[1].GetOffset())
	assert_equal(t, messageSize, entries[1].GetOffsetBytes())
	assert_equal(t, messageSize, entries[1].GetSizeBytes())

	assert_equal(t, 2, entries[2].GetOffset())
	assert_equal(t, 2*messageSize, entries[2].GetOffsetBytes())
	assert_equal(t, messageSize, entries[2].GetSizeBytes())

	assert_equal(t, 3, entries[3].GetOffset())
	assert_equal(t, 3*messageSize, entries[3].GetOffsetBytes())
	assert_equal(t, messageSize, entries[3].GetSizeBytes())

	index01, err := os.ReadFile(formatPathForSegment(cfg.Path, uint32(expectPartitionKey), 4) + extIndex)
	assert_noerror(t, err)

	entries = readIndexEntries(bytes.NewReader(index01))
	assert_equal(t, 4, entries[0].GetOffset())
	assert_equal(t, 0, entries[0].GetOffsetBytes())
	assert_equal(t, messageSize, entries[0].GetSizeBytes())

	assert_equal(t, 5, entries[1].GetOffset())
	assert_equal(t, messageSize, entries[1].GetOffsetBytes())
	assert_equal(t, messageSize, entries[1].GetSizeBytes())

	assert_equal(t, 6, entries[2].GetOffset())
	assert_equal(t, 2*messageSize, entries[2].GetOffsetBytes())
	assert_equal(t, messageSize, entries[2].GetSizeBytes())

	assert_equal(t, 7, entries[3].GetOffset())
	assert_equal(t, 3*messageSize, entries[3].GetOffsetBytes())
	assert_equal(t, messageSize, entries[3].GetSizeBytes())
}
