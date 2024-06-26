package diskq

import (
	"bytes"
	"os"
	"testing"
	"time"
)

func Test_Partition_create(t *testing.T) {
	testPath, done := tempDir()
	t.Cleanup(done)

	p, err := createOrOpenPartition(testPath, Options{}, 1)
	assert_noerror(t, err)
	assert_equal(t, testPath, p.path)
	assert_equal(t, 1, p.index)

	offsets, err := GetPartitionSegmentStartOffsets(testPath, 1)
	assert_noerror(t, err)
	assert_equal(t, 1, len(offsets))
}

func Test_Partition_open(t *testing.T) {
	testPath, done := tempDir()
	t.Cleanup(done)

	p, err := createOrOpenPartition(testPath, Options{}, 1)
	assert_noerror(t, err)
	assert_equal(t, testPath, p.path)
	assert_equal(t, 1, p.index)
	assert_noerror(t, p.Close())

	opened, err := createOrOpenPartition(testPath, Options{}, 1)
	assert_noerror(t, err)
	assert_equal(t, testPath, opened.path)
	assert_equal(t, 1, opened.index)
	assert_noerror(t, opened.Close())

	offsets, err := GetPartitionSegmentStartOffsets(testPath, 1)
	assert_noerror(t, err)
	assert_equal(t, 1, len(offsets))
}

func Test_Partition_writeToNewActiveSegment(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	m := Message{
		PartitionKey: "aaa",
		TimestampUTC: time.Date(2024, 01, 02, 12, 11, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	}
	expectPartitionKey := hashIndexForMessage(m, DefaultPartitionCount)
	messageSize := messageSizeBytes(m)

	cfg := Options{
		SegmentSizeBytes: 3 * messageSize,
	}

	dq, err := New(testPath, cfg)
	assert_noerror(t, err)

	_, _, _ = dq.Push(m)
	_, _, _ = dq.Push(m)
	_, _, _ = dq.Push(m)
	_, _, _ = dq.Push(m)

	// expect a new active segment
	assert_equal(t, 3, dq.partitions[expectPartitionKey].activeSegment.startOffset)

	_, _, _ = dq.Push(m)
	_, _, _ = dq.Push(m)
	_, _, _ = dq.Push(m)
	_, _, _ = dq.Push(m)

	assert_equal(t, 6, dq.partitions[expectPartitionKey].activeSegment.startOffset)

	// now we need to tear apart the individual partition files

	index00, err := os.ReadFile(FormatPathForSegment(testPath, uint32(expectPartitionKey), 0) + ExtIndex)
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

	index01, err := os.ReadFile(FormatPathForSegment(testPath, uint32(expectPartitionKey), 3) + ExtIndex)
	assert_noerror(t, err)

	entries = readIndexEntries(bytes.NewReader(index01))
	assert_equal(t, 3, entries[0].GetOffset())
	assert_equal(t, 0, entries[0].GetOffsetBytes())
	assert_equal(t, messageSize, entries[0].GetSizeBytes())

	assert_equal(t, 4, entries[1].GetOffset())
	assert_equal(t, messageSize, entries[1].GetOffsetBytes())
	assert_equal(t, messageSize, entries[1].GetSizeBytes())

	assert_equal(t, 5, entries[2].GetOffset())
	assert_equal(t, 2*messageSize, entries[2].GetOffsetBytes())
	assert_equal(t, messageSize, entries[2].GetSizeBytes())

	index02, err := os.ReadFile(FormatPathForSegment(testPath, uint32(expectPartitionKey), 6) + ExtIndex)
	assert_noerror(t, err)

	entries = readIndexEntries(bytes.NewReader(index02))
	assert_equal(t, 6, entries[0].GetOffset())
	assert_equal(t, 0, entries[0].GetOffsetBytes())
	assert_equal(t, messageSize, entries[0].GetSizeBytes())

	assert_equal(t, 7, entries[1].GetOffset())
	assert_equal(t, messageSize, entries[1].GetOffsetBytes())
	assert_equal(t, messageSize, entries[1].GetSizeBytes())
}
