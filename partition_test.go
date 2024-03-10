package diskq

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func Test_getSegmentOffsets(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             testPath,
		SegmentSizeBytes: 1024,
	}

	p, err := createPartition(cfg, 01)
	assert_noerror(t, err)
	assert_notnil(t, p)

	for x := 0; x < 32; x++ {
		_, err = p.Write(Message{
			Data: []byte(strings.Repeat("a", 64)),
		})
		assert_noerror(t, err)
	}

	offsets, err := getPartitionSegmentOffsets(cfg, 01)
	assert_noerror(t, err)
	assert_equal(t, 3, len(offsets))
	assert_equal(t, []uint64{0, 13, 26}, offsets)
}

func Test_getSegmentEndTimestamp(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             testPath,
		SegmentSizeBytes: 32 * 1024 * 1024, // 32mb
	}

	p, err := createPartition(cfg, 00)
	assert_noerror(t, err)
	assert_notnil(t, p)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 10, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 11, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	err = p.closeActiveSegmentUnsafe()
	assert_noerror(t, err)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 12, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 13, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	err = p.closeActiveSegmentUnsafe()
	assert_noerror(t, err)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 14, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 15, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	offsets, err := getPartitionSegmentOffsets(cfg, 0)
	assert_noerror(t, err)
	assert_equal(t, 3, len(offsets))

	endTimestamp, err := getSegmentEndTimestamp(cfg, 0, offsets[1])
	assert_noerror(t, err)
	assert_equal(t, time.Date(2024, 01, 02, 12, 12, 10, 9, time.UTC), endTimestamp)
}

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

	dq.Push(m)
	dq.Push(m)
	dq.Push(m)
	dq.Push(m)

	// expect a new active segment
	assert_equal(t, 4, dq.partitions[expectPartitionKey].activeSegment.startOffset)

	dq.Push(m)
	dq.Push(m)
	dq.Push(m)
	dq.Push(m)

	// now we need to tear apart the individual partition files

	index00, err := os.ReadFile(formatPathForSegment(cfg, uint32(expectPartitionKey), 0) + extIndex)
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

	index01, err := os.ReadFile(formatPathForSegment(cfg, uint32(expectPartitionKey), 4) + extIndex)
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

func readIndexEntries(r io.Reader) (output []segmentIndex) {
	for {
		var si segmentIndex
		err := binary.Read(r, binary.LittleEndian, &si)
		if err == io.EOF {
			return
		}
		output = append(output, si)
	}
}

func messageSizeBytes(m Message) int64 {
	data := new(bytes.Buffer)
	Encode(m, data)
	return int64(data.Len())
}
