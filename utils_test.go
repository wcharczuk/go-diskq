package diskq

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func Test_getSegmentOffsets(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Options{
		SegmentSizeBytes: 1024,
	}

	p, err := createPartition(testPath, cfg, 01)
	assert_noerror(t, err)
	assert_notnil(t, p)

	for x := 0; x < 32; x++ {
		_, err = p.Write(Message{
			Data: []byte(strings.Repeat("a", 64)),
		})
		assert_noerror(t, err)
	}

	offsets, err := GetPartitionSegmentStartOffsets(testPath, 01)
	assert_noerror(t, err)
	assert_equal(t, 3, len(offsets))
	assert_equal(t, []uint64{0, 13, 26}, offsets)
}

func Test_getSegmentNewestOffset(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Options{
		SegmentSizeBytes: 1024,
	}

	p, err := createPartition(testPath, cfg, 01)
	assert_noerror(t, err)
	assert_notnil(t, p)

	for x := 0; x < 32; x++ {
		_, err = p.Write(Message{
			Data: []byte(strings.Repeat("a", 64)),
		})
		assert_noerror(t, err)
	}
	offsets, err := GetPartitionSegmentStartOffsets(testPath, 01)
	assert_noerror(t, err)

	offset, err := GetSegmentNewestOffset(testPath, 01, offsets[len(offsets)-1])
	assert_noerror(t, err)
	assert_equal(t, 31, offset)
}

func Test_getSegmentNewestOffset_empty(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	segmentPath := FormatPathForSegment(testPath, 1, 0)
	_ = os.MkdirAll(filepath.Dir(segmentPath), 0755)

	f, err := os.Create(segmentPath + ExtIndex)
	assert_noerror(t, err)
	assert_noerror(t, f.Close())

	offset, err := GetSegmentNewestOffset(testPath, 01, 0)
	assert_noerror(t, err)
	assert_equal(t, 0, offset)
}

func Test_getSegmentOldestTimestamp(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Options{
		SegmentSizeBytes: 32 * 1024 * 1024, // 32mb
	}

	p, err := createPartition(testPath, cfg, 00)
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

	offsets, err := GetPartitionSegmentStartOffsets(testPath, 0)
	assert_noerror(t, err)
	assert_equal(t, 3, len(offsets))

	endTimestamp, err := GetSegmentOldestTimestamp(testPath, 0, offsets[1])
	assert_noerror(t, err)
	assert_equal(t, time.Date(2024, 01, 02, 12, 12, 10, 9, time.UTC), endTimestamp)
}

func Test_getSegmentNewestTimestamp(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Options{
		SegmentSizeBytes: 32 * 1024 * 1024, // 32mb
	}

	p, err := createPartition(testPath, cfg, 00)
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

	offsets, err := GetPartitionSegmentStartOffsets(testPath, 0)
	assert_noerror(t, err)
	assert_equal(t, 3, len(offsets))

	endTimestamp, err := GetSegmentNewestTimestamp(testPath, 0, offsets[1])
	assert_noerror(t, err)
	assert_equal(t, time.Date(2024, 01, 02, 12, 13, 10, 9, time.UTC), endTimestamp)

	endTimestamp, err = GetSegmentNewestTimestamp(testPath, 0, offsets[len(offsets)-1])
	assert_noerror(t, err)
	assert_equal(t, time.Date(2024, 01, 02, 12, 15, 10, 9, time.UTC), endTimestamp)
}

func Test_getSegmentNewestOldestTimestamps(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Options{
		SegmentSizeBytes: 32 * 1024 * 1024, // 32mb
	}

	p, err := createPartition(testPath, cfg, 00)
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

	offsets, err := GetPartitionSegmentStartOffsets(testPath, 0)
	assert_noerror(t, err)
	assert_equal(t, 3, len(offsets))

	oldest, newest, err := GetSegmentOldestNewestTimestamps(testPath, 0, offsets[1])
	assert_noerror(t, err)
	assert_equal(t, time.Date(2024, 01, 02, 12, 12, 10, 9, time.UTC), oldest)
	assert_equal(t, time.Date(2024, 01, 02, 12, 13, 10, 9, time.UTC), newest)
}

func Test_getSegmentNewestOldestTimestamps_empty(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Options{
		SegmentSizeBytes: 32 * 1024 * 1024, // 32mb
	}

	p, err := createPartition(testPath, cfg, 00)
	assert_noerror(t, err)
	assert_notnil(t, p)

	offsets, err := GetPartitionSegmentStartOffsets(testPath, 0)
	assert_noerror(t, err)
	assert_equal(t, 1, len(offsets))

	oldest, newest, err := GetSegmentOldestNewestTimestamps(testPath, 0, offsets[0])
	assert_noerror(t, err)
	assert_equal(t, true, oldest.IsZero())
	assert_equal(t, true, newest.IsZero())
}

func Test_getSegmentNewestOldestTimestamps_single(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Options{
		SegmentSizeBytes: 32 * 1024 * 1024, // 32mb
	}

	p, err := createPartition(testPath, cfg, 00)
	assert_noerror(t, err)
	assert_notnil(t, p)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 10, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	offsets, err := GetPartitionSegmentStartOffsets(testPath, 0)
	assert_noerror(t, err)
	assert_equal(t, 1, len(offsets))

	oldest, newest, err := GetSegmentOldestNewestTimestamps(testPath, 0, offsets[0])
	assert_noerror(t, err)
	assert_equal(t, false, oldest.IsZero())
	assert_equal(t, false, newest.IsZero())
	assert_equal(t, time.Date(2024, 01, 02, 12, 10, 10, 9, time.UTC), oldest)
	assert_equal(t, time.Date(2024, 01, 02, 12, 10, 10, 9, time.UTC), newest)
}

func Test_getPartitionSizeBytes(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	m := Message{
		PartitionKey: "aaa",
		TimestampUTC: time.Date(2024, 01, 02, 12, 11, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	}
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

	sizeBytes, err := GetPartitionSizeBytes(testPath, 0, false /*skipActiveSegment*/)
	assert_noerror(t, err)
	assert_equal(t, 4*messageSize, sizeBytes)

	sizeBytes, err = GetPartitionSizeBytes(testPath, 0, true /*skipActiveSegment*/)
	assert_noerror(t, err)
	assert_equal(t, 3*messageSize, sizeBytes)
}
