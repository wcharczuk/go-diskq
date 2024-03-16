package diskq

import (
	"testing"
	"time"
)

func Test_GetOffsetAfter_empty(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             testPath,
		SegmentSizeBytes: 32 * 1024 * 1024, // 32mb
	}

	p, err := createPartition(cfg, 00)
	assert_noerror(t, err)
	assert_notnil(t, p)

	offsets, err := getPartitionSegmentOffsets(cfg.Path, 0)
	assert_noerror(t, err)
	assert_equal(t, 1, len(offsets))

	foundOffset, found, err := GetOffsetAfter(cfg.Path, 0, time.Date(2024, 01, 02, 12, 9, 10, 9, time.UTC))
	assert_noerror(t, err)
	assert_equal(t, false, found)
	assert_equal(t, 0, foundOffset)
}

func Test_GetOffsetAfter_first(t *testing.T) {
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
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 12, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	err = p.closeActiveSegmentUnsafe()
	assert_noerror(t, err)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 13, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
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

	err = p.closeActiveSegmentUnsafe()
	assert_noerror(t, err)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 16, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 17, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 18, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	offsets, err := getPartitionSegmentOffsets(cfg.Path, 0)
	assert_noerror(t, err)
	assert_equal(t, 3, len(offsets))

	foundOffset, found, err := GetOffsetAfter(cfg.Path, 0, time.Date(2024, 01, 02, 12, 9, 10, 9, time.UTC))
	assert_noerror(t, err)
	assert_equal(t, true, found)
	assert_equal(t, 0, foundOffset)
}

func Test_GetOffsetAfter_beyondNewest(t *testing.T) {
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
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 12, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	err = p.closeActiveSegmentUnsafe()
	assert_noerror(t, err)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 13, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
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

	err = p.closeActiveSegmentUnsafe()
	assert_noerror(t, err)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 16, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 17, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 18, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	offsets, err := getPartitionSegmentOffsets(cfg.Path, 0)
	assert_noerror(t, err)
	assert_equal(t, 3, len(offsets))

	foundOffset, found, err := GetOffsetAfter(cfg.Path, 0, time.Date(2024, 01, 02, 12, 19, 10, 9, time.UTC))
	assert_noerror(t, err)
	assert_equal(t, false, found)
	assert_equal(t, 0, foundOffset)
}

func Test_GetOffsetAfter_equalsOldest(t *testing.T) {
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
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 12, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	err = p.closeActiveSegmentUnsafe()
	assert_noerror(t, err)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 13, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
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

	err = p.closeActiveSegmentUnsafe()
	assert_noerror(t, err)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 16, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 17, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 18, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	offsets, err := getPartitionSegmentOffsets(cfg.Path, 0)
	assert_noerror(t, err)
	assert_equal(t, 3, len(offsets))

	foundOffset, found, err := GetOffsetAfter(cfg.Path, 0, time.Date(2024, 01, 02, 12, 13, 10, 9, time.UTC))
	assert_noerror(t, err)
	assert_equal(t, true, found)
	assert_equal(t, 4, foundOffset)
}

func Test_GetOffsetAfter_equalsNewest(t *testing.T) {
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
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 12, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	err = p.closeActiveSegmentUnsafe()
	assert_noerror(t, err)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 13, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
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

	err = p.closeActiveSegmentUnsafe()
	assert_noerror(t, err)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 16, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 17, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 18, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	offsets, err := getPartitionSegmentOffsets(cfg.Path, 0)
	assert_noerror(t, err)
	assert_equal(t, 3, len(offsets))

	foundOffset, found, err := GetOffsetAfter(cfg.Path, 0, time.Date(2024, 01, 02, 12, 15, 10, 9, time.UTC))
	assert_noerror(t, err)
	assert_equal(t, true, found)
	assert_equal(t, 6, foundOffset)
}
