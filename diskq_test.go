package diskq

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func Test_Diskq_create(t *testing.T) {
	tempPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             tempPath,
		PartitionCount:   3,
		SegmentSizeBytes: 1024, // 1kb
	}

	dq, err := New(cfg)
	assert_noerror(t, err)

	partitionOffsets := make(map[uint32]map[uint64]struct{})
	for partitionIndex := 0; partitionIndex < int(cfg.PartitionCount); partitionIndex++ {
		partitionOffsets[uint32(partitionIndex)] = make(map[uint64]struct{})
	}

	for x := 0; x < 100; x++ {
		m := Message{
			PartitionKey: UUIDv4().String(),
			Data:         []byte(fmt.Sprintf("data-%06d", x)),
		}
		partition, offset, err := dq.Push(m)
		assert_noerror(t, err)
		_, ok := partitionOffsets[partition][offset]
		assert_equal(t, false, ok)
		partitionOffsets[partition][offset] = struct{}{}
	}

	dirEntries, err := os.ReadDir(tempPath)
	assert_noerror(t, err)
	assert_equal(t, 2, len(dirEntries))

	dirEntries, err = os.ReadDir(formatPathForPartitions(tempPath))
	assert_noerror(t, err)
	assert_equal(t, 3, len(dirEntries))
}

func Test_Diskq_sentinelFailure(t *testing.T) {
	tempPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             tempPath,
		PartitionCount:   3,
		SegmentSizeBytes: 1024, // 1kb
	}

	_, err := New(cfg)
	assert_noerror(t, err)

	_, err = New(cfg)
	assert_error(t, err)
}

func Test_Diskq_create_thenOpen(t *testing.T) {
	tempPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             tempPath,
		PartitionCount:   3,
		SegmentSizeBytes: 1024, // 1kb
	}

	func() {
		dq, err := New(cfg)
		assert_noerror(t, err)
		defer func() { _ = dq.Close() }()

		partitionOffsets := make(map[uint32]map[uint64]struct{})
		for partitionIndex := 0; partitionIndex < int(cfg.PartitionCount); partitionIndex++ {
			partitionOffsets[uint32(partitionIndex)] = make(map[uint64]struct{})
		}

		var partitionKeys = []string{"aaa", "bbb", "ccc"}
		for x := 0; x < 100; x++ {
			m := Message{
				PartitionKey: partitionKeys[x%len(partitionKeys)],
				Data:         []byte(fmt.Sprintf("data-%06d", x)),
			}
			partition, offset, err := dq.Push(m)
			assert_noerror(t, err)
			_, ok := partitionOffsets[partition][offset]
			assert_equal(t, false, ok)
			partitionOffsets[partition][offset] = struct{}{}
		}

		dirEntries, err := os.ReadDir(tempPath)
		assert_noerror(t, err)
		assert_equal(t, 2, len(dirEntries))

		dirEntries, err = os.ReadDir(formatPathForPartitions(tempPath))
		assert_noerror(t, err)
		assert_equal(t, 3, len(dirEntries))
	}()

	dq, err := New(cfg)
	assert_noerror(t, err)
	defer func() { _ = dq.Close() }()

	assert_equal(t, 3, len(dq.partitions))

	assert_equal(t, 31, dq.partitions[0].activeSegment.startOffset)
	assert_equal(t, 34, dq.partitions[0].activeSegment.endOffset)
	assert_equal(t, 31, dq.partitions[1].activeSegment.startOffset)
	assert_equal(t, 33, dq.partitions[1].activeSegment.endOffset)
	assert_equal(t, 31, dq.partitions[2].activeSegment.startOffset)
	assert_equal(t, 33, dq.partitions[2].activeSegment.endOffset)
}

func Test_Diskq_Sync(t *testing.T) {
	tempPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             tempPath,
		PartitionCount:   3,
		SegmentSizeBytes: 1024, // 1kb
	}

	dq, err := New(cfg)
	assert_noerror(t, err)

	partitionOffsets := make(map[uint32]map[uint64]struct{})
	for partitionIndex := 0; partitionIndex < int(cfg.PartitionCount); partitionIndex++ {
		partitionOffsets[uint32(partitionIndex)] = make(map[uint64]struct{})
	}

	for x := 0; x < 100; x++ {
		m := Message{
			PartitionKey: UUIDv4().String(),
			Data:         []byte(fmt.Sprintf("data-%06d", x)),
		}
		partition, offset, err := dq.Push(m)
		assert_noerror(t, err)
		_, ok := partitionOffsets[partition][offset]
		assert_equal(t, false, ok)
		partitionOffsets[partition][offset] = struct{}{}
	}

	err = dq.Sync()
	assert_noerror(t, err)
}

func Test_Diskq_createsNewSegments(t *testing.T) {
	tempPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             tempPath,
		PartitionCount:   3,
		SegmentSizeBytes: 1024, // 1kb
	}

	dq, err := New(cfg)
	assert_noerror(t, err)

	for x := 0; x < 11; x++ {
		// each message ends up being about 62b
		m := Message{
			PartitionKey: "one",
			Data:         []byte(strings.Repeat("a", 32)),
		}
		_, _, err := dq.Push(m)
		assert_noerror(t, err)
	}

	dirEntries, err := os.ReadDir(tempPath)
	assert_noerror(t, err)
	assert_equal(t, 2, len(dirEntries))

	dirEntries, err = os.ReadDir(formatPathForPartitions(tempPath))
	assert_noerror(t, err)
	assert_equal(t, 3, len(dirEntries))

	partitionIndex := dq.partitionForMessage(Message{PartitionKey: "one"}).index

	partitionDirEntries, err := os.ReadDir(formatPathForPartition(cfg.Path, partitionIndex))
	assert_noerror(t, err)
	assert_equal(t, 3, len(partitionDirEntries))

	for x := 0; x < 12; x++ {
		m := Message{
			PartitionKey: "one",
			Data:         []byte(strings.Repeat("a", 32)),
		}
		_, _, err := dq.Push(m)
		assert_noerror(t, err)
	}

	partitionDirEntries, err = os.ReadDir(formatPathForPartition(cfg.Path, partitionIndex))
	assert_noerror(t, err)
	assert_equal(t, 6, len(partitionDirEntries))
}

func Test_Diskq_GetOffset(t *testing.T) {
	tempPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             tempPath,
		PartitionCount:   3,
		SegmentSizeBytes: 1024, // 1kb
	}

	dq, err := New(cfg)
	assert_noerror(t, err)

	partitionOffsets := make(map[uint32]map[uint64]struct{})
	for partitionIndex := 0; partitionIndex < int(cfg.PartitionCount); partitionIndex++ {
		partitionOffsets[uint32(partitionIndex)] = make(map[uint64]struct{})
	}

	for x := 0; x < 100; x++ {
		m := Message{
			PartitionKey: UUIDv4().String(),
			Data:         []byte(fmt.Sprintf("data-%06d", x)),
		}
		partition, offset, err := dq.Push(m)
		assert_noerror(t, err)
		_, ok := partitionOffsets[partition][offset]
		assert_equal(t, false, ok)
		partitionOffsets[partition][offset] = struct{}{}
	}

	var randomPartitionIndex uint32
	var randomPartitionOffsets map[uint64]struct{}
	for randomPartitionIndex, randomPartitionOffsets = range partitionOffsets {
		break
	}

	var randomOffset uint64
	for randomOffset = range randomPartitionOffsets {
		break
	}

	message, ok, err := dq.GetOffset(randomPartitionIndex, randomOffset)
	assert_noerror(t, err)
	assert_equal(t, true, ok)
	assert_equal(t, true, strings.HasPrefix(string(message.Data), "data-"))
}

func Test_Diskq_Vacuum_byAge(t *testing.T) {
	tempPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             tempPath,
		PartitionCount:   3,
		SegmentSizeBytes: 1024, // 1kb
		RetentionMaxAge:  32 * time.Hour,
	}

	dq, err := New(cfg)
	assert_noerror(t, err)

	var partitionKeys = []string{"aaa", "bbb", "ccc"}
	var index int
	hour := 64
	for x := 0; x < 64*3; x++ {
		m := Message{
			PartitionKey: partitionKeys[index],
			Data:         []byte(strings.Repeat("a", 32)),
			TimestampUTC: time.Now().UTC().Add(-time.Duration(hour) * time.Hour),
		}

		_, _, err := dq.Push(m)
		assert_noerror(t, err)
		index = (index + 1) % len(partitionKeys)
		if index == 0 {
			hour--
		}
	}

	offsets, err := getPartitionSegmentOffsets(cfg.Path, 0)
	assert_noerror(t, err)
	assert_equal(t, 4, len(offsets))

	offsets, err = getPartitionSegmentOffsets(cfg.Path, 1)
	assert_noerror(t, err)
	assert_equal(t, 4, len(offsets))

	offsets, err = getPartitionSegmentOffsets(cfg.Path, 2)
	assert_noerror(t, err)
	assert_equal(t, 4, len(offsets))

	err = dq.Vacuum()
	assert_noerror(t, err)

	offsets, err = getPartitionSegmentOffsets(cfg.Path, 0)
	assert_noerror(t, err)
	assert_equal(t, 2, len(offsets))

	offsets, err = getPartitionSegmentOffsets(cfg.Path, 1)
	assert_noerror(t, err)
	assert_equal(t, 2, len(offsets))

	offsets, err = getPartitionSegmentOffsets(cfg.Path, 2)
	assert_noerror(t, err)
	assert_equal(t, 2, len(offsets))
}

func Test_Diskq_Vacuum_bySize(t *testing.T) {
	tempPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:              tempPath,
		PartitionCount:    3,
		SegmentSizeBytes:  1024, // 1kb
		RetentionMaxBytes: 4096, // 4kb
	}

	dq, err := New(cfg)
	assert_noerror(t, err)

	var partitionKeys = []string{"aaa", "bbb", "ccc"}
	var index int
	for x := 0; x < 64*3; x++ {
		m := Message{
			PartitionKey: partitionKeys[index],
			Data:         []byte(strings.Repeat("a", 256)),
		}

		_, _, err := dq.Push(m)
		assert_noerror(t, err)
		index = (index + 1) % len(partitionKeys)
	}

	offsets, err := getPartitionSegmentOffsets(cfg.Path, 0)
	assert_noerror(t, err)
	assert_equal(t, 17, len(offsets))

	offsets, err = getPartitionSegmentOffsets(cfg.Path, 1)
	assert_noerror(t, err)
	assert_equal(t, 17, len(offsets))

	offsets, err = getPartitionSegmentOffsets(cfg.Path, 2)
	assert_noerror(t, err)
	assert_equal(t, 17, len(offsets))

	err = dq.Vacuum()
	assert_noerror(t, err)

	offsets, err = getPartitionSegmentOffsets(cfg.Path, 0)
	assert_noerror(t, err)
	assert_equal(t, 4, len(offsets))

	offsets, err = getPartitionSegmentOffsets(cfg.Path, 1)
	assert_noerror(t, err)
	assert_equal(t, 4, len(offsets))

	offsets, err = getPartitionSegmentOffsets(cfg.Path, 2)
	assert_noerror(t, err)
	assert_equal(t, 4, len(offsets))
}
