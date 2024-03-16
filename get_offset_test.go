package diskq

import (
	"fmt"
	"strings"
	"testing"
)

func Test_GetOffset(t *testing.T) {
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

	message, ok, err := GetOffset(tempPath, randomPartitionIndex, randomOffset)
	assert_noerror(t, err)
	assert_equal(t, true, ok)
	assert_equal(t, true, strings.HasPrefix(string(message.Data), "data-"))
}
