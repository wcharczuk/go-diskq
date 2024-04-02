package diskq

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func Test_Stats(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Options{
		PartitionCount:   3,
		SegmentSizeBytes: 1024, // 1kb
	}

	dq, err := New(testPath, cfg)
	assert_noerror(t, err)

	now := time.Now().UTC()

	for x := 0; x < 100; x++ {
		m := Message{
			PartitionKey: fmt.Sprintf("data-%06d", x),
			Data:         []byte(strings.Repeat("a", 64)),
			TimestampUTC: now.Add(-time.Duration(100-x) * time.Second),
		}
		_, _, err := dq.Push(m)
		assert_noerror(t, err)
	}

	stats, err := GetStats(testPath)
	assert_noerror(t, err)
	assert_equal(t, true, stats.InUse)
	assert_equal(t, testPath, stats.Path)
	assert_equal(t, 3, len(stats.Partitions))
	assert_equal(t, 9500, stats.SizeBytes)
	assert_equal(t, 98*time.Second, stats.Age)

	err = dq.Close()
	assert_noerror(t, err)

	stats, err = GetStats(testPath)
	assert_noerror(t, err)
	assert_equal(t, false, stats.InUse)
	assert_equal(t, testPath, stats.Path)
	assert_equal(t, 3, len(stats.Partitions))
	assert_equal(t, 9500, stats.SizeBytes)
	assert_equal(t, 98*time.Second, stats.Age)
}
