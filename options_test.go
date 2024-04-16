package diskq

import (
	"testing"
	"time"
)

func Test_MaybeReadOptions(t *testing.T) {
	tempPath, done := tempDir()
	t.Cleanup(done)

	cfg := Options{
		PartitionCount:    5,
		SegmentSizeBytes:  1024, // 1kb
		RetentionMaxBytes: 2048,
		RetentionMaxAge:   24 * time.Hour,
	}

	dq, err := New(tempPath, cfg)
	assert_noerror(t, err)

	err = dq.Close()
	assert_noerror(t, err)

	opts, found, err := MaybeReadOptions(tempPath)
	assert_noerror(t, err)
	assert_equal(t, true, found)
	assert_equal(t, 5, opts.PartitionCount)
	assert_equal(t, 1024, opts.SegmentSizeBytes)
	assert_equal(t, 2048, opts.RetentionMaxBytes)
	assert_equal(t, 24*time.Hour, opts.RetentionMaxAge)
}

func Test_MaybeReadOptions_notFound(t *testing.T) {
	tempPath, done := tempDir()
	t.Cleanup(done)

	opts, found, err := MaybeReadOptions(tempPath)
	assert_noerror(t, err)
	assert_equal(t, false, found)
	assert_equal(t, 0, opts.PartitionCount)
	assert_equal(t, 0, opts.SegmentSizeBytes)
	assert_equal(t, 0, opts.RetentionMaxBytes)
	assert_equal(t, 0, opts.RetentionMaxAge)
}
