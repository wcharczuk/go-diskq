package diskq

import (
	"time"
)

const (
	// DefaultPartitionCount is the default partition count.
	DefaultPartitionCount = 3
	// DefaultSegmentSizeBytes is the default segment size in bytes.
	DefaultSegmentSizeBytes = 32 * 1024 * 1024 // 32mb
)

// Options are the options for the diskq.
type Options struct {
	// PartitionCount is the nubmer of partitions
	// to create the diskq with.
	PartitionCount uint32
	// SegmentSizeBytes is the size of a segement of
	// each partition in bytes.
	//
	// When writing new messages, if the partition exceeds
	// this size a new segment will be created.
	SegmentSizeBytes int64
	// RetentionMaxBytes is the maximum size of a partition in bytes
	// as enforced by calling `Vacuum` on the diskq.
	RetentionMaxBytes int64
	// RetentionMaxAge is the maximum age of messages in a partition
	// as enforced by calling `Vacuum` on the diskq.
	RetentionMaxAge time.Duration
}

// PartitionCountOrDefault returns the partition count or a default value.
//
// The default value is 3 partitions.
func (c Options) PartitionCountOrDefault() uint32 {
	if c.PartitionCount > 0 {
		return c.PartitionCount
	}
	return DefaultPartitionCount
}

func (c Options) SegmentSizeBytesOrDefault() int64 {
	if c.SegmentSizeBytes > 0 {
		return c.SegmentSizeBytes
	}
	return DefaultSegmentSizeBytes
}
