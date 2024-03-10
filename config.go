package diskq

import (
	"fmt"
	"time"
)

const (
	defaultPartitionCount   = 3
	defaultSegmentSizeBytes = 32 * 1024 * 1024 // 32mb
)

// Config are the options for the diskq.
type Config struct {
	Path              string
	PartitionCount    uint32
	RetentionMaxBytes int64
	RetentionMaxAge   time.Duration
	SegmentSizeBytes  int64
}

func (c Config) Validate() error {
	if c.Path == "" {
		return fmt.Errorf("diskq; config validation; `Path` is required")
	}
	return nil
}

func (c Config) PartitionCountOrDefault() uint32 {
	if c.PartitionCount > 0 {
		return c.PartitionCount
	}
	return defaultPartitionCount
}

func (c Config) SegmentSizeBytesOrDefault() int64 {
	if c.SegmentSizeBytes > 0 {
		return c.SegmentSizeBytes
	}
	return defaultSegmentSizeBytes
}
