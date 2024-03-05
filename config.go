package diskq

import (
	"time"
)

// Config are the options for the diskq.
type Config struct {
	Path              string
	PartitionCount    uint32
	RetentionMaxBytes int64
	RetentionMaxAge   time.Duration
	SegmentSizeBytes  int64
}
