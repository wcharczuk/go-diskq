package diskq

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

const (
	// DefaultPartitionCount is the default partition count.
	DefaultPartitionCount = 3
	// DefaultSegmentSizeBytes is the default segment size in bytes.
	DefaultSegmentSizeBytes = 32 * 1024 * 1024 // 32mb
)

// MaybeReadOptions tries to read the previous options that were written to
// the default location when a diskq was accessed last.
//
// You can then pass these options to the constructor for the diskq.
func MaybeReadOptions(path string) (cfg Options, found bool, err error) {
	settingsPath := FormatPathForSettings(path)
	if _, statErr := os.Stat(settingsPath); statErr != nil {
		return
	}
	f, err := os.Open(settingsPath)
	if err != nil {
		err = fmt.Errorf("diskq; cannot open settings file: %w", err)
		return
	}
	defer func() { _ = f.Close() }()
	if err = json.NewDecoder(f).Decode(&cfg); err != nil {
		err = fmt.Errorf("diskq; cannot decode settings file: %w", err)
	}
	found = true
	return
}

// Options are the options for the diskq.
type Options struct {
	// PartitionCount is the nubmer of partitions to split data across.
	//
	// Each partition is sized and vacuumed separately, and consumers can be opened
	// against individual partitions.
	//
	// Messages are assigned to partitions based on their partition key, with the goal
	// of an even distribution of messages given a randomized partition key.
	//
	// If unset, a default value of 3 for [Options.PartitionCount] will be used.
	PartitionCount uint32 `json:"partition_count,omitempty"`
	// SegmentSizeBytes is the size in bytes of a segement of each partition.
	//
	// When writing new messages, if the partition message data file exceeds
	// this size a new segment will be created, allowing a partition
	// to be split across multiple files with a general goal for
	// the size of the data file. Because the segment is closed when
	// the data file size exceeds the [Options.SegmentSizeBytes] in practice
	// segments will be slightly larger than the setting when they're closed.
	//
	// These segments are also the atomic unit that is deleted when
	// vacuuming the partition; to have a tigher budget for keeping
	// partitions near a given size, you should use a smaller segment size.
	//
	// If unset, a default value of 32MiB for [Options.SegmentSizeBytes] will be used.
	SegmentSizeBytes int64 `json:"segment_size_bytes,omitempty"`
	// RetentionMaxBytes is the maximum size of a partition in bytes
	// as enforced by calling [Diskq.Vacuum]. The size constraint
	// applies to a single partition, and does not consider the
	// active partition size.
	RetentionMaxBytes int64 `json:"retention_max_bytes,omitempty"`
	// RetentionMaxAge is the maximum age of messages in a partition
	// as enforced by calling [Diskq.Vacuum].
	RetentionMaxAge time.Duration `json:"retention_max_age,omitempty"`
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

// SegmentSizeBytesOrDefault returns the partition segment size
// in bytes or a default.
//
// The default value is 32MiB.
func (c Options) SegmentSizeBytesOrDefault() int64 {
	if c.SegmentSizeBytes > 0 {
		return c.SegmentSizeBytes
	}
	return DefaultSegmentSizeBytes
}
