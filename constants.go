package diskq

import (
	"fmt"
	"strings"
)

const (
	// ExtData is the extension of segment data files, including the leading dot.
	ExtData = ".data"
	// ExtIndex is the extension of segment index files, including the leading dot.
	ExtIndex = ".index"
	// ExtTimeIndex is the extension of segment timeindex files, including the leading dot.
	ExtTimeIndex = ".timeindex"
)

// ConsumerStartBehavior controls how the consumer determines the
// first offset it will read from.
type ConsumerStartBehavior uint8

const (
	// ConsumerStartBehaviorOldest is a consumer start behavior that
	// starts a consumer at the absolute oldest offset present on disk.
	ConsumerStartBehaviorOldest ConsumerStartBehavior = iota
	// ConsumerStartBehaviorAtOffset is a consumer start behavior that
	// starts a consumer at a given offset.
	ConsumerStartBehaviorAtOffset
	// ConsumerStartBehaviorActiveSegmentOldest is a consumer start behavior that
	// starts a consumer at the beginning of the active segment for the partition.
	ConsumerStartBehaviorActiveSegmentOldest
	// ConsumerStartBehaviorNewest is a consumer start behavior that
	// starts a consumer at the end of the active segment, or the newest offset.
	ConsumerStartBehaviorNewest
)

// String returns a string form of the consumer start behavior.
func (csb ConsumerStartBehavior) String() string {
	switch csb {
	case ConsumerStartBehaviorOldest:
		return "oldest"
	case ConsumerStartBehaviorAtOffset:
		return "at-offset"
	case ConsumerStartBehaviorActiveSegmentOldest:
		return "active-oldest"
	case ConsumerStartBehaviorNewest:
		return "newest"
	default:
		return ""
	}
}

// ParseConsumerStartBehavior parses a raw string as a consumer start behavior.
func ParseConsumerStartBehavior(raw string) (startBehavior ConsumerStartBehavior, err error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "oldest":
		startBehavior = ConsumerStartBehaviorOldest
	case "at-offset":
		startBehavior = ConsumerStartBehaviorAtOffset
	case "active-oldest":
		startBehavior = ConsumerStartBehaviorActiveSegmentOldest
	case "newest":
		startBehavior = ConsumerStartBehaviorNewest
	default:
		err = fmt.Errorf("absurd consumer start behavior: %s", raw)
	}
	return
}

// ConsumerEndBehavior controls how the consumer behaves when the
// last offset is read in the active segment.
type ConsumerEndBehavior uint8

const (
	// ConsumerEndBehaviorWait is a consumer end behavior that
	// will cause the consumer to wait for new offsets when it reaches
	// the end of the active segment.
	ConsumerEndBehaviorWait ConsumerEndBehavior = iota
	// ConsumerEndBehaviorAtOffset is a consumer end behavior that
	// will cause the consumer to exit when it reaches a given offset.
	ConsumerEndBehaviorAtOffset
	// ConsumerEndBehaviorAtOffset is a consumer end behavior that
	// will cause the consumer to exit when it reaches the
	// end of the active segment.
	ConsumerEndBehaviorClose
)

// String returns a string form of the consumer end behavior.
func (ceb ConsumerEndBehavior) String() string {
	switch ceb {
	case ConsumerEndBehaviorWait:
		return "wait"
	case ConsumerEndBehaviorAtOffset:
		return "at-offset"
	case ConsumerEndBehaviorClose:
		return "close"
	default:
		return ""
	}
}

// ParseConsumerEndBehavior parses a given raw string as a consumer end behavior.
func ParseConsumerEndBehavior(raw string) (endBehavior ConsumerEndBehavior, err error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "wait":
		endBehavior = ConsumerEndBehaviorWait
	case "at-offset":
		endBehavior = ConsumerEndBehaviorAtOffset
	case "close":
		endBehavior = ConsumerEndBehaviorClose
	default:
		err = fmt.Errorf("absurd consumer end behavior: %s", raw)
	}
	return
}
