package diskq

import (
	"fmt"
	"os"
	"sync"
	"time"
)

func createOrOpenPartition(path string, cfg Options, partitionIndex uint32) (*Partition, error) {
	intendedPath := FormatPathForPartition(path, partitionIndex)
	if _, err := os.Stat(intendedPath); err != nil {
		return createPartition(path, cfg, partitionIndex)
	}
	return openPartition(path, cfg, partitionIndex)
}

func createPartition(path string, cfg Options, partitionIndex uint32) (*Partition, error) {
	intendedPath := FormatPathForPartition(path, partitionIndex)
	if err := os.MkdirAll(intendedPath, 0755); err != nil {
		return nil, fmt.Errorf("diskq; create partition; cannot create intended path: %w", err)
	}

	segment, err := createSegment(path, partitionIndex, 0)
	if err != nil {
		return nil, err
	}
	return &Partition{
		path:          path,
		cfg:           cfg,
		index:         partitionIndex,
		activeSegment: segment,
	}, nil
}

func openPartition(path string, cfg Options, partitionIndex uint32) (*Partition, error) {
	intendedPath := FormatPathForPartition(path, partitionIndex)
	dirEntries, err := os.ReadDir(intendedPath)
	if err != nil {
		return nil, fmt.Errorf("diskq; open partition; cannot read intended path: %w", err)
	}
	p := &Partition{
		path:  path,
		cfg:   cfg,
		index: partitionIndex,
	}
	if len(dirEntries) == 0 {
		return nil, fmt.Errorf("diskq; empty partition directory")
	}

	lastSegmentStartOffset, err := ParseSegmentOffsetFromPath(dirEntries[len(dirEntries)-1].Name())
	if err != nil {
		return nil, err
	}
	segment, err := openSegment(path, partitionIndex, uint64(lastSegmentStartOffset))
	if err != nil {
		return nil, err
	}
	p.activeSegment = segment
	return p, nil
}

// Partition organizes the time ordered series of messages that make up a stream.
//
// The partition is responsible for rotating which segment it's writing to actively,
// as well a partition is responsible for implementing the vacuum steps for itself.
type Partition struct {
	mu            sync.Mutex
	path          string
	cfg           Options
	index         uint32
	activeSegment *Segment
}

// Write writes a given message to the active segment of the partition.
//
// You will almost never need to call this directly, instead call [Diskq.Push].
func (p *Partition) Write(message Message) (offset uint64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset, err = p.activeSegment.writeUnsafe(message); err != nil {
		return
	}
	if p.shouldCloseActiveSegmentUnsafe(p.activeSegment) {
		if err = p.closeActiveSegmentUnsafe(); err != nil {
			return
		}
	}
	return
}

// Vacuum removes old segments from the partitions as defined
// by the diskq [Options] fields [Options.RetentionMaxBytes] and [Options.RetentionMaxAge].
//
// You will almost never need to call this function directly, and should instead
// call [Diskq.Vacuum].
func (p *Partition) Vacuum() error {
	if p.cfg.RetentionMaxAge == 0 && p.cfg.RetentionMaxBytes == 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.activeSegment == nil || p.activeSegment.data == nil {
		return nil
	}

	segmentOffsets, err := GetPartitionSegmentStartOffsets(p.path, p.index)
	if err != nil {
		return fmt.Errorf("diskq; partition; cannot get segment offsets: %w", err)
	}

	activeOffset := p.activeSegment.startOffset

	for _, startOffset := range segmentOffsets {
		if startOffset == activeOffset {
			return nil
		}
		if p.cfg.RetentionMaxAge > 0 {
			doVacuumSegment, err := p.shouldVacuumSegmentByAge(startOffset)
			if err != nil {
				return fmt.Errorf("diskq; partition; should vacuum segment by age error: %w", err)
			}
			if doVacuumSegment {
				if err := p.vacuumSegment(startOffset); err != nil {
					return fmt.Errorf("diskq; partition; cannot vacuum offset %d (by age): %w", startOffset, err)
				}
				continue
			}
		}
		if p.cfg.RetentionMaxBytes > 0 {
			partitionSizeBytes, err := GetPartitionSizeBytes(p.path, p.index, true /*skipActiveSegment*/)
			if err != nil {
				return fmt.Errorf("diskq; partition; vacuum; cannot get partition size: %w", err)
			}
			if partitionSizeBytes > p.cfg.RetentionMaxBytes {
				if err := p.vacuumSegment(startOffset); err != nil {
					return fmt.Errorf("diskq; partition; vacuum; cannot vacuum offset %d (by size): %w", startOffset, err)
				}
				continue
			}
		}
	}
	return nil
}

// Sync calls fsync on the underlying active segment file
// handles for the partition.
func (p *Partition) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.activeSegment.Sync()
}

func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activeSegment != nil {
		_ = p.activeSegment.Close()
	}
	return nil
}

//
// internal
//

func (p *Partition) shouldCloseActiveSegmentUnsafe(segment *Segment) bool {
	return int64(segment.endDataBytes) >= p.cfg.SegmentSizeBytesOrDefault()
}

func (p *Partition) closeActiveSegmentUnsafe() error {
	newActive, err := createSegment(p.path, p.index, p.activeSegment.endOffset)
	if err != nil {
		return err
	}
	p.activeSegment = newActive
	return nil
}

//
// vacuum utils
//

func (p *Partition) shouldVacuumSegmentByAge(startOffset uint64) (doVacuumSegment bool, err error) {
	var oldestTimestamp time.Time
	oldestTimestamp, err = GetSegmentOldestTimestamp(p.path, p.index, startOffset)
	if err != nil {
		return
	}
	cutoff := time.Now().UTC().Add(-p.cfg.RetentionMaxAge)
	doVacuumSegment = oldestTimestamp.Before(cutoff)
	return
}

func (p *Partition) vacuumSegment(startOffset uint64) error {
	segmentRoot := FormatPathForSegment(p.path, p.index, startOffset)
	if err := os.Remove(segmentRoot + ExtData); err != nil {
		return err
	}
	if err := os.Remove(segmentRoot + ExtIndex); err != nil {
		return err
	}
	if err := os.Remove(segmentRoot + ExtTimeIndex); err != nil {
		return err
	}
	return nil
}
