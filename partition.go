package diskq

import (
	"fmt"
	"os"
	"sync"
	"time"
)

func NewPartition(path string, cfg Options, partitionIndex uint32) (*Partition, error) {
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

	segment, err := CreateSegment(path, partitionIndex, 0)
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
		cfg:   cfg,
		index: partitionIndex,
	}
	if len(dirEntries) == 0 {
		return nil, fmt.Errorf("diskq; empty partition directory")
	}

	lastSegmentStartOffset, err := parseSegmentOffsetFromPath(dirEntries[len(dirEntries)-1].Name())
	if err != nil {
		return nil, err
	}
	segment, err := OpenSegment(path, partitionIndex, uint64(lastSegmentStartOffset))
	if err != nil {
		return nil, err
	}
	p.activeSegment = segment
	return p, nil
}

type Partition struct {
	mu            sync.Mutex
	path          string
	cfg           Options
	index         uint32
	activeSegment *Segment
}

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
// by the configuration fields RetentionMaxBytes and RetentionMaxAge.
func (p *Partition) Vacuum() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cfg.RetentionMaxAge == 0 && p.cfg.RetentionMaxBytes == 0 {
		return nil
	}
	if p.activeSegment == nil || p.activeSegment.data == nil {
		return nil
	}

	segmentOffsets, err := GetPartitionSegmentStartOffsets(p.path, p.index)
	if err != nil {
		return err
	}

	activeOffset := p.activeSegment.startOffset

	for _, startOffset := range segmentOffsets {
		if startOffset == activeOffset {
			return nil
		}
		if p.cfg.RetentionMaxAge > 0 {
			doVacuumSegment, err := p.shouldVacuumSegmentByAge(startOffset)
			if err != nil {
				return err
			}
			if doVacuumSegment {
				if err := p.vacuumSegment(startOffset); err != nil {
					return err
				}
				continue
			}
		}
		if p.cfg.RetentionMaxBytes > 0 {
			partitionSizeBytes, err := GetPartitionSizeBytes(p.path, p.index)
			if err != nil {
				return err
			}
			if partitionSizeBytes > p.cfg.RetentionMaxBytes {
				if err := p.vacuumSegment(startOffset); err != nil {
					return err
				}
				continue
			}
		}
	}
	return nil
}

// Sync calls fsync on the underlying handles
// of the active segment for the partition.
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
	return int64(segment.endOffsetBytes) >= p.cfg.SegmentSizeBytesOrDefault()
}

func (p *Partition) closeActiveSegmentUnsafe() error {
	newActive, err := CreateSegment(p.path, p.index, p.activeSegment.endOffset)
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
	var endTimestamp time.Time
	endTimestamp, err = GetSegmentOldestTimestamp(p.path, p.index, startOffset)
	if err != nil {
		return
	}
	cutoff := time.Now().UTC().Add(-p.cfg.RetentionMaxAge)
	doVacuumSegment = endTimestamp.Before(cutoff)
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
