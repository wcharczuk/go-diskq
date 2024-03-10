package diskq

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"
	"time"
)

func NewPartition(cfg Config, partitionIndex uint32) (*Partition, error) {
	intendedPath := formatPathForPartition(cfg, partitionIndex)
	if _, err := os.Stat(intendedPath); err != nil {
		return createPartition(cfg, partitionIndex)
	}
	return openPartition(cfg, partitionIndex)
}

func createPartition(cfg Config, partitionIndex uint32) (*Partition, error) {
	intendedPath := formatPathForPartition(cfg, partitionIndex)
	if err := os.MkdirAll(intendedPath, 0755); err != nil {
		return nil, fmt.Errorf("diskq; create partition; cannot create intended path: %w", err)
	}

	segment, err := CreateSegment(cfg, partitionIndex, 0)
	if err != nil {
		return nil, err
	}
	return &Partition{
		cfg:           cfg,
		index:         partitionIndex,
		activeSegment: segment,
	}, nil
}

func openPartition(cfg Config, partitionIndex uint32) (*Partition, error) {
	intendedPath := formatPathForPartition(cfg, partitionIndex)
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
	segment, err := OpenSegment(cfg, partitionIndex, uint64(lastSegmentStartOffset))
	if err != nil {
		return nil, err
	}
	p.activeSegment = segment
	return p, nil
}

type Partition struct {
	mu            sync.Mutex
	cfg           Config
	index         uint32
	activeSegment *Segment
}

func (p *Partition) GetOffset(offset uint64) (m Message, ok bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	startOffset, ok, err := p.getSegmentForOffsetUnsafe(offset)
	if err != nil || !ok {
		// there can be an issue with reading the dir entries
		// - or -
		// the offset could belong to a vacuumed segment
		return
	}
	m, ok, err = getSegmentOffset(p.cfg, p.index, startOffset, offset)
	return
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

func (p *Partition) Vacuum() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cfg.RetentionMaxAge == 0 && p.cfg.RetentionMaxBytes == 0 {
		return nil
	}
	segmentOffsets, err := getPartitionSegmentOffsets(p.cfg, p.index)
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
			partitionSizeBytes, err := p.getSizeBytes()
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

func (p *Partition) getSegmentForOffsetUnsafe(offset uint64) (startOffset uint64, ok bool, err error) {
	var entries []uint64
	entries, err = getPartitionSegmentOffsets(p.cfg, p.index)
	if err != nil {
		return
	}
	startOffset, ok = getSegmentStartOffsetForOffset(entries, offset)
	return
}

func (p *Partition) shouldCloseActiveSegmentUnsafe(segment *Segment) bool {
	return int64(segment.endOffsetBytes) > p.cfg.SegmentSizeBytesOrDefault()
}

func (p *Partition) closeActiveSegmentUnsafe() error {
	newActive, err := CreateSegment(p.cfg, p.index, p.activeSegment.endOffset)
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
	endTimestamp, err = getSegmentEndTimestamp(p.cfg, p.index, startOffset)
	if err != nil {
		return
	}
	cutoff := time.Now().UTC().Add(-p.cfg.RetentionMaxAge)
	doVacuumSegment = endTimestamp.Before(cutoff)
	return
}

func (p *Partition) vacuumSegment(startOffset uint64) error {
	segmentRoot := formatPathForSegment(p.cfg, p.index, startOffset)
	if err := os.Remove(segmentRoot + extData); err != nil {
		return err
	}
	if err := os.Remove(segmentRoot + extIndex); err != nil {
		return err
	}
	if err := os.Remove(segmentRoot + extTimeIndex); err != nil {
		return err
	}
	return nil
}

func (p *Partition) getSizeBytes() (sizeBytes int64, err error) {
	var offsets []uint64
	offsets, err = getPartitionSegmentOffsets(p.cfg, p.index)
	if err != nil {
		return
	}
	var info fs.FileInfo
	for _, offset := range offsets {
		segmentRoot := formatPathForSegment(p.cfg, p.index, offset)
		info, err = os.Stat(segmentRoot + extData)
		if err != nil {
			return
		}
		sizeBytes += info.Size()
	}
	return
}

func getSegmentEndTimestamp(cfg Config, partitionIndex uint32, startOffset uint64) (ts time.Time, err error) {
	var segment segmentTimeIndex
	var f *os.File
	f, err = os.Open(formatPathForSegment(cfg, partitionIndex, startOffset) + extTimeIndex)
	if err != nil {
		return
	}
	defer f.Close()

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return
	}
	err = binary.Read(f, binary.LittleEndian, &segment)
	if err != nil {
		return
	}
	ts = segment.GetTimestampUTC()
	return
}

func getSegmentEndOffset(cfg Config, partitionIndex uint32, startOffset uint64) (endOffset uint64, err error) {
	var segment segmentIndex
	var f *os.File
	f, err = os.Open(formatPathForSegment(cfg, partitionIndex, startOffset) + extIndex)
	if err != nil {
		return
	}
	defer f.Close()

	if _, err = f.Seek(-int64(segmentIndexSize), io.SeekEnd); err != nil {
		return
	}
	err = binary.Read(f, binary.LittleEndian, &segment)
	if err != nil {
		return
	}
	endOffset = segment.GetOffset()
	return
}

func getSegmentStartOffsetForOffset(entries []uint64, offset uint64) (uint64, bool) {
	for x := len(entries) - 1; x >= 0; x-- {
		startOffset := entries[x]
		if startOffset <= offset {
			return startOffset, true
		}
	}
	return 0, false
}

func getPartitionSegmentOffsets(cfg Config, partitionIndex uint32) ([]uint64, error) {
	entries, err := os.ReadDir(formatPathForPartition(cfg, partitionIndex))
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	var output []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(e.Name(), extData) {
			continue
		}
		segmentStartOffset, err := parseSegmentOffsetFromPath(e.Name())
		if err != nil {
			return nil, err
		}
		output = append(output, segmentStartOffset)
	}
	return output, nil
}
