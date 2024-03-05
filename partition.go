package diskq

import "sync"

func OpenPartition(cfg Config, partitionIndex uint32) (*Partition, error) {
	return nil, nil
}

type Partition struct {
	mu       sync.Mutex
	cfg      Config
	index    uint32
	segments []*Segment
}

func (p *Partition) GetSegmentForOffset(offset uint64) *Segment {
	return nil
}

func (p *Partition) Write(message *Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.segments[len(p.segments)-1].Write(message); err != nil {
		return err
	}
	if p.shouldCloseActiveSegmentUnsafe(p.segments[len(p.segments)-1]) {
		if err := p.closeActiveSegmentUnsafe(); err != nil {
			return err
		}
	}
	return nil
}

//
// internal
//

func (p *Partition) shouldCloseActiveSegmentUnsafe(segment *Segment) bool {
	return p.cfg.SegmentSizeBytes < int64(segment.endOffsetBytes)
}

func (p *Partition) closeActiveSegmentUnsafe() error {
	activeSegment := p.segments[len(p.segments)-1]
	newActive, err := CreateSegment(p.cfg, p.index, activeSegment.endOffset+1)
	if err != nil {
		return err
	}
	p.segments = append(p.segments, newActive)
	return nil
}
