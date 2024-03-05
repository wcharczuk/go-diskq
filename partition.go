package diskq

type Partition struct {
	index    uint32
	active   *Segment
	segments []Segment
}

func (p *Partition) GetSegmentForOffset(offset uint64) *Segment {
	return nil
}

func (p *Partition) ActiveSegment() *Segment {
	return p.active
}
