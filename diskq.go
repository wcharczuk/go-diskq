package diskq

import (
	"sync"
)

func New[A any](cfg Config, encoder Encoder[A]) (*Diskq[A], error) {
	return nil, nil
}

type Diskq[A any] struct {
	mu         sync.RWMutex
	partitions []Partition
}

func (dq *Diskq[A]) Push(value A) (offset int64, err error) {
	return
}

func (dq *Diskq[A]) GetOffset(offset int) (v A, ok bool, err error) {
	return
}

// Vacuum applies retention policies, if configured.
//
// Vacuum will _not_ apply retention to the current segment
// if there are no other, closed segments.
func (dq *Diskq[A]) Vacuum() (err error) {
	return
}
