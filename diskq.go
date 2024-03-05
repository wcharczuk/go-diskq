package diskq

import (
	"hash/fnv"
	"sync"
)

func CreateOrOpen(cfg Config) (*Diskq, error) {
	return nil, nil
}

type Diskq struct {
	mu         sync.RWMutex
	cfg        Config
	partitions []Partition
}

func (dq *Diskq) Push(value Message) (offset int64, err error) {
	return
}

func (dq *Diskq) GetOffset(offset int) (v Message, ok bool, err error) {
	return
}

func (dq *Diskq) Consume(startAtOffset int) (c *Consumer, err error) {
	return
}

func (dq *Diskq) Vacuum() (err error) {
	return
}

func (dq *Diskq) partitionForMessage(m *Message) *Partition {
	hashIndex := dq.hashIndexForMessage(m)
	if hashIndex < 0 || len(dq.partitions) >= hashIndex {
		return nil
	}
	return &dq.partitions[hashIndex]
}

func (dq *Diskq) hashIndexForMessage(m *Message) int {
	h := fnv.New32()
	_, _ = h.Write([]byte(m.PartitionKey))
	return int(h.Sum32()) % len(dq.partitions)
}
