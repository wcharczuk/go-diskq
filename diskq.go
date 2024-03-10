package diskq

import (
	"fmt"
	"hash/fnv"
	"time"
)

// New creates or opens a diskq based on a given config.
func New(cfg Config) (*Diskq, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	d := &Diskq{
		cfg: cfg,
	}
	for partitionIndex := 0; partitionIndex < int(cfg.PartitionCountOrDefault()); partitionIndex++ {
		p, err := NewPartition(cfg, uint32(partitionIndex))
		if err != nil {
			return nil, err
		}
		d.partitions = append(d.partitions, p)
	}
	return d, nil
}

type Diskq struct {
	cfg        Config
	partitions []*Partition
}

func (dq *Diskq) Push(value Message) (partition uint32, offset uint64, err error) {
	if value.PartitionKey == "" {
		value.PartitionKey = UUIDv4().String()
	}
	if value.TimestampUTC.IsZero() {
		value.TimestampUTC = time.Now().UTC()
	}
	p := dq.partitionForMessage(value)
	if p == nil {
		err = fmt.Errorf("diskq; partition couldn't be resolved for message")
		return
	}
	offset, err = p.Write(value)
	if err != nil {
		return
	}
	partition = p.index
	return
}

func (dq *Diskq) GetOffset(partitionIndex uint32, offset uint64) (v Message, ok bool, err error) {
	if partitionIndex >= uint32(len(dq.partitions)) {
		return
	}
	v, ok, err = dq.partitions[partitionIndex].GetOffset(offset)
	return
}

func (dq *Diskq) Consume(partitionIndex uint32, options ConsumerOptions) (*Consumer, error) {
	return openConsumer(dq.cfg, partitionIndex, options)
}

// Vacuum deletes old segments from all partitions
// if retention is configured.
func (dq *Diskq) Vacuum() (err error) {
	if dq.cfg.RetentionMaxAge == 0 && dq.cfg.RetentionMaxBytes == 0 {
		return
	}
	for _, partition := range dq.partitions {
		if err = partition.Vacuum(); err != nil {
			return
		}
	}
	return
}

func (dq *Diskq) Close() error {
	for _, p := range dq.partitions {
		_ = p.Close()
	}
	return nil
}

//
// internal helpers
//

func (dq *Diskq) partitionForMessage(m Message) *Partition {
	hashIndex := hashIndexForMessage(m, len(dq.partitions))
	if hashIndex < 0 || hashIndex >= len(dq.partitions) {
		return nil
	}
	return dq.partitions[hashIndex]
}

func hashIndexForMessage(m Message, partitions int) int {
	h := fnv.New32()
	_, _ = h.Write([]byte(m.PartitionKey))
	return int(h.Sum32()) % partitions
}
