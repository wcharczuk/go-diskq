package diskq

import (
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"time"
)

// New creates or opens a diskq based on a given config.
//
// The `Diskq` type itself should be thought of as a producer with
// exclusive access to write to the data directory named in the config.
func New(cfg Config) (*Diskq, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	d := &Diskq{
		id:  UUIDv4(),
		cfg: cfg,
	}
	if err := os.MkdirAll(cfg.Path, 0755); err != nil {
		return nil, fmt.Errorf("diskq; cannot ensure data directory: %w", err)
	}
	if err := d.writeSentinel(); err != nil {
		return nil, err
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

// Diskq is the root struct of the queue.
//
// It could be thought of primarily as the "producer" in the
// streaming system; you will use this type to "Push" messages into the streams.
type Diskq struct {
	id         UUID
	cfg        Config
	partitions []*Partition
}

// Push pushes a new message into the diskq, returning the partition it was written to,
// the offset it was written to, and any errors that were generated while writing the message.
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

// GetOffset finds and decodes a message by offset in a given partition and returns it.
func (dq *Diskq) GetOffset(partitionIndex uint32, offset uint64) (v Message, ok bool, err error) {
	if partitionIndex >= uint32(len(dq.partitions)) {
		return
	}
	v, ok, err = dq.partitions[partitionIndex].GetOffset(offset)
	return
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

// Sync calls `fsync` on each of the partition file handles.
//
// This has the effect of realizing any buffered data to disk.
//
// You shouldn't ever need to call this, but it's here if you do need to.
func (dq *Diskq) Sync() error {
	for _, p := range dq.partitions {
		if err := maybeSync(p.activeSegment.index); err != nil {
			return err
		}
		if err := maybeSync(p.activeSegment.timeindex); err != nil {
			return err
		}
		if err := maybeSync(p.activeSegment.data); err != nil {
			return err
		}
	}
	return nil
}

// Close releases any resources associated with the diskq and
// removes the sentinel file.
func (dq *Diskq) Close() error {
	for _, p := range dq.partitions {
		_ = p.Close()
	}
	return dq.releaseSentinel()
}

//
// internal methods
//

func (dq *Diskq) writeSentinel() error {
	sentinelPath := filepath.Join(dq.cfg.Path, "owner")
	sf, err := os.OpenFile(sentinelPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	defer func() { _ = sf.Close() }()

	_, err = sf.Write(dq.id[:])
	return err
}

func (dq *Diskq) releaseSentinel() error {
	return os.Remove(filepath.Join(dq.cfg.Path, "owner"))
}

func maybeSync(wr io.Writer) error {
	if typed, ok := wr.(*os.File); ok {
		return typed.Sync()
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
