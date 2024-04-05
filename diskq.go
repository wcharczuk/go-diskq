package diskq

import (
	"fmt"
	"os"
	"time"
)

// New creates or opens a diskq based on a given config.
//
// The `Diskq` type itself should be thought of as a producer with
// exclusive access to write to the data directory named in the config.
//
// If the diskq exists on disk, new empty partitions will be created
// and existing ones opened at their latest offsets for writing.
func New(path string, cfg Options) (*Diskq, error) {
	d := &Diskq{
		id:   UUIDv4(),
		path: path,
		cfg:  cfg,
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("diskq; cannot ensure data directory: %w", err)
	}
	if err := d.writeSentinel(); err != nil {
		return nil, err
	}
	for partitionIndex := 0; partitionIndex < int(cfg.PartitionCountOrDefault()); partitionIndex++ {
		p, err := NewPartition(path, cfg, uint32(partitionIndex))
		if err != nil {
			return nil, err
		}
		d.partitions = append(d.partitions, p)
	}
	return d, nil
}

// Diskq is the root struct of the diskq.
//
// It could be thought of primarily as the "producer" in the
// streaming system; you will use this type to "Push" messages into the partitions.
//
// Close will release open file handles that are held by the partitions.
type Diskq struct {
	id         UUID
	path       string
	cfg        Options
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
		if err := p.Sync(); err != nil {
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
	sentinelPath := FormatPathForSentinel(dq.path)
	sf, err := os.OpenFile(sentinelPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("diskq; write sentinel; cannot open file in exclusive mode: %w", err)
	}
	defer func() { _ = sf.Close() }()
	_, err = sf.Write(dq.id[:])
	if err != nil {
		return fmt.Errorf("diskq; write sentinel; cannot write id to file: %w", err)
	}
	return nil
}

func (dq *Diskq) releaseSentinel() error {
	sentinelPath := FormatPathForSentinel(dq.path)
	return os.Remove(sentinelPath)
}

func (dq *Diskq) partitionForMessage(m Message) *Partition {
	hashIndex := hashIndexForMessage(m, len(dq.partitions))
	if hashIndex < 0 || hashIndex >= len(dq.partitions) {
		return nil
	}
	return dq.partitions[hashIndex]
}
