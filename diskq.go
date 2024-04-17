package diskq

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// New opens or creates a diskq based on a given path and config.
//
// The `Diskq` type itself should be thought of as a producer with
// exclusive access to write to the data directory named in the config.
//
// The [path] should be the location on disk you want to hold all
// the data associated with the diskq; you cannot split the diskq up
// across multiple disparate paths.
//
// The [cfg] should be customized for what you'd like to use for
// this particular "session" of the diskq producer. You may, for example,
// change the partition count between sessions, or change the segment size
// and retention settings. If you'd like to reuse a previous session's options
// for the diskq, you can use the helper [MaybeReadOptions], and pass the returned
// options from that helper as the [cfg] argument.
//
// If the diskq exists on disk, existing partitions will be opened, and if
// the configured partition count is greater than the number of existing
// partitions, new empty partitions will be created. Existing "orphaned" partitions
// will be left in place until vacuuming potentially removes them.
func New(path string, cfg Options) (*Diskq, error) {
	d := &Diskq{
		id:   UUIDv4(),
		path: path,
		cfg:  cfg,
	}
	if _, err := os.Stat(path); err != nil {
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, fmt.Errorf("diskq; cannot ensure data directory: %w", err)
		}
	}
	if err := d.writeSettings(); err != nil {
		return nil, err
	}
	if err := d.writeSentinel(); err != nil {
		return nil, err
	}
	for partitionIndex := 0; partitionIndex < int(cfg.PartitionCountOrDefault()); partitionIndex++ {
		p, err := createOrOpenPartition(path, cfg, uint32(partitionIndex))
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

// ID returns the id of the diskq producer.
func (dq *Diskq) ID() UUID { return dq.id }

// Path returns the path of the diskq producer.
func (dq *Diskq) Path() string { return dq.path }

// Options returns the config of the diskq producer.
func (dq *Diskq) Options() Options { return dq.cfg }

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

// Vacuum deletes old segments from all partitions if retention is configured.
//
// Vacuum will operate on the partitions as they're found on disk; specifically the currently
// configured partition count is ignored in lieu of the extant partition list.
func (dq *Diskq) Vacuum() (err error) {
	if dq.cfg.RetentionMaxAge == 0 && dq.cfg.RetentionMaxBytes == 0 {
		return
	}

	var partitionIndexes []uint32
	partitionIndexes, err = GetPartitions(dq.path)
	if err != nil {
		return
	}
	var partition *Partition
	for _, partitionIndex := range partitionIndexes {
		partition, err = openPartition(dq.path, dq.cfg, partitionIndex)
		if err != nil {
			return
		}
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
	return dq.partitions[hashIndex]
}

func (dq *Diskq) writeSettings() error {
	f, err := os.Create(FormatPathForSettings(dq.path))
	if err != nil {
		return fmt.Errorf("diskq; cannot create settings file: %w", err)
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err = enc.Encode(dq.cfg); err != nil {
		return fmt.Errorf("diskq; cannot encode settings file: %w", err)
	}
	return nil
}
