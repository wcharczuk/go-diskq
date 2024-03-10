package diskq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
)

type ConsumerOptions struct {
	StartAtBehavior ConsumerStartAtBehavior
	StartAtOffset   uint64
}

// OpenConsumer creates a new consumer for a given config, partition and options.
//
// There can be many consumers for a given partition, and you can consume partitions that may be
// written to by external processes.
func OpenConsumer(cfg Config, partitionIndex uint32, options ConsumerOptions) (*Consumer, error) {
	notify, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	c := &Consumer{
		cfg:              cfg,
		partitionIndex:   partitionIndex,
		options:          options,
		messages:         make(chan MessageWithOffset),
		errors:           make(chan error, 1),
		done:             make(chan struct{}),
		advanceEvents:    make(chan struct{}, 1),
		indexWriteEvents: make(chan struct{}, 1),
		dataWriteEvents:  make(chan struct{}, 1),
		notify:           notify,
	}
	go c.read()
	return c, nil
}

type MessageWithOffset struct {
	PartitionIndex uint32
	Offset         uint64
	Message        Message
}

type ConsumerStartAtBehavior uint8

const (
	ConsumerStartAtBeginning ConsumerStartAtBehavior = iota
	ConsumerStartAtOffset
	ConsumerStartAtActiveSegmentStart
	ConsumerStartAtActiveSegmentLatest
)

type Consumer struct {
	cfg            Config
	partitionIndex uint32
	options        ConsumerOptions
	messages       chan MessageWithOffset
	errors         chan error
	notify         *fsnotify.Watcher

	partitionActiveSegment uint64
	workingSegment         uint64

	advanceEvents    chan struct{}
	indexWriteEvents chan struct{}
	dataWriteEvents  chan struct{}

	done chan struct{}

	indexHandle *os.File
	dataHandle  *os.File
}

func (c *Consumer) Messages() <-chan MessageWithOffset {
	return c.messages
}

func (c *Consumer) Errors() <-chan error {
	return c.errors
}

func (c *Consumer) Close() error {
	close(c.done)
	c.done = nil
	if c.messages != nil {
		close(c.messages)
		c.messages = nil
	}
	if c.errors != nil {
		close(c.errors)
		c.errors = nil
	}
	if c.advanceEvents != nil {
		close(c.advanceEvents)
		c.advanceEvents = nil
	}
	if c.indexWriteEvents != nil {
		close(c.indexWriteEvents)
		c.indexWriteEvents = nil
	}
	if c.dataWriteEvents != nil {
		close(c.dataWriteEvents)
		c.dataWriteEvents = nil
	}
	if c.indexHandle != nil {
		_ = c.indexHandle.Close()
	}
	if c.dataHandle != nil {
		_ = c.dataHandle.Close()
	}
	return c.notify.Close()
}

//
// internal methods
//

func (c *Consumer) read() {
	partitionPath := formatPathForPartition(c.cfg, c.partitionIndex)
	var workingSegmentData segmentIndex
	// fsnotify loop
	go func() {
		for {
			select {
			case event, ok := <-c.notify.Events:
				if !ok {
					return
				}
				if event.Has(fsnotify.Create) {
					if strings.HasSuffix(event.Name, extData) {
						newSegmentStartAt, _ := parseSegmentOffsetFromPath(event.Name)
						atomic.StoreUint64(&c.partitionActiveSegment, newSegmentStartAt)
						select {
						case <-c.done:
							return
						case c.advanceEvents <- struct{}{}:
						default:
						}
					}
					continue
				}
				if atomic.LoadUint64(&c.partitionActiveSegment) != atomic.LoadUint64(&c.workingSegment) {
					continue
				}
				if event.Has(fsnotify.Write) {
					if event.Name == c.indexHandle.Name() {
						select {
						case <-c.done:
							return
						case c.indexWriteEvents <- struct{}{}:
						default:
						}
					} else if event.Name == c.dataHandle.Name() {
						select {
						case <-c.done:
							return
						case c.dataWriteEvents <- struct{}{}:
						default:
						}
					}
				}
				continue
			case err, ok := <-c.notify.Errors:
				if !ok {
					return
				}
				c.error(fmt.Errorf("diskq; consumer; notify returned error: %w", err))
				return
			}
		}
	}()

	offsets, err := getPartitionSegmentOffsets(c.cfg, c.partitionIndex)
	if err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot get partition offsets: %w", err))
		return
	}

	// set the active segment
	atomic.StoreUint64(&c.partitionActiveSegment, offsets[len(offsets)-1])
	effectiveConsumeAtOffset, err := c.determineEffectiveConsumeAtOffset(offsets)
	if err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot determine effective consume at offset: %w", err))
		return
	}

	workingSegment, _ := getSegmentStartOffsetForOffset(offsets, effectiveConsumeAtOffset)
	atomic.StoreUint64(&c.workingSegment, workingSegment)
	c.indexHandle, err = openSegmentFileForRead(c.cfg, c.partitionIndex, c.workingSegment, extIndex)
	if err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot open index file: %w", err))
		return
	}
	c.dataHandle, err = openSegmentFileForRead(c.cfg, c.partitionIndex, c.workingSegment, extData)
	if err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot open data file: %w", err))
		return
	}

	err = c.notify.Add(partitionPath)
	if err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot watch partition path: %w", err))
		return
	}

	// seek to the correct offset
	relativeOffset := effectiveConsumeAtOffset - c.workingSegment
	indexSeekToBytes := int64(segmentIndexSize) * int64(relativeOffset)
	if indexSeekToBytes > 0 {
		if _, err = c.indexHandle.Seek(indexSeekToBytes, io.SeekStart); err != nil {
			c.error(fmt.Errorf("diskq; consumer; cannot seek index data: %w", err))
			return
		}
	}

	var ok bool
	ok, err = c.readNextSegmentIndex(&workingSegmentData)
	if err != nil {
		c.error(err)
		return
	}
	if !ok {
		return
	}

	if workingSegmentData.GetOffsetBytes() > 0 {
		if _, err = c.dataHandle.Seek(int64(workingSegmentData.GetOffsetBytes()), io.SeekStart); err != nil {
			c.error(fmt.Errorf("diskq; consumer; cannot seek data file: %w", err))
			return
		}
	}

	var m Message
	var messageData []byte
	for {
		messageData = make([]byte, workingSegmentData.GetSizeBytes())
		if _, err = c.dataHandle.Read(messageData); err != nil {
			c.error(fmt.Errorf("diskq; consumer; cannot read data file: %w", err))
			return
		}
		if err = Decode(&m, bytes.NewReader(messageData)); err != nil {
			c.error(fmt.Errorf("diskq; consumer; cannot decode message from data file: %w", err))
			return
		}

		select {
		case <-c.done:
			return
		case c.messages <- MessageWithOffset{
			PartitionIndex: c.partitionIndex,
			Offset:         workingSegmentData.GetOffset(),
			Message:        m,
		}:
		}

		ok, err = c.readNextSegmentIndex(&workingSegmentData)
		if err != nil {
			c.error(err)
			return
		}
		if !ok {
			return
		}
	}
}

func (c *Consumer) readNextSegmentIndex(workingSegmentData *segmentIndex) (ok bool, err error) {
	var indexStat fs.FileInfo
	indexStat, err = c.indexHandle.Stat()
	if err != nil {
		return
	}
	indexPosition, err := c.indexHandle.Seek(0, io.SeekCurrent)
	if err != nil {
		return
	}
	if (indexStat.Size() - indexPosition) < int64(segmentIndexSize) {
		if c.isActiveSegment() {
			ok, err = c.waitForNewOffset(workingSegmentData)
			return
		}

		ok, err = c.advanceToNextSegment(workingSegmentData)
		return
	}

	if err = binary.Read(c.indexHandle, binary.LittleEndian, workingSegmentData); err != nil {
		err = fmt.Errorf("cannot read next working segment index: %w", err)
		return
	}
	ok, err = c.maybeWaitForDataWriteEvents(workingSegmentData)
	return
}

func (c *Consumer) isActiveSegment() bool {
	return atomic.LoadUint64(&c.partitionActiveSegment) == atomic.LoadUint64(&c.workingSegment)
}

func (c *Consumer) advanceToNextSegment(workingSegmentData *segmentIndex) (ok bool, err error) {
	var offsets []uint64
	offsets, err = getPartitionSegmentOffsets(c.cfg, c.partitionIndex)
	if err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot get partition offsets: %w", err))
		return
	}

	_ = c.indexHandle.Close()
	_ = c.dataHandle.Close()

	atomic.StoreUint64(&c.workingSegment, c.getNextSegment(offsets))
	c.indexHandle, err = openSegmentFileForRead(c.cfg, c.partitionIndex, c.workingSegment, extIndex)
	if err != nil {
		err = fmt.Errorf("diskq; consumer; cannot open index file: %w", err)
		return
	}
	c.dataHandle, err = openSegmentFileForRead(c.cfg, c.partitionIndex, c.workingSegment, extData)
	if err != nil {
		err = fmt.Errorf("diskq; consumer; cannot open data file: %w", err)
		return
	}
	ok, err = c.readNextSegmentIndex(workingSegmentData)
	return
}

func (c *Consumer) waitForNewOffset(workingSegmentData *segmentIndex) (ok bool, err error) {
	var indexStat fs.FileInfo
	var indexPosition int64

sized:
	for {
		select {
		case _, ok = <-c.advanceEvents:
			ok, err = c.advanceToNextSegment(workingSegmentData)
			return

		case _, ok = <-c.indexWriteEvents:
			if !ok {
				return
			}
			indexStat, err = c.indexHandle.Stat()
			if err != nil {
				return
			}
			indexPosition, err = c.indexHandle.Seek(0, io.SeekCurrent)
			if err != nil {
				return
			}
			if indexStat.Size()-indexPosition >= int64(segmentIndexSize) {
				break sized
			}
		}
	}

	readErr := binary.Read(c.indexHandle, binary.LittleEndian, workingSegmentData)
	if readErr != nil {
		err = fmt.Errorf("diskq; consumer; wait for new offsets; cannot read segment: %w", readErr)
		return
	}
	ok, err = c.maybeWaitForDataWriteEvents(workingSegmentData)
	return
}

func (c *Consumer) maybeWaitForDataWriteEvents(workingSegmentData *segmentIndex) (ok bool, err error) {
	var dataStat fs.FileInfo
	dataStat, err = c.dataHandle.Stat()
	if err != nil {
		return
	}
	dataSizeBytes := dataStat.Size()

	dataPosition, err := c.dataHandle.Seek(0, io.SeekCurrent)
	if err != nil {
		return
	}

	if dataPosition != int64(workingSegmentData.GetOffsetBytes()) {
		err = fmt.Errorf("corrupted working segment position versus data position; %d vs %d", dataPosition, workingSegmentData.GetOffsetBytes())
		return
	}

	if dataSizeBytes-dataPosition < int64(workingSegmentData.GetSizeBytes()) {
		for {
			_, ok = <-c.dataWriteEvents
			if !ok {
				return
			}
			dataStat, err = c.dataHandle.Stat()
			if err != nil {
				return
			}
			dataSizeBytes = dataStat.Size()
			dataPosition, err = c.dataHandle.Seek(0, io.SeekCurrent)
			if err != nil {
				return
			}
			if dataSizeBytes-dataPosition >= int64(workingSegmentData.GetSizeBytes()) {
				return
			}
		}
	}
	ok = true
	return
}

func (c *Consumer) getNextSegment(offsets []uint64) (nextWorkingSegment uint64) {
	for _, offset := range offsets {
		if offset > c.workingSegment {
			return offset
		}
	}
	return c.partitionActiveSegment
}

func (c *Consumer) error(err error) {
	if err != nil && c.errors != nil {
		c.errors <- err
	}
}

func (c *Consumer) determineEffectiveConsumeAtOffset(offsets []uint64) (uint64, error) {
	switch c.options.StartAtBehavior {
	case ConsumerStartAtOffset:
		if c.options.StartAtOffset < offsets[0] {
			return 0, fmt.Errorf("diskq; consume; invalid start at offset: %d", c.options.StartAtOffset)
		}
		return c.options.StartAtOffset, nil
	case ConsumerStartAtBeginning:
		return offsets[0], nil
	case ConsumerStartAtActiveSegmentStart:
		return offsets[len(offsets)-1], nil
	case ConsumerStartAtActiveSegmentLatest:
		return getSegmentEndOffset(c.cfg, c.partitionIndex, offsets[len(offsets)-1])
	default:
		return 0, fmt.Errorf("diskq; consume; absurd start at behavior: %d", c.options.StartAtBehavior)
	}
}
