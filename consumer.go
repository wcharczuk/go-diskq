package diskq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
)

type ConsumerOptions struct {
	StartAtBehavior ConsumerStartAtBehavior
	StartAtOffset   uint64
}

func openConsumer(cfg Config, partitionIndex uint32, options ConsumerOptions) (*Consumer, error) {
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
						tracef("%d | saw new active segment %d", c.partitionIndex, newSegmentStartAt)
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
	if _, err = c.indexHandle.Seek(indexSeekToBytes, io.SeekStart); err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot seek index data: %w", err))
		return
	}

	var ok bool
	if err = binary.Read(c.indexHandle, binary.LittleEndian, &workingSegmentData); err != nil {
		if err != io.EOF {
			c.error(fmt.Errorf("diskq; consumer; cannot read index data: %w", err))
			return
		}
		if c.isActiveSegment() {
			ok, err = c.waitForNewOffset(&workingSegmentData)
			if err != nil {
				tracef("%d | before runloop wait for new offset error", c.partitionIndex)
				c.error(err)
				return
			}
			if !ok {
				tracef("%d | exiting before runloop on channel close", c.partitionIndex)
				return
			}
		}
	}

	if _, err = c.dataHandle.Seek(int64(workingSegmentData.GetOffsetBytes()), io.SeekStart); err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot seek data file: %w", err))
		return
	}

	var m Message
	for {
		data := make([]byte, workingSegmentData.GetSizeBytes())
		if _, err = c.dataHandle.Read(data); err != nil {
			c.error(fmt.Errorf("diskq; consumer; cannot read data file: %w", err))
			tracef("%d | exiting data read error", c.partitionIndex)
			return
		}
		if err = Decode(&m, bytes.NewReader(data)); err != nil {
			c.error(fmt.Errorf("diskq; consumer; cannot decode message from data file: %w", err))
			tracef("%d | exiting data decode", c.partitionIndex)
			return
		}

		select {
		case <-c.done:
			tracef("%d | exiting on done close", c.partitionIndex)
			return
		case c.messages <- MessageWithOffset{
			PartitionIndex: c.partitionIndex,
			Offset:         workingSegmentData.GetOffset(),
			Message:        m,
		}:
		}

		if err = binary.Read(c.indexHandle, binary.LittleEndian, &workingSegmentData); err != nil {
			if err != io.EOF {
				tracef("%d | read index segment data error", c.partitionIndex)
				c.error(fmt.Errorf("diskq; consumer; cannot read index data: %w", err))
				return
			}
			if c.isActiveSegment() {
				ok, err = c.waitForNewOffset(&workingSegmentData)
				if err != nil {
					tracef("%d | exiting on wait for new offset error", c.partitionIndex)
					c.error(err)
					return
				}
				if !ok {
					tracef("%d | exiting runloop on channel close", c.partitionIndex)
					return
				}
				continue
			}

			ok, err = c.advanceToNextSegment(&workingSegmentData)
			if err != nil {
				tracef("%d | exiting on advance to next segment error", c.partitionIndex)
				c.error(err)
				return
			}
			if !ok {
				tracef("%d | exiting runloop on channel close", c.partitionIndex)
				return
			}
		}
	}
}

func (c *Consumer) isActiveSegment() bool {
	return atomic.LoadUint64(&c.partitionActiveSegment) == atomic.LoadUint64(&c.workingSegment)
}

func (c *Consumer) advanceToNextSegment(workingSegmentData *segmentIndex) (ok bool, err error) {
	tracef("%d | advance to next segment", c.partitionIndex)
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

	if err = binary.Read(c.indexHandle, binary.LittleEndian, workingSegmentData); err != nil {
		if err != io.EOF {
			c.error(fmt.Errorf("diskq; consumer; cannot read index data: %w", err))
			return
		}
		if c.isActiveSegment() {
			ok, err = c.waitForNewOffset(workingSegmentData)
			return
		}
	}
	ok = true
	return
}

func (c *Consumer) waitForNewOffset(workingSegmentData *segmentIndex) (ok bool, err error) {
	ok, err = c.waitForIndexWriteEvents(workingSegmentData)
	if err != nil {
		return
	}
	if !ok {
		return
	}

	readErr := binary.Read(c.indexHandle, binary.LittleEndian, workingSegmentData)
	if readErr != nil && readErr != io.EOF {
		err = fmt.Errorf("diskq; consumer; wait for new offsets; cannot read segment: %w", readErr)
		return
	}

	ok, err = c.waitForDataWriteEvents(workingSegmentData)
	return
}

func (c *Consumer) waitForIndexWriteEvents(workingSegmentData *segmentIndex) (ok bool, err error) {
	for {
		select {
		case _, ok = <-c.advanceEvents:
			ok, err = c.advanceToNextSegment(workingSegmentData)
			return
		case _, ok = <-c.indexWriteEvents:
			if !ok {
				return
			}
			indexStat, _ := c.indexHandle.Stat()
			if indexStat.Size()-int64(workingSegmentData.GetOffsetBytes()) >= int64(segmentIndexSize) {
				return
			}
		}
	}
}

func (c *Consumer) waitForDataWriteEvents(workingSegmentData *segmentIndex) (ok bool, err error) {
	dataStat, _ := c.dataHandle.Stat()
	dataSizeBytes := dataStat.Size()
	if dataSizeBytes-int64(workingSegmentData.GetOffsetBytes()) < int64(workingSegmentData.GetSizeBytes()) {
		for {
			_, ok = <-c.dataWriteEvents
			if !ok {
				return
			}
			dataStat, _ = c.dataHandle.Stat()
			dataSizeBytes = dataStat.Size()
			if dataSizeBytes-int64(workingSegmentData.GetOffsetBytes()) >= int64(workingSegmentData.GetSizeBytes()) {
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
		return offsets[len(offsets)-1], nil
	default:
		return 0, fmt.Errorf("diskq; consume; absurd start at behavior: %d", c.options.StartAtBehavior)
	}
}

var traceEnabled = os.Getenv("DISKQ_TRACE") != ""

func tracef(format string, args ...any) {
	if traceEnabled {
		fmt.Println(fmt.Sprintf(format, args...))
	}
}
