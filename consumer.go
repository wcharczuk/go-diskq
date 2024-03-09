package diskq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

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
		cfg:            cfg,
		partitionIndex: partitionIndex,
		options:        options,
		messages:       make(chan MessageWithOffset),
		errors:         make(chan error, 1),
		done:           make(chan struct{}),
		notifyEvents:   make(chan struct{}, 1), // we buffer this so that we always unblock
		notify:         notify,
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

	partitionActiveSegmentMu sync.Mutex
	partitionActiveSegment   uint64

	notifyEvents chan struct{}

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
	if c.notifyEvents != nil {
		close(c.notifyEvents)
		c.notifyEvents = nil
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

// read does basically everything.
//
// it's incredibly complicated, but generally we figure out which segment to read from
// seek into that segment, then start reading offsets out and pushing into the messages channel.
//
// if we eof on that segment, we figure out what to do next, which is either swap out the
// working segment for the _next_ segment, or block because we're on the active segment for
// the partition and we have to wait for new offsets to be written.
func (c *Consumer) read() {
	partitionPath := formatPathForPartition(c.cfg, c.partitionIndex)
	var workingStartOffset uint64

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
						c.partitionActiveSegmentMu.Lock()
						c.partitionActiveSegment, _ = parseSegmentOffsetFromPath(event.Name)
						c.partitionActiveSegmentMu.Unlock()
					}
					continue
				}
				if event.Has(fsnotify.Write) {
					if c.indexHandle != nil {
						if event.Name == c.indexHandle.Name() && workingStartOffset == c.partitionActiveSegment {
							select {
							case <-c.done:
								return
							case c.notifyEvents <- struct{}{}:
							default:
							}
						}
					}
					continue
				}
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
	c.partitionActiveSegmentMu.Lock()
	c.partitionActiveSegment = offsets[len(offsets)-1]
	c.partitionActiveSegmentMu.Unlock()

	effectiveConsumeAtOffset, err := c.determineEffectiveConsumeAtOffset(offsets)
	if err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot determine effective consume at offset: %w", err))
		return
	}

	workingStartOffset, _ = getSegmentStartOffsetForOffset(offsets, effectiveConsumeAtOffset)
	c.indexHandle, err = openSegmentFileForRead(c.cfg, c.partitionIndex, workingStartOffset, extIndex)
	if err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot open index file: %w", err))
		return
	}
	c.dataHandle, err = openSegmentFileForRead(c.cfg, c.partitionIndex, workingStartOffset, extData)
	if err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot open data file: %w", err))
		return
	}

	// this notify handles _everything_ for the partition
	err = c.notify.Add(partitionPath)
	if err != nil {
		c.error(fmt.Errorf("diskq; consumer; watch partition path: %w", err))
		return
	}

	// seek to the correct offset
	relativeOffset := effectiveConsumeAtOffset - workingStartOffset
	indexSeekToBytes := int64(segmentIndexSize) * int64(relativeOffset)
	if _, err = c.indexHandle.Seek(indexSeekToBytes, io.SeekStart); err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot seek index data: %w", err))
		return
	}

	var ok bool
	var workingSegmentData segmentIndex
	if err = binary.Read(c.indexHandle, binary.LittleEndian, &workingSegmentData); err != nil {
		if err != io.EOF {
			c.error(fmt.Errorf("diskq; consumer; cannot read index data: %w", err))
			return
		}
		if workingStartOffset == c.partitionActiveSegment {
			ok, err = c.waitForNewOffsets(&workingSegmentData)
			if err != nil {
				c.error(err)
				return
			}
			if !ok {
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
			return
		}
		if err = Decode(&m, bytes.NewReader(data)); err != nil {
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

		if err = binary.Read(c.indexHandle, binary.LittleEndian, &workingSegmentData); err != nil {
			if err != io.EOF {
				c.error(fmt.Errorf("diskq; consumer; cannot read index data: %w", err))
				return
			}
			if workingStartOffset == c.partitionActiveSegment {
				ok, err = c.waitForNewOffsets(&workingSegmentData)
				if err != nil {
					c.error(err)
					return
				}
				if !ok {
					return
				}
				continue
			}

			// assumption:
			// workingOffset != c.partitionActiveSegment

			offsets, err = getPartitionSegmentOffsets(c.cfg, c.partitionIndex)
			if err != nil {
				c.error(fmt.Errorf("diskq; consumer; cannot get partition offsets: %w", err))
				return
			}

			// close the existing handles
			_ = c.indexHandle.Close()
			_ = c.dataHandle.Close()

			workingStartOffset, _ = getSegmentStartOffsetForOffset(offsets, workingSegmentData.GetOffset()+1)
			c.indexHandle, err = openSegmentFileForRead(c.cfg, c.partitionIndex, workingStartOffset, extIndex)
			if err != nil {
				c.error(fmt.Errorf("diskq; consumer; cannot open index file: %w", err))
				return
			}
			c.dataHandle, err = openSegmentFileForRead(c.cfg, c.partitionIndex, workingStartOffset, extData)
			if err != nil {
				c.error(fmt.Errorf("diskq; consumer; cannot open data file: %w", err))
				return
			}
			if err = binary.Read(c.indexHandle, binary.LittleEndian, &workingSegmentData); err != nil {
				c.error(fmt.Errorf("diskq; consumer; cannot read index data: %w", err))
				return
			}
		}
	}
}

func (c *Consumer) waitForNewOffsets(workingSegmentData *segmentIndex) (ok bool, err error) {
	_, ok = <-c.notifyEvents
	if !ok {
		return
	}
	err = binary.Read(c.indexHandle, binary.LittleEndian, workingSegmentData)
	if err != nil {
		err = fmt.Errorf("diskq; consumer; wait for new offsets; cannot read segment: %w", err)
	}
	return
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
