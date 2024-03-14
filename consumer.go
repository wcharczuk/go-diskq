package diskq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
)

// OpenConsumer creates a new consumer for a given config, partition and options.
//
// There can be many consumers for a given partition, and you can consume partitions that may be
// written to by external processes.
func OpenConsumer(path string, partitionIndex uint32, options ConsumerOptions) (*Consumer, error) {
	_, err := os.Stat(formatPathForPartition(path, partitionIndex))
	if err != nil {
		return nil, fmt.Errorf("diskq; consumer; cannot stat data path: %w", err)
	}
	var notify *fsnotify.Watcher
	if options.EndBehavior != ConsumerEndAndClose {
		notify, err = fsnotify.NewWatcher()
		if err != nil {
			return nil, err
		}
	}

	c := &Consumer{
		path:           path,
		partitionIndex: partitionIndex,
		options:        options,
		messages:       make(chan MessageWithOffset),
		errors:         make(chan error, 1),
		done:           make(chan struct{}),
		didStart:       make(chan struct{}),

		notify: notify,

		// for internal filesystem events we have a buffer of (1) event
		// so that we continually proceed when there are new offsets in
		// the active segment.
		advanceEvents:    make(chan struct{}, 1),
		indexWriteEvents: make(chan struct{}, 1),
		dataWriteEvents:  make(chan struct{}, 1),
	}
	go c.read()
	<-c.didStart
	return c, nil
}

// ConsumerOptions are options that control how consumers behave.
type ConsumerOptions struct {
	StartAtBehavior ConsumerStartBehavior
	StartAtOffset   uint64
	EndBehavior     ConsumerEndBehavior
	EndAtOffset     uint64
}

// MessageWithOffset is a special wrapping type for messages
// that adds the partition index and the offset of messages
// read by consumers.
type MessageWithOffset struct {
	Message
	PartitionIndex uint32
	Offset         uint64
}

// ConsumerStartBehavior controls how the consumer determines the
// first offset it will read from.
type ConsumerStartBehavior uint8

// ConsumerStartAtBehavior values.
const (
	ConsumerStartAtBeginning ConsumerStartBehavior = iota
	ConsumerStartAtOffset
	ConsumerStartAtActiveSegmentStart
	ConsumerStartAtActiveSegmentLatest
)

// ConsumerEndBehavior controls how the consumer behaves when the
// last offset is read in the active segment.
type ConsumerEndBehavior uint8

// ConsumerEndBehavior values.
const (
	ConsumerEndAndWait ConsumerEndBehavior = iota
	ConsumerEndAtOffset
	ConsumerEndAndClose
)

// Consumer handles reading messages from a given partition.
//
// Consumers can start at known offsets, or at queried offsets based
// on the start behavior.
//
// Consumers can also end at specific offsets, or end when the last offset
// is read in the active segment, or just block and wait for new offsets
// to be written to the active segment (this is the default behavior).
type Consumer struct {
	mu             sync.Mutex
	path           string
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

	didStart chan struct{}
	done     chan struct{}

	indexHandle *os.File
	dataHandle  *os.File
}

// Messages is how you will read the messages the consumer sees
// as it reads the segments for the given partition.
//
// The messages channel is unbuffered, so you must read the message
// for the consumer to be able to continue.
func (c *Consumer) Messages() <-chan MessageWithOffset {
	return c.messages
}

// Errors returns a channel that carries errors returned when reading
// messages through the lifetime of the consumer.
//
// Generally there will be at most (1) error pushed into this channel, and
// the channel will be buffered so you can read it out later.
func (c *Consumer) Errors() <-chan error {
	return c.errors
}

// Close closes the consumer and frees any held resources like file handles.
//
// Generally close is called when the read loop exits on its own, but you can
// stop a consumer early if you call `Close` before the read loop completes.
//
// Once a consumer is closed it cannot be re-used; to start consuming again,
// open a new consumer with `OpenConsumer(...)`.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done == nil {
		return nil
	}

	close(c.done)
	// NOTE (wc): we have to set this to be nil
	// 	so that we'll know if we've closed the consumer.
	//	We should _not_ set read channels to nil because they
	//	will not show as closed on read.
	c.done = nil

	close(c.messages)
	close(c.errors)
	close(c.advanceEvents)
	close(c.indexWriteEvents)
	close(c.dataWriteEvents)

	_ = c.indexHandle.Close()
	c.indexHandle = nil

	_ = c.dataHandle.Close()
	c.dataHandle = nil

	// we may not be notifying at all!
	if c.notify != nil {
		_ = c.notify.Close()
	}
	return nil
}

//
// internal methods
//

func (c *Consumer) read() {
	defer func() {
		if err := c.Close(); err != nil {
			c.error(err)
		}
	}()

	close(c.didStart)
	c.didStart = nil

	var workingSegmentData segmentIndex
	ok, err := c.initializeRead(&workingSegmentData)
	if err != nil {
		c.error(err)
		return
	}
	if !ok {
		return
	}

	var m Message
	var messageData []byte
	for {
		select {
		case <-c.done:
			return
		default:
		}

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

		if c.options.EndBehavior == ConsumerEndAtOffset && workingSegmentData.GetOffset() == c.options.EndAtOffset {
			return
		}

		ok, err = c.readNextSegmentIndexAndMaybeWaitForDataWrites(&workingSegmentData)
		if err != nil {
			c.error(err)
			return
		}
		if !ok {
			return
		}
	}
}

func (c *Consumer) initializeRead(workingSegmentData *segmentIndex) (ok bool, err error) {
	partitionPath := formatPathForPartition(c.path, c.partitionIndex)
	if c.options.EndBehavior != ConsumerEndAndClose {
		go c.listenForFilesystemEvents()
	}

	offsets, err := getPartitionSegmentOffsets(c.path, c.partitionIndex)
	if err != nil {
		return
	}

	// set the active segment
	atomic.StoreUint64(&c.partitionActiveSegment, offsets[len(offsets)-1])
	effectiveConsumeAtOffset, err := c.determineEffectiveConsumeAtOffset(offsets)
	if err != nil {
		return
	}

	workingSegmentOffset, _ := getSegmentStartOffsetForOffset(offsets, effectiveConsumeAtOffset)
	atomic.StoreUint64(&c.workingSegment, workingSegmentOffset)

	c.indexHandle, err = openSegmentFileForRead(c.path, c.partitionIndex, c.workingSegment, extIndex)
	if err != nil {
		return
	}
	c.dataHandle, err = openSegmentFileForRead(c.path, c.partitionIndex, c.workingSegment, extData)
	if err != nil {
		return
	}

	if c.options.EndBehavior != ConsumerEndAndClose {
		err = c.notify.Add(partitionPath)
		if err != nil {
			return
		}
	}

	// seek to the correct offset
	relativeOffset := effectiveConsumeAtOffset - c.workingSegment
	indexSeekToBytes := int64(segmentIndexSize) * int64(relativeOffset)
	if indexSeekToBytes > 0 {
		if _, err = c.indexHandle.Seek(indexSeekToBytes, io.SeekStart); err != nil {
			return
		}
	}

	ok, err = c.readNextSegmentIndex(workingSegmentData)
	if err != nil {
		return
	}
	if !ok {
		return
	}
	if workingSegmentData.GetOffsetBytes() > 0 {
		if _, err = c.dataHandle.Seek(int64(workingSegmentData.GetOffsetBytes()), io.SeekStart); err != nil {
			return
		}
	}
	ok = true
	return
}

func (c *Consumer) listenForFilesystemEvents() {
	for {
		select {
		case <-c.done:
			return
		default:
		}

		select {
		case <-c.done:
			return
		case event, ok := <-c.notify.Events:
			if !ok {
				return
			}
			if c.indexHandle == nil {
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
		if c.isReadingActiveSegment() {
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
	ok = true
	return
}

func (c *Consumer) readNextSegmentIndexAndMaybeWaitForDataWrites(workingSegmentData *segmentIndex) (ok bool, err error) {
	ok, err = c.readNextSegmentIndex(workingSegmentData)
	if err != nil || !ok {
		return
	}
	ok, err = c.maybeWaitForDataWriteEvents(workingSegmentData)
	return
}

func (c *Consumer) isReadingActiveSegment() bool {
	return atomic.LoadUint64(&c.partitionActiveSegment) == atomic.LoadUint64(&c.workingSegment)
}

func (c *Consumer) advanceToNextSegment(workingSegmentData *segmentIndex) (ok bool, err error) {
	err = c.advanceFilesToNextSegment()
	if err != nil {
		return
	}
	ok, err = c.readNextSegmentIndex(workingSegmentData)
	return
}

func (c *Consumer) advanceFilesToNextSegment() (err error) {
	var offsets []uint64
	offsets, err = getPartitionSegmentOffsets(c.path, c.partitionIndex)
	if err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot get partition offsets: %w", err))
		return
	}
	_ = c.indexHandle.Close()
	_ = c.dataHandle.Close()

	c.indexWriteEvents = make(chan struct{}, 1)
	c.dataWriteEvents = make(chan struct{}, 1)

	atomic.StoreUint64(&c.workingSegment, c.getNextSegment(offsets))
	c.indexHandle, err = openSegmentFileForRead(c.path, c.partitionIndex, c.workingSegment, extIndex)
	if err != nil {
		err = fmt.Errorf("diskq; consumer; cannot open index file: %w", err)
		return
	}
	c.dataHandle, err = openSegmentFileForRead(c.path, c.partitionIndex, c.workingSegment, extData)
	if err != nil {
		err = fmt.Errorf("diskq; consumer; cannot open data file: %w", err)
		return
	}
	return
}

func (c *Consumer) waitForNewOffset(workingSegmentData *segmentIndex) (ok bool, err error) {
	if c.options.EndBehavior == ConsumerEndAndClose {
		return
	}

	var indexStat fs.FileInfo
	var indexPosition int64

sized:
	for {
		select {
		case <-c.done:
			return
		case _, ok = <-c.advanceEvents:
			if !ok {
				return
			}
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
		err = fmt.Errorf("corrupted working segment position versus data position; data_position=%d vs working_segment=%d", dataPosition, workingSegmentData.GetOffsetBytes())
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
	if err != nil && c.done != nil {
		select {
		case <-c.done:
			return
		case c.errors <- err:
		}
	}
}

func (c *Consumer) determineEffectiveConsumeAtOffset(offsets []uint64) (uint64, error) {
	switch c.options.StartAtBehavior {
	case ConsumerStartAtOffset:
		if c.options.StartAtOffset < offsets[0] {
			return 0, fmt.Errorf("diskq; consume; invalid start at offset: %d", c.options.StartAtOffset)
		}
		return c.options.StartAtOffset + 1, nil
	case ConsumerStartAtBeginning:
		return offsets[0], nil
	case ConsumerStartAtActiveSegmentStart:
		return offsets[len(offsets)-1], nil
	case ConsumerStartAtActiveSegmentLatest:
		return getSegmentEndOffset(c.path, c.partitionIndex, offsets[len(offsets)-1])
	default:
		return 0, fmt.Errorf("diskq; consume; absurd start at behavior: %d", c.options.StartAtBehavior)
	}
}
