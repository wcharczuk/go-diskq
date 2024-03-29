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
	_, err := os.Stat(FormatPathForPartition(path, partitionIndex))
	if err != nil {
		return nil, fmt.Errorf("diskq; consumer; cannot stat data path: %w", err)
	}
	var fsNotify *fsnotify.Watcher
	if options.EndBehavior != ConsumerEndBehaviorClose {
		fsNotify, err = fsnotify.NewWatcher()
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
		fsNotify:       fsNotify,

		// for internal filesystem events we have a buffer of (1) event
		// so that we continually proceed when there are new offsets in
		// the active segment.
		advanceEvents:    make(chan struct{}, 1),
		indexWriteEvents: make(chan struct{}, 1),
		dataWriteEvents:  make(chan struct{}, 1),
	}
	go c.read()
	<-c.didStart // make sure not to return until the read goroutine is scheduled
	return c, nil
}

// ConsumerOptions are options that control how consumers behave.
type ConsumerOptions struct {
	StartBehavior ConsumerStartBehavior
	StartOffset   uint64
	EndBehavior   ConsumerEndBehavior
	EndOffset     uint64
}

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
	fsNotify       *fsnotify.Watcher

	partitionActiveSegmentStartOffset uint64
	workingSegmentStartOffset         uint64
	workingSegmentIndex               SegmentIndex

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
	//	will not show as closed on read by user code if they're
	//	set to nil.
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
	if c.fsNotify != nil {
		_ = c.fsNotify.Close()
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

	ok, err := c.initializeRead()
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

		messageData = make([]byte, c.workingSegmentIndex.GetSizeBytes())
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
			Offset:         c.workingSegmentIndex.GetOffset(),
			Message:        m,
		}:
		}

		if c.options.EndBehavior == ConsumerEndBehaviorAtOffset && c.workingSegmentIndex.GetOffset() == c.options.EndOffset {
			return
		}

		ok, err = c.readNextSegmentIndexAndMaybeWaitForDataWrites()
		if err != nil {
			c.error(err)
			return
		}
		if !ok {
			return
		}
	}
}

func (c *Consumer) initializeRead() (ok bool, err error) {
	partitionPath := FormatPathForPartition(c.path, c.partitionIndex)

	offsets, err := GetPartitionSegmentStartOffsets(c.path, c.partitionIndex)
	if err != nil {
		return
	}

	// set the active segment
	atomic.StoreUint64(&c.partitionActiveSegmentStartOffset, offsets[len(offsets)-1])
	effectiveConsumeAtOffset, err := c.determineEffectiveConsumeAtOffset(offsets)
	if err != nil {
		return
	}

	workingSegmentOffset, _ := GetSegmentStartOffsetForOffset(offsets, effectiveConsumeAtOffset)
	atomic.StoreUint64(&c.workingSegmentStartOffset, workingSegmentOffset)

	c.indexHandle, err = OpenSegmentFileForRead(c.path, c.partitionIndex, c.workingSegmentStartOffset, ExtIndex)
	if err != nil {
		return
	}
	c.dataHandle, err = OpenSegmentFileForRead(c.path, c.partitionIndex, c.workingSegmentStartOffset, ExtData)
	if err != nil {
		return
	}

	// seek to the correct offset
	relativeOffset := effectiveConsumeAtOffset - c.workingSegmentStartOffset
	indexSeekToBytes := int64(SegmentIndexSizeBytes) * int64(relativeOffset)
	if indexSeekToBytes > 0 {
		if _, err = c.indexHandle.Seek(indexSeekToBytes, io.SeekStart); err != nil {
			return
		}
	}

	if c.options.EndBehavior != ConsumerEndBehaviorClose {
		listenFilesystemStarted := make(chan struct{})
		go c.listenForFilesystemEvents(listenFilesystemStarted)
		<-listenFilesystemStarted
		err = c.fsNotify.Add(partitionPath)
		if err != nil {
			return
		}
	}

	ok, err = c.readNextSegmentIndex()
	if err != nil {
		return
	}
	if !ok {
		return
	}
	if c.workingSegmentIndex.GetOffsetBytes() > 0 {
		if _, err = c.dataHandle.Seek(int64(c.workingSegmentIndex.GetOffsetBytes()), io.SeekStart); err != nil {
			return
		}
	}
	ok = true
	return
}

func (c *Consumer) listenForFilesystemEvents(started chan struct{}) {
	close(started)
	for {
		select {
		case <-c.done:
			return
		default:
		}

		select {
		case <-c.done:
			return
		case event, ok := <-c.fsNotify.Events:
			if !ok {
				return
			}
			if c.indexHandle == nil {
				return
			}
			if event.Has(fsnotify.Create) {
				if strings.HasSuffix(event.Name, ExtData) {
					newSegmentStartAt, _ := parseSegmentOffsetFromPath(event.Name)
					atomic.StoreUint64(&c.partitionActiveSegmentStartOffset, newSegmentStartAt)
					select {
					case <-c.done:
						return
					case c.advanceEvents <- struct{}{}:
					default:
					}
				}
				continue
			}
			if atomic.LoadUint64(&c.partitionActiveSegmentStartOffset) != atomic.LoadUint64(&c.workingSegmentStartOffset) {
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
		case err, ok := <-c.fsNotify.Errors:
			if !ok {
				return
			}
			c.error(fmt.Errorf("diskq; consumer; notify returned error: %w", err))
			return
		}
	}
}

func (c *Consumer) readNextSegmentIndex() (ok bool, err error) {
	var indexStat fs.FileInfo
	indexStat, err = c.indexHandle.Stat()
	if err != nil {
		return
	}
	currentIndexPosition, err := c.indexHandle.Seek(0, io.SeekCurrent)
	if err != nil {
		return
	}

	if (indexStat.Size() - currentIndexPosition) < int64(SegmentIndexSizeBytes) {
		if c.isReadingActiveSegment() {
			ok, err = c.waitForNewOffset()
			return
		}
		ok, err = c.advanceToNextSegment()
		return
	}

	if err = binary.Read(c.indexHandle, binary.LittleEndian, &c.workingSegmentIndex); err != nil {
		err = fmt.Errorf("cannot read next working segment index: %w", err)
		return
	}
	ok = true
	return
}

func (c *Consumer) readNextSegmentIndexAndMaybeWaitForDataWrites() (ok bool, err error) {
	ok, err = c.readNextSegmentIndex()
	if err != nil || !ok {
		return
	}
	ok, err = c.maybeWaitForDataWriteEvents()
	return
}

func (c *Consumer) isReadingActiveSegment() bool {
	return atomic.LoadUint64(&c.partitionActiveSegmentStartOffset) == atomic.LoadUint64(&c.workingSegmentStartOffset)
}

func (c *Consumer) advanceToNextSegment() (ok bool, err error) {
	err = c.advanceFilesToNextSegment()
	if err != nil {
		return
	}
	ok, err = c.readNextSegmentIndex()
	return
}

func (c *Consumer) advanceFilesToNextSegment() (err error) {
	var offsets []uint64
	offsets, err = GetPartitionSegmentStartOffsets(c.path, c.partitionIndex)
	if err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot get partition offsets: %w", err))
		return
	}
	_ = c.indexHandle.Close()
	_ = c.dataHandle.Close()

	c.indexWriteEvents = make(chan struct{}, 1)
	c.dataWriteEvents = make(chan struct{}, 1)

	atomic.StoreUint64(&c.workingSegmentStartOffset, c.getNextSegment(offsets))
	c.indexHandle, err = OpenSegmentFileForRead(c.path, c.partitionIndex, c.workingSegmentStartOffset, ExtIndex)
	if err != nil {
		err = fmt.Errorf("diskq; consumer; cannot open index file: %w", err)
		return
	}
	c.dataHandle, err = OpenSegmentFileForRead(c.path, c.partitionIndex, c.workingSegmentStartOffset, ExtData)
	if err != nil {
		err = fmt.Errorf("diskq; consumer; cannot open data file: %w", err)
		return
	}
	return
}

func (c *Consumer) waitForNewOffset() (ok bool, err error) {
	if c.options.EndBehavior == ConsumerEndBehaviorClose {
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
			ok, err = c.advanceToNextSegment()
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
			if indexStat.Size()-indexPosition >= int64(SegmentIndexSizeBytes) {
				break sized
			}
		}
	}

	readErr := binary.Read(c.indexHandle, binary.LittleEndian, &c.workingSegmentIndex)
	if readErr != nil {
		err = fmt.Errorf("diskq; consumer; wait for new offsets; cannot read segment: %w", readErr)
		return
	}

	ok, err = c.maybeWaitForDataWriteEvents()
	return
}

func (c *Consumer) maybeWaitForDataWriteEvents() (ok bool, err error) {
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
	if dataSizeBytes-dataPosition < int64(c.workingSegmentIndex.GetSizeBytes()) {
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
			if dataSizeBytes-dataPosition >= int64(c.workingSegmentIndex.GetSizeBytes()) {
				return
			}
		}
	}
	ok = true
	return
}

func (c *Consumer) getNextSegment(offsets []uint64) (nextWorkingSegment uint64) {
	for _, offset := range offsets {
		if offset > c.workingSegmentStartOffset {
			return offset
		}
	}
	return c.partitionActiveSegmentStartOffset
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
	switch c.options.StartBehavior {
	case ConsumerStartBehaviorAtOffset:
		if c.options.StartOffset < offsets[0] {
			return 0, fmt.Errorf("diskq; consumer; start at offset beyond oldest offset: %d", c.options.StartOffset)
		}
		newestOffset, err := GetSegmentNewestOffset(c.path, c.partitionIndex, offsets[len(offsets)-1])
		if err != nil {
			return 0, err
		}
		if c.options.StartOffset > newestOffset {
			return 0, fmt.Errorf("diskq; consumer; start at offset beyond newest offset: %d", c.options.StartOffset)
		}
		return c.options.StartOffset + 1, nil
	case ConsumerStartBehaviorOldest:
		return offsets[0], nil
	case ConsumerStartBehaviorActiveSegmentOldest:
		return offsets[len(offsets)-1], nil
	case ConsumerStartBehaviorNewest:
		return GetSegmentNewestOffset(c.path, c.partitionIndex, offsets[len(offsets)-1])
	default:
		return 0, fmt.Errorf("diskq; consumer; absurd start at behavior: %d", c.options.StartBehavior)
	}
}
