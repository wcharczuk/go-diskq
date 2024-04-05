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
	var fsEvents *fsnotify.Watcher
	if options.EndBehavior != ConsumerEndBehaviorClose {
		fsEvents, err = fsnotify.NewWatcher()
		if err != nil {
			return nil, err
		}
	}
	c := &Consumer{
		path:             path,
		partitionIndex:   partitionIndex,
		options:          options,
		fsEvents:         fsEvents,
		messages:         make(chan MessageWithOffset),
		errors:           make(chan error, 1),
		done:             make(chan struct{}),
		didStart:         make(chan struct{}),
		advanceEvents:    make(chan struct{}, 1),
		indexWriteEvents: make(chan struct{}, 1),
		dataWriteEvents:  make(chan struct{}, 1),
	}
	if err := c.initialize(); err != nil {
		_ = c.Close()
		return nil, err
	}
	go c.read()
	<-c.didStart
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
	mu sync.Mutex

	path           string
	partitionIndex uint32
	options        ConsumerOptions
	fsEvents       *fsnotify.Watcher

	messages         chan MessageWithOffset
	errors           chan error
	advanceEvents    chan struct{}
	indexWriteEvents chan struct{}
	dataWriteEvents  chan struct{}
	didStart         chan struct{}
	done             chan struct{}
	closed           uint32

	partitionActiveSegmentStartOffset uint64
	workingSegmentStartOffset         uint64
	workingSegmentIndex               SegmentIndex

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

	if atomic.LoadUint32(&c.closed) == 1 {
		return nil
	}

	close(c.done)
	atomic.StoreUint32(&c.closed, 1)

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
	if c.fsEvents != nil {
		_ = c.fsEvents.Close()
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

	ok, err := c.setupFirstRead()
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
		if atomic.LoadUint32(&c.closed) == 1 {
			return
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
		if atomic.LoadUint32(&c.closed) == 1 {
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

func (c *Consumer) initialize() (err error) {
	partitionPath := FormatPathForPartition(c.path, c.partitionIndex)
	offsets, err := GetPartitionSegmentStartOffsets(c.path, c.partitionIndex)
	if err != nil {
		return
	}
	atomic.StoreUint64(&c.partitionActiveSegmentStartOffset, offsets[len(offsets)-1])
	effectiveConsumeAtOffset, err := c.determineEffectiveConsumeAtOffset(offsets)
	if err != nil {
		return
	}

	workingSegmentOffset, workingSegmentOffsetOK := GetSegmentStartOffsetForOffset(offsets, effectiveConsumeAtOffset)
	if !workingSegmentOffsetOK {
		err = fmt.Errorf("diskq; consumer; invalid working segment offset")
		return
	}

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
		var indexStat fs.FileInfo
		indexStat, err = c.indexHandle.Stat()
		if err != nil {
			return
		}
		indexSizeBytes := indexStat.Size()
		if indexSizeBytes < indexSeekToBytes {
			err = fmt.Errorf("diskq; consumer; invalid start offset, index file is too small")
			return
		}
		if _, err = c.indexHandle.Seek(indexSeekToBytes, io.SeekStart); err != nil {
			return
		}
	}

	if c.options.EndBehavior != ConsumerEndBehaviorClose {
		listenFilesystemStarted := make(chan struct{})
		go c.listenForFilesystemEvents(listenFilesystemStarted)
		<-listenFilesystemStarted
		err = c.fsEvents.Add(partitionPath)
		if err != nil {
			return
		}
	}
	return
}

func (c *Consumer) setupFirstRead() (ok bool, err error) {
	ok, err = c.readNextSegmentIndex()
	if !ok || err != nil {
		return
	}

	ok, err = c.hasEnoughDataForRead()
	if err != nil {
		return
	}
	if !ok {
		ok, err = c.maybeWaitForDataWriteEventsUntilSizeGoal(int64(c.workingSegmentIndex.GetOffsetBytes() + c.workingSegmentIndex.GetSizeBytes()))
		if !ok || err != nil {
			return
		}
	}
	if c.workingSegmentIndex.GetOffsetBytes() > 0 {
		if _, err = c.dataHandle.Seek(int64(c.workingSegmentIndex.GetOffsetBytes()), io.SeekStart); err != nil {
			return
		}
	}
	return
}

func (c *Consumer) readNextSegmentIndexAndMaybeWaitForDataWrites() (ok bool, err error) {
	ok, err = c.readNextSegmentIndex()
	if !ok || err != nil {
		return
	}
	sizeGoal := int64(c.workingSegmentIndex.GetOffsetBytes() + c.workingSegmentIndex.GetSizeBytes())
	ok, err = c.maybeWaitForDataWriteEventsUntilSizeGoal(sizeGoal)
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
		case event, ok := <-c.fsEvents.Events:
			if !ok {
				return
			}
			ok = c.handleFilesystemEvent(event)
			if !ok {
				return
			}

		case err, ok := <-c.fsEvents.Errors:
			if !ok {
				return
			}
			c.error(fmt.Errorf("diskq; consumer; notify returned error: %w", err))
			return
		}
	}
}

func (c *Consumer) handleFilesystemEvent(event fsnotify.Event) (ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.indexHandle == nil {
		return
	}
	if event.Has(fsnotify.Create) {
		if strings.HasSuffix(event.Name, ExtIndex) {
			newSegmentStartAt, err := parseSegmentOffsetFromPath(event.Name)
			if err != nil {
				c.error(err)
				return
			}
			atomic.StoreUint64(&c.partitionActiveSegmentStartOffset, newSegmentStartAt)
			select {
			case c.advanceEvents <- struct{}{}:
			default:
			}
		}
	} else if event.Has(fsnotify.Write) {
		if event.Name == c.indexHandle.Name() {
			select {
			case c.indexWriteEvents <- struct{}{}:
			default:
			}
		}
		if event.Name == c.dataHandle.Name() {
			select {
			case c.dataWriteEvents <- struct{}{}:
			default:
			}
		}
	}
	ok = true
	return
}

func (c *Consumer) readNextSegmentIndex() (ok bool, err error) {
	ok, err = c.hasEnoughIndexForRead()
	if err != nil {
		return
	}
	if !ok {
		if c.isReadingActiveSegment() {
			// if we _don't_ have enough data for a read, and we're on the active
			// segment, and we are set to close on end, return
			// assumes ok=false,err=nil
			if c.options.EndBehavior == ConsumerEndBehaviorClose {
				return
			}
			ok, err = c.maybeWaitForNextOffsetOrAdvance()
			if !ok || err != nil {
				return
			}
		} else {
			// we're not on the active segment, so we _should_ be able to advance
			// to another segment.
			err = c.advanceFilesToNextSegment()
			if err != nil {
				return
			}
			// check if we can read immediately
			ok, err = c.hasEnoughIndexForRead()
			if err != nil {
				return
			}
			// if we have enough index data for a read, spectacular! lets read.
			if !ok {
				// we might have kicked over to an empty active segment; we should still close!
				// this is derrived by hasEnoughIndexForRead returning false, and checking if
				// we're on the active segment and that the options mean we should close.
				if c.isReadingActiveSegment() && c.options.EndBehavior == ConsumerEndBehaviorClose {
					return
				}
				ok, err = c.maybeWaitForIndexWrite()
				if !ok || err != nil {
					return
				}
			}
		}
	}
	if err = binary.Read(c.indexHandle, binary.LittleEndian, &c.workingSegmentIndex); err != nil {
		err = fmt.Errorf("diskq; consumer; cannot read next working segment index: %w", err)
		return
	}
	return
}

func (c *Consumer) advanceFilesToNextSegment() (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var offsets []uint64
	offsets, err = GetPartitionSegmentStartOffsets(c.path, c.partitionIndex)
	if err != nil {
		c.error(fmt.Errorf("diskq; consumer; cannot get partition start offsets: %w", err))
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

func (c *Consumer) maybeWaitForNextOffsetOrAdvance() (ok bool, err error) {
	var didAdvance bool
	didAdvance, ok, err = c.maybeWaitForIndexWriteOrAdvance()
	if err != nil {
		return
	}
	if !ok {
		return
	}
	if didAdvance {
		ok, err = c.maybeWaitForIndexWrite()
		if !ok || err != nil {
			return
		}
	}
	return
}

func (c *Consumer) maybeWaitForIndexWrite() (ok bool, err error) {
	ok, err = c.hasEnoughIndexForRead()
	if ok || err != nil {
		return
	}
	for {
		select {
		case <-c.done:
			return
		case _, ok = <-c.indexWriteEvents:
			if !ok {
				return
			}
			ok, err = c.hasEnoughIndexForRead()
			if err != nil {
				return
			}
			if ok {
				return
			}
		}
	}
}

func (c *Consumer) maybeWaitForIndexWriteOrAdvance() (didAdvance bool, ok bool, err error) {
	ok, err = c.hasEnoughIndexForRead()
	if ok || err != nil {
		return
	}

	for {
		select {
		case <-c.done:
			return
		case _, ok = <-c.advanceEvents:
			if !ok {
				return
			}
			err = c.advanceFilesToNextSegment()
			if err != nil {
				return
			}
			// ok is already true!
			didAdvance = true
			return
		case _, ok = <-c.indexWriteEvents:
			if !ok {
				return
			}
			ok, err = c.hasEnoughIndexForRead()
			if err != nil {
				return
			}
			if ok {
				return
			}
		}
	}
}

func (c *Consumer) hasEnoughIndexForRead() (ok bool, err error) {
	var indexStat fs.FileInfo
	indexStat, err = c.indexHandle.Stat()
	if err != nil {
		return
	}
	var indexPosition int64
	indexPosition, err = c.indexHandle.Seek(0, io.SeekCurrent)
	if err != nil {
		return
	}
	indexSizeBytes := indexStat.Size()
	needIndexBytes := indexPosition + int64(SegmentIndexSizeBytes)
	ok = indexSizeBytes >= needIndexBytes
	return
}

func (c *Consumer) hasEnoughDataForRead() (ok bool, err error) {
	var dataStat fs.FileInfo
	dataStat, err = c.dataHandle.Stat()
	if err != nil {
		return
	}
	dataSizeBytes := dataStat.Size()
	needDataBytes := int64(c.workingSegmentIndex.GetOffsetBytes() + c.workingSegmentIndex.GetSizeBytes())
	ok = dataSizeBytes >= needDataBytes
	return
}

func (c *Consumer) isReadingActiveSegment() bool {
	return atomic.LoadUint64(&c.partitionActiveSegmentStartOffset) == atomic.LoadUint64(&c.workingSegmentStartOffset)
}

func (c *Consumer) maybeWaitForDataWriteEventsUntilSizeGoal(sizeGoal int64) (ok bool, err error) {
	if !c.isReadingActiveSegment() {
		ok = true
		return
	}
	var dataStat fs.FileInfo
	dataStat, err = c.dataHandle.Stat()
	if err != nil {
		return
	}
	dataSizeBytes := dataStat.Size()
	for dataSizeBytes < sizeGoal {
		select {
		case <-c.done:
			return
		case _, eventOK := <-c.dataWriteEvents:
			if !eventOK {
				return
			}
			dataStat, err = c.dataHandle.Stat()
			if err != nil {
				return
			}
			dataSizeBytes = dataStat.Size()
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
	if atomic.LoadUint32(&c.closed) == 1 {
		return
	}
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
			return 0, fmt.Errorf("diskq; consumer; start at offset before than oldest offset: %d", c.options.StartOffset)
		}
		newestOffset, err := GetSegmentNewestOffset(c.path, c.partitionIndex, offsets[len(offsets)-1])
		if err != nil {
			return 0, err
		}
		if c.options.StartOffset > newestOffset {
			return 0, fmt.Errorf("diskq; consumer; start at offset beyond newest offset: %d", c.options.StartOffset)
		}
		return c.options.StartOffset, nil
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
