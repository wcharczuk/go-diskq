package diskq

import (
	"sync"
	"sync/atomic"
)

// [OpenMarkedConsumerGroup] returns a new marked consumer group that reads a given path.
//
// A marked consumer group lets you open a consumer group with automatic progress tracking. It wraps a standard
// consumer group with offset markers for each partition at a known path within the diskq data directory. If opening
// a consumer group with the same name after it's already recorded some offsets, it will resume the consumer for each
// position at the previously recorded offset, overiding [ConsumerOptions.StartBehavior] and [ConsumerOptions.StartOffset] on
// the returned consumer options from the consumer group options [ConsumerGroupOptions.OptionsForPartition] delegate.
//
// To manually record the offset for a given message as successfully processed, use the [MarkedConsumerGroup.SetLatestOffset] helper function
// on the [MarkedConsumerGroup] struct itself.
//
// Alternatively, you can enable [MarkedConsumerGroupOptions.AutoSetLatestOffset] on the options for the marked consumer group which will set the
// latest offset for a message's partition when it's read automatically by the consumer group before it's passed to your channel receiver.
// This is not enabled by default because it is dangerous to make assumptions about if the message was processed successfully, but
// it is implemented here for convenience.
func OpenMarkedConsumerGroup(dataPath, groupName string, options MarkedConsumerGroupOptions) (*MarkedConsumerGroup, error) {
	markedConsumerGroup := &MarkedConsumerGroup{
		groupName:     groupName,
		offsetMarkers: make(map[uint32]*OffsetMarker),

		messages: make(chan MessageWithOffset),
		errors:   make(chan error),
		done:     make(chan struct{}),
		didExit:  make(chan struct{}),
		didStart: make(chan struct{}),
	}
	existingOptionsForConsumer := options.ConsumerGroupOptions.OptionsForConsumer
	options.OptionsForConsumer = func(partitionIndex uint32) (consumerOptions ConsumerOptions, err error) {
		if existingOptionsForConsumer != nil {
			consumerOptions, err = existingOptionsForConsumer(partitionIndex)
			if err != nil {
				return
			}
		}
		var offsetMarker *OffsetMarker
		var found bool
		offsetMarker, found, err = OpenOrCreateOffsetMarker(FormatPathForMarkedConsumerGroupOffsetMarker(dataPath, groupName, partitionIndex), options.OffsetMarkerOptions)
		if err != nil {
			return
		}
		markedConsumerGroup.offsetMarkersMu.Lock()
		markedConsumerGroup.offsetMarkers[partitionIndex] = offsetMarker
		markedConsumerGroup.offsetMarkersMu.Unlock()
		if found {
			offsetMarker.ApplyToConsumerOptions(&consumerOptions)
		}
		return consumerOptions, nil
	}

	cg, err := OpenConsumerGroup(dataPath, options.ConsumerGroupOptions)
	if err != nil {
		return nil, err
	}
	markedConsumerGroup.cg = cg
	markedConsumerGroup.options = options

	go markedConsumerGroup.pipeEvents()
	<-markedConsumerGroup.didStart
	return markedConsumerGroup, nil
}

// MarkedConsumerGroupOptions are options for a marked consumer group.
type MarkedConsumerGroupOptions struct {
	ConsumerGroupOptions
	OffsetMarkerOptions
	AutoSetLatestOffset bool
}

// MarkedConsumerGroup is a wrapped consumer group that has offset markers automatically configured.
type MarkedConsumerGroup struct {
	mu      sync.Mutex
	cg      *ConsumerGroup
	options MarkedConsumerGroupOptions

	messages chan MessageWithOffset
	errors   chan error
	done     chan struct{}
	didExit  chan struct{}
	didStart chan struct{}

	closed uint32

	groupName       string
	offsetMarkersMu sync.Mutex
	offsetMarkers   map[uint32]*OffsetMarker
}

// SetLatestOffset sets the latest offset for a given partition.
func (m *MarkedConsumerGroup) SetLatestOffset(partitionIndex uint32, offset uint64) {
	m.offsetMarkersMu.Lock()
	defer m.offsetMarkersMu.Unlock()

	if offsetMarker, ok := m.offsetMarkers[partitionIndex]; ok && offsetMarker != nil {
		offsetMarker.SetLatestOffset(offset)
	}
}

// Messages returns the messages channel.
//
// As with consumer groups, and consumers generally, you should use the
//
//	msg, ok := <-mcg.Messages()
//
// form of a channel read on this channel to detect when the channel is closed.
func (m *MarkedConsumerGroup) Messages() <-chan MessageWithOffset {
	return m.messages
}

// Errors returns the errors channel.
//
// As with consumer groups, and consumers generally, you should use the
//
//	err, ok := <-mcg.Errors()
//
// form of a channel read on this channel to detect when the channel is closed.
func (m *MarkedConsumerGroup) Errors() <-chan error {
	return m.errors
}

// Close closes the consumer.
func (m *MarkedConsumerGroup) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if atomic.LoadUint32(&m.closed) == 1 {
		return nil
	}
	atomic.StoreUint32(&m.closed, 1)
	close(m.done)
	<-m.didExit
	return nil
}

//
// internal methods
//

func (m *MarkedConsumerGroup) pipeEvents() {
	defer func() {
		close(m.messages)
		close(m.errors)
		_ = m.closeOffsetMarkers()
		_ = m.closeConsumers()
		close(m.didExit)
	}()
	close(m.didStart)

	var msg MessageWithOffset
	var ok bool
	var err error
	for {
		if atomic.LoadUint32(&m.closed) == 1 {
			return
		}
		select {
		case <-m.done:
			return
		case msg, ok = <-m.cg.Messages():
			if !ok {
				return
			}
			select {
			case <-m.done:
				return
			case m.messages <- msg:
				if m.options.AutoSetLatestOffset {
					m.SetLatestOffset(msg.PartitionIndex, msg.Offset)
				}
				continue
			}
		case err, ok = <-m.cg.Errors():
			if !ok {
				return
			}
			select {
			case <-m.done:
				return
			case m.errors <- err:
				continue
			}
		}
	}
}

func (m *MarkedConsumerGroup) closeOffsetMarkers() (err error) {
	for _, om := range m.offsetMarkers {
		if err = om.Close(); err != nil {
			return
		}
	}
	return
}

func (m *MarkedConsumerGroup) closeConsumers() (err error) {
	for _, om := range m.cg.consumers {
		if err = om.Close(); err != nil {
			return
		}
	}
	return
}
