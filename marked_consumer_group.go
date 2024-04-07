package diskq

import (
	"path/filepath"
	"sync"
)

// OpenMarkedConsumerGroup returns a new marked consumer group.
func OpenMarkedConsumerGroup(dataPath, groupName string, options ConsumerGroupOptions, offsetMarkerOptions OffsetMarkerOptions) (*MarkedConsumerGroup, error) {
	markedConsumerGroup := &MarkedConsumerGroup{
		groupName:     groupName,
		offsetMarkers: make(map[uint32]*OffsetMarker),

		messages: make(chan MessageWithOffset),
		errors:   make(chan error),
		done:     make(chan struct{}),
		didExit:  make(chan struct{}),
		didStart: make(chan struct{}),
	}
	existingOptionsForConsumer := options.OptionsForConsumer
	options.OptionsForConsumer = func(partitionIndex uint32) (options ConsumerOptions, err error) {
		if existingOptionsForConsumer != nil {
			options, err = existingOptionsForConsumer(partitionIndex)
			if err != nil {
				return
			}
		}
		var offsetMarker *OffsetMarker
		offsetMarker, _, err = OpenOrCreateOffsetMarker(FormatPathForMarkedConsumerGroupOffsetMarker(dataPath, groupName, partitionIndex), offsetMarkerOptions)
		if err != nil {
			return
		}

		markedConsumerGroup.mu.Lock()
		markedConsumerGroup.offsetMarkers[partitionIndex] = offsetMarker
		markedConsumerGroup.mu.Unlock()

		offsetMarker.ApplyToConsumerOptions(&options)
		return options, nil
	}

	cg, err := OpenConsumerGroup(dataPath, options)
	if err != nil {
		return nil, err
	}
	markedConsumerGroup.cg = cg

	go markedConsumerGroup.pipeEvents()
	<-markedConsumerGroup.didStart
	return markedConsumerGroup, nil
}

// MarkedConsumerGroup is a wrapped consumer group that has offset markers automatically configured.
type MarkedConsumerGroup struct {
	mu sync.Mutex
	cg *ConsumerGroup

	messages chan MessageWithOffset
	errors   chan error
	done     chan struct{}
	didExit  chan struct{}
	didStart chan struct{}

	groupName     string
	offsetMarkers map[uint32]*OffsetMarker
}

// SetLatestOffset sets the latest offset for a given partition.
func (m *MarkedConsumerGroup) SetLatestOffset(partitionIndex uint32, offset uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if offsetMarker, ok := m.offsetMarkers[partitionIndex]; ok && offsetMarker != nil {
		offsetMarker.SetLatestOffset(offset)
	}
}

// Messages returns the messages channel.
func (m *MarkedConsumerGroup) Messages() <-chan MessageWithOffset {
	return m.messages
}

// Errors returns the errors channel.
func (m *MarkedConsumerGroup) Errors() <-chan error {
	return m.errors
}

func (m *MarkedConsumerGroup) pipeEvents() {
	defer func() {
		close(m.messages)
		close(m.errors)
		close(m.didExit)

		_ = m.Close()
	}()
	close(m.didStart)

	var msg MessageWithOffset
	var ok bool
	var err error
	for {
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
				continue
			}
			// AUTOSET ???
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

func FormatPathForMarkedConsumerGroupOffsetMarker(dataPath, groupName string, partitionIndex uint32) string {
	return filepath.Join(dataPath, "groups", groupName, FormatPartitionIndexForPath(partitionIndex))
}
