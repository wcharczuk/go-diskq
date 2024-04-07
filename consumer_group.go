package diskq

import (
	"sync"
	"sync/atomic"
	"time"
)

// OpenConsumerGroup opens a new consumer group.
//
// A consumer group reads from all partitions at once, and scans for new partitions
// if they're added, mapping each partition to an underlying consumer.
func OpenConsumerGroup(dataPath string, options ConsumerGroupOptions) (*ConsumerGroup, error) {
	cg := &ConsumerGroup{
		id:            UUIDv4(),
		path:          dataPath,
		options:       options,
		consumers:     make(map[uint32]*Consumer),
		offsetMarkers: make(map[uint32]*OffsetMarker),
		consumerExits: make(chan struct{}),
		messages:      make(chan MessageWithOffset),
		errors:        make(chan error),
		didStart:      make(chan struct{}),
		done:          make(chan struct{}),
	}
	go cg.start()
	<-cg.didStart
	return cg, nil
}

// ConsumerGroupOptions are extra options for consumer groups.
type ConsumerGroupOptions struct {
	OptionsForConsumer    func(uint32) (ConsumerOptions, error)
	OnCloseConsumer       func(uint32) error
	PartitionScanInterval time.Duration
}

// PartitionScanIntervalOrDefault returns the partition scan interval or a default.
func (cg ConsumerGroupOptions) PartitionScanIntervalOrDefault() time.Duration {
	if cg.PartitionScanInterval > 0 {
		return cg.PartitionScanInterval
	}
	return 5 * time.Second
}

// ConsumerGroup is a consumer that reads from all partitions at once, and periodically
// scans for new partitions, or stops reading partitions that may have been deleted.
//
// Partitions are read from their start offset by default, and you can only control
// the end behavior in practice.
type ConsumerGroup struct {
	mu              sync.Mutex
	id              UUID
	path            string
	options         ConsumerGroupOptions
	consumers       map[uint32]*Consumer
	offsetMarkers   map[uint32]*OffsetMarker
	consumerExits   chan struct{}
	messages        chan MessageWithOffset
	errors          chan error
	didStart        chan struct{}
	done            chan struct{}
	activeConsumers int32
}

// Messages returns a channel that will receive messages from
// the individual consumers.
//
// You should use the `msg, ok := <-cg.Messages()` form of channel
// reads when reading this channel to detect if the channel
// is closed, which would indicate the all of the consumer group
// consumers have reached the end of their respective partitions
// with the end behavior of "close".
func (cg *ConsumerGroup) Messages() <-chan MessageWithOffset {
	return cg.messages
}

// Errors returns a channel that will receive errors from the
// individual consumers.
func (cg *ConsumerGroup) Errors() <-chan error {
	return cg.errors
}

// Close closes the consumer groups and all the consumers
// it may have started.
//
// Close is safe to call more than once.
func (cg *ConsumerGroup) Close() error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if cg.done == nil {
		return nil
	}

	close(cg.done)
	cg.done = nil
	for _, consumer := range cg.consumers {
		_ = cg.onCloseConsumer(consumer.partitionIndex)
		_ = consumer.Close()
	}
	close(cg.messages)
	close(cg.errors)
	return nil
}

//
// internal methods
//

func (cg *ConsumerGroup) start() {
	defer func() {
		if err := cg.Close(); err != nil {
			cg.error(err)
		}
	}()

	close(cg.didStart)
	cg.didStart = nil

	ticker := time.NewTicker(cg.options.PartitionScanIntervalOrDefault())
	defer ticker.Stop()
	if ok := cg.scanForPartitions(); !ok {
		return
	}

	for {
		select {
		case <-cg.done:
			return
		default:
		}

		select {
		case <-cg.done:
			return
		case <-cg.consumerExits:
			if atomic.AddInt32(&cg.activeConsumers, -1) == 0 {
				return
			}
		case <-ticker.C:
			if ok := cg.scanForPartitions(); !ok {
				return
			}
		}
	}
}

func (cg *ConsumerGroup) onCreateConsumer(partitionIndex uint32) (ConsumerOptions, error) {
	if cg.options.OptionsForConsumer != nil {
		return cg.options.OptionsForConsumer(partitionIndex)
	}
	return ConsumerOptions{}, nil
}

func (cg *ConsumerGroup) onCloseConsumer(partitionIndex uint32) error {
	if cg.options.OnCloseConsumer != nil {
		return cg.options.OnCloseConsumer(partitionIndex)
	}
	return nil
}

func (cg *ConsumerGroup) scanForPartitions() (ok bool) {
	partitions, err := getPartitionsLookup(cg.path)
	if err != nil {
		if ok = cg.error(err); !ok {
			return
		}
	}
	for partitionIndex := range partitions {
		if _, hasConsumer := cg.consumers[partitionIndex]; !hasConsumer {
			consumerOptions, err := cg.onCreateConsumer(partitionIndex)
			if err != nil {
				if ok = cg.error(err); !ok {
					return
				}
			}
			consumer, err := OpenConsumer(cg.path, partitionIndex, consumerOptions)
			if err != nil {
				if ok = cg.error(err); !ok {
					return
				}
			}
			started := make(chan struct{})
			atomic.AddInt32(&cg.activeConsumers, 1)
			go cg.pipeEvents(consumer, started)
			<-started
			cg.consumers[partitionIndex] = consumer
		}
	}
	var toRemove []uint32
	for partitionIndex, consumer := range cg.consumers {
		if _, hasConsumer := partitions[partitionIndex]; !hasConsumer {
			_ = consumer.Close()
			toRemove = append(toRemove, partitionIndex)
		}
	}
	for _, id := range toRemove {
		if err := cg.onCloseConsumer(id); err != nil {
			if ok = cg.error(err); !ok {
				return
			}
		}
		atomic.AddInt32(&cg.activeConsumers, -1)
		delete(cg.consumers, id)
	}
	ok = true
	return
}

func (cg *ConsumerGroup) pipeEvents(consumer *Consumer, consumerStarted chan struct{}) {
	defer func() {
		cg.consumerExits <- struct{}{}
	}()
	close(consumerStarted)
	for {
		select {
		case <-cg.done:
			return
		case msg, ok := <-consumer.messages:
			if !ok {
				return
			}
			select {
			case <-cg.done:
				return
			case cg.messages <- msg:
			}
		case err, ok := <-consumer.errors:
			if !ok {
				return
			}
			select {
			case <-cg.done:
				return
			case cg.errors <- err:
			}
		}
	}
}

func (cg *ConsumerGroup) error(err error) (ok bool) {
	if cg.done != nil {
		select {
		case cg.errors <- err:
			ok = true
			return
		case <-cg.done:
			return
		}
	}
	return
}
