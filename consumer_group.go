package diskq

import (
	"sync"
	"sync/atomic"
	"time"
)

// OpenConsumerGropu opens a new consumer group.
func OpenConsumerGroup(path string, options func(uint32) ConsumerOptions) (*ConsumerGroup, error) {
	cg := &ConsumerGroup{
		path:          path,
		options:       options,
		consumers:     make(map[uint32]*Consumer),
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

// ConsumerGroupOptions returns a given set of consumer options for any
// consumers created for a consumer group.
func ConsumerGroupOptions(opts ConsumerOptions) func(uint32) ConsumerOptions {
	return func(_ uint32) ConsumerOptions { return opts }
}

// ConsumerGroup is a consumer that reads from all partitions at once, and periodically
// scans for new partitions, or stops reading partitions that may have been deleted.
//
// Partitions are read from their start offset by default, and you can only control
// the end behavior in practice.
type ConsumerGroup struct {
	mu            sync.Mutex
	path          string
	options       func(uint32) ConsumerOptions
	consumers     map[uint32]*Consumer
	consumerExits chan struct{}
	messages      chan MessageWithOffset
	errors        chan error
	didStart      chan struct{}
	done          chan struct{}

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

	ticker := time.NewTicker(5 * time.Second)
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

func (cg *ConsumerGroup) consumerOptionsForPartition(partitionIndex uint32) ConsumerOptions {
	if cg.options != nil {
		return cg.options(partitionIndex)
	}
	return ConsumerOptions{}
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
			consumer, err := OpenConsumer(cg.path, partitionIndex, cg.consumerOptionsForPartition(partitionIndex))
			if err != nil {
				if ok = cg.error(err); !ok {
					return
				}
			}
			started := make(chan struct{})
			go cg.pipeEvents(consumer, started)
			atomic.AddInt32(&cg.activeConsumers, 1)
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
