package diskq

import (
	"context"
	"os"
)

func openConsumer(ctx context.Context, cfg Config, partitionIndex uint32, startAtOffset uint64, horizonBehavior ConsumerHorizonBehavior) (<-chan Message, error) {
	output := make(chan Message)
	c := &consumer{
		cfg:             cfg,
		partitionIndex:  partitionIndex,
		startAtOffset:   startAtOffset,
		horizonBehavior: horizonBehavior,
		messages:        output,
	}
	go c.read(ctx)
	return output, nil
}

type ConsumerHorizonBehavior uint8

const (
	ConsumerStopAtHorizon = iota
	ConsumerListenAtHorizon
)

type consumer struct {
	cfg             Config
	partitionIndex  uint32
	startAtOffset   uint64
	horizonBehavior ConsumerHorizonBehavior
	index           *os.File
	data            *os.File
	messages        chan Message
}

func (c *consumer) read(ctx context.Context) {
}
