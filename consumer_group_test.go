package diskq

import (
	"fmt"
	"strings"
	"testing"
)

func Test_ConsumerGroup_readToEnd(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             testPath,
		PartitionCount:   3,
		SegmentSizeBytes: 1024,
	}

	dq, err := New(cfg)
	assert_noerror(t, err)
	defer func() { _ = dq.Close() }()

	for x := 0; x < 64; x++ {
		_, _, err = dq.Push(Message{
			PartitionKey: fmt.Sprintf("data-%d", x),
			Data:         []byte(strings.Repeat("a", 64)),
		})
		assert_noerror(t, err)
	}

	cg, err := OpenConsumerGroup(testPath, ConsumerGroupOptions(ConsumerOptions{
		StartBehavior: ConsumerStartBehaviorOldest,
		EndBehavior:   ConsumerEndBehaviorClose,
	}))
	assert_noerror(t, err)
	defer func() { _ = cg.Close() }()

	var messages []MessageWithOffset
	for {
		msg, ok := <-cg.Messages()
		if !ok {
			break
		}
		messages = append(messages, msg)
	}
	assert_equal(t, 64, len(messages))
}
