package diskq

import (
	"testing"
)

func Test_ConsumerGroup_readToEnd(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Options{
		PartitionCount:   3,
		SegmentSizeBytes: 1024,
	}

	dq, err := New(testPath, cfg)
	assert_noerror(t, err)
	defer func() { _ = dq.Close() }()

	for x := 0; x < 64; x++ {
		_, _, err = dq.Push(testMessage(int(x), 128))
		assert_noerror(t, err)
	}
	cg, err := OpenConsumerGroup(testPath, ConsumerGroupOptionsFromConsumerOptions(ConsumerOptions{
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
