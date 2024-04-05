package diskq

import (
	"testing"
	"time"
)

func Test_ConsumerGroup_readToEnd(t *testing.T) {
	testPath, done := tempDir()
	t.Cleanup(done)

	dq, err := New(testPath, Options{
		PartitionCount:   3,
		SegmentSizeBytes: 1024,
	})
	assert_noerror(t, err)
	defer func() { _ = dq.Close() }()

	for x := 0; x < 64; x++ {
		_, _, err = dq.Push(testMessage(int(x), 128))
		assert_noerror(t, err)
	}
	cg, err := OpenConsumerGroup(testPath, ConsumerGroupOptions{
		OptionsForConsumer: func(_ uint32) (ConsumerOptions, error) {
			return ConsumerOptions{
				StartBehavior: ConsumerStartBehaviorOldest,
				EndBehavior:   ConsumerEndBehaviorClose,
			}, nil
		},
	})
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

func Test_ConsumerGroup_addsPartitions(t *testing.T) {
	testPath, done := tempDir()
	t.Cleanup(done)

	dq, err := New(testPath, Options{
		PartitionCount:   1,
		SegmentSizeBytes: 1024,
	})
	assert_noerror(t, err)

	for x := 0; x < 8; x++ {
		_, _, err = dq.Push(testMessage(int(x), 128))
		assert_noerror(t, err)
	}
	cg, err := OpenConsumerGroup(testPath, ConsumerGroupOptions{
		OptionsForConsumer: func(_ uint32) (ConsumerOptions, error) {
			return ConsumerOptions{
				StartBehavior: ConsumerStartBehaviorOldest,
				EndBehavior:   ConsumerEndBehaviorWait,
			}, nil
		},
		PartitionScanInterval: 100 * time.Millisecond,
	})
	assert_noerror(t, err)
	defer func() { _ = cg.Close() }()

	var messages []MessageWithOffset
	for x := 0; x < 8; x++ {
		msg, ok := <-cg.Messages()
		if !ok {
			break
		}
		messages = append(messages, msg)
	}
	assert_equal(t, 8, len(messages))
	_ = dq.Close()

	dq, err = New(testPath, Options{
		PartitionCount:   3,
		SegmentSizeBytes: 1024,
	})
	assert_noerror(t, err)

	var partition uint32
	partitions := make(map[uint32]int)
	for x := 0; x < 24; x++ {
		partition, _, err = dq.Push(testMessage(int(x), 128))
		assert_noerror(t, err)
		partitions[partition]++
	}
	for x := 0; x < 24; x++ {
		msg, ok := <-cg.Messages()
		if !ok {
			break
		}
		messages = append(messages, msg)
	}
	assert_equal(t, 32, len(messages))
	for x := 0; x < 3; x++ {
		assert_notequal(t, 0, partitions[uint32(x)])
	}
}
