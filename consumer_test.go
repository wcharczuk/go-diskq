package diskq

import (
	"fmt"
	"strings"
	"testing"
)

func Test_Consumer_startFromBeginning(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             testPath,
		PartitionCount:   1,
		SegmentSizeBytes: 1024,
	}

	dq, err := New(cfg)
	assert_noerror(t, err)
	defer func() { _ = dq.Close() }()

	var offset uint64
	for x := 0; x < 64; x++ {
		_, offset, err = dq.Push(Message{
			PartitionKey: fmt.Sprintf("data-%d", x),
			Data:         []byte(strings.Repeat("a", 64)),
		})
		assert_noerror(t, err)
		assert_equal(t, x, offset)
	}

	c, err := OpenConsumer(testPath, 0, ConsumerOptions{
		StartBehavior: ConsumerStartBehaviorOldest,
	})
	assert_noerror(t, err)
	defer func() { _ = c.Close() }()

	for x := 0; x < 64; x++ {
		select {
		case err = <-c.Errors():
		default:
		}
		assert_noerror(t, err)
		msg := <-c.Messages()
		assert_equal(t, x, msg.Offset)
		assert_equal(t, fmt.Sprintf("data-%d", x), msg.Message.PartitionKey)
	}
}

func Test_Consumer_startFromBeginning_endAtLatest(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             testPath,
		PartitionCount:   1,
		SegmentSizeBytes: 1024,
	}

	dq, err := New(cfg)
	assert_noerror(t, err)
	defer func() { _ = dq.Close() }()

	var offset uint64
	for x := 0; x < 64; x++ {
		_, offset, err = dq.Push(Message{
			PartitionKey: fmt.Sprintf("data-%d", x),
			Data:         []byte(strings.Repeat("a", 64)),
		})
		assert_noerror(t, err)
		assert_equal(t, x, offset)
	}

	c, err := OpenConsumer(testPath, 0, ConsumerOptions{
		StartBehavior: ConsumerStartBehaviorOldest,
		EndBehavior:   ConsumerEndBehaviorClose,
	})
	assert_noerror(t, err)
	defer func() { _ = c.Close() }()

	var x int
messageloop:
	for {
		select {
		case msg, ok := <-c.Messages():
			if !ok {
				break messageloop
			}
			assert_equal(t, fmt.Sprintf("data-%d", x), msg.Message.PartitionKey)
			x++
		case err, ok := <-c.Errors():
			if !ok {
				break messageloop
			}
			assert_noerror(t, err)
		}
	}
}

func Test_ParseConsumerStartBehavior(t *testing.T) {
	tests := [...]struct {
		Raw       string
		Expect    ConsumerStartBehavior
		ExpectErr error
	}{
		{Raw: "", ExpectErr: fmt.Errorf("absurd consumer start behavior: %s", "")},
		{Raw: "not-a-real-value", ExpectErr: fmt.Errorf("absurd consumer start behavior: %s", "not-a-real-value")},
		{Raw: "oldest", Expect: ConsumerStartBehaviorOldest},
		{Raw: "OLDEST", Expect: ConsumerStartBehaviorOldest},
		{Raw: "  oldest  ", Expect: ConsumerStartBehaviorOldest},
		{Raw: "  OLDEST  ", Expect: ConsumerStartBehaviorOldest},
		{Raw: "at-offset", Expect: ConsumerStartBehaviorAtOffset},
		{Raw: "active-oldest", Expect: ConsumerStartBehaviorActiveSegmentOldest},
		{Raw: "newest", Expect: ConsumerStartBehaviorNewest},

		{Raw: ConsumerStartBehaviorOldest.String(), Expect: ConsumerStartBehaviorOldest},
		{Raw: ConsumerStartBehaviorAtOffset.String(), Expect: ConsumerStartBehaviorAtOffset},
		{Raw: ConsumerStartBehaviorActiveSegmentOldest.String(), Expect: ConsumerStartBehaviorActiveSegmentOldest},
		{Raw: ConsumerStartBehaviorNewest.String(), Expect: ConsumerStartBehaviorNewest},
	}

	for _, tc := range tests {
		actual, actualErr := ParseConsumerStartBehavior(tc.Raw)
		if tc.ExpectErr != nil {
			assert_notnil(t, actualErr)
			assert_equal(t, tc.ExpectErr.Error(), actualErr.Error())
		} else {
			assert_equal(t, tc.Expect, actual)
		}
	}
}

func Test_ParseConsumerEndBehavior(t *testing.T) {
	tests := [...]struct {
		Raw       string
		Expect    ConsumerEndBehavior
		ExpectErr error
	}{
		{Raw: "", ExpectErr: fmt.Errorf("absurd consumer end behavior: %s", "")},
		{Raw: "not-a-real-value", ExpectErr: fmt.Errorf("absurd consumer end behavior: %s", "not-a-real-value")},
		{Raw: "wait", Expect: ConsumerEndBehaviorWait},
		{Raw: "  wait  ", Expect: ConsumerEndBehaviorWait},
		{Raw: "WAIT", Expect: ConsumerEndBehaviorWait},
		{Raw: "  WAIT  ", Expect: ConsumerEndBehaviorWait},

		{Raw: "at-offset", Expect: ConsumerEndBehaviorAtOffset},
		{Raw: "close", Expect: ConsumerEndBehaviorClose},

		{Raw: ConsumerEndBehaviorWait.String(), Expect: ConsumerEndBehaviorWait},
		{Raw: ConsumerEndBehaviorAtOffset.String(), Expect: ConsumerEndBehaviorAtOffset},
		{Raw: ConsumerEndBehaviorClose.String(), Expect: ConsumerEndBehaviorClose},
	}

	for _, tc := range tests {
		actual, actualErr := ParseConsumerEndBehavior(tc.Raw)
		if tc.ExpectErr != nil {
			assert_notnil(t, actualErr)
			assert_equal(t, tc.ExpectErr.Error(), actualErr.Error())
		} else {
			assert_equal(t, tc.Expect, actual)
		}
	}
}
