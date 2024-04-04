package diskq

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
)

func Test_Consumer_startFromBeginning(t *testing.T) {
	testPath, done := tempDir()
	t.Cleanup(done)

	cfg := Options{
		PartitionCount:   1,
		SegmentSizeBytes: 1024,
	}

	dq, err := New(testPath, cfg)
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

func Test_Consumer_startAtOffset_newest(t *testing.T) {
	testPath, done := tempDir()
	t.Cleanup(done)

	cfg := Options{
		PartitionCount:   1,
		SegmentSizeBytes: 1024,
	}

	dq, err := New(testPath, cfg)
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
		StartBehavior: ConsumerStartBehaviorAtOffset,
		StartOffset:   offset,
	})
	assert_noerror(t, err)
	defer func() { _ = c.Close() }()

	gotMessages := make(chan MessageWithOffset, 3)
	started := make(chan struct{})
	go func() {
		close(started)
		for {
			select {
			case err, ok := <-c.Errors():
				if !ok {
					return
				}
				assert_noerror(t, err)
			case msg, ok := <-c.Messages():
				if !ok {
					return
				}
				gotMessages <- msg
			}
		}
	}()
	<-started

	var newOffset uint64
	_, newOffset, err = dq.Push(Message{
		PartitionKey: fmt.Sprintf("data-%d", offset+1),
		Data:         []byte(strings.Repeat("a", 64)),
	})
	assert_noerror(t, err)
	assert_equal(t, newOffset, offset+1)
	_, newOffset, err = dq.Push(Message{
		PartitionKey: fmt.Sprintf("data-%d", offset+2),
		Data:         []byte(strings.Repeat("a", 64)),
	})
	assert_noerror(t, err)
	assert_equal(t, newOffset, offset+2)
	_, newOffset, err = dq.Push(Message{
		PartitionKey: fmt.Sprintf("data-%d", offset+3),
		Data:         []byte(strings.Repeat("a", 64)),
	})
	assert_noerror(t, err)
	assert_equal(t, newOffset, offset+3)
}

func Test_Consumer_startNewest(t *testing.T) {
	testPath, done := tempDir()
	t.Cleanup(done)

	cfg := Options{
		PartitionCount:   1,
		SegmentSizeBytes: 1024,
	}

	dq, err := New(testPath, cfg)
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
		StartBehavior: ConsumerStartBehaviorNewest,
	})
	assert_noerror(t, err)
	defer func() { _ = c.Close() }()

	gotMessages := make(chan MessageWithOffset, 3)
	started := make(chan struct{})
	go func() {
		close(started)
		for {
			select {
			case err, ok := <-c.Errors():
				if !ok {
					return
				}
				assert_noerror(t, err)
			case msg, ok := <-c.Messages():
				if !ok {
					return
				}
				gotMessages <- msg
			}
		}
	}()
	<-started

	var newOffset uint64
	_, newOffset, err = dq.Push(Message{
		PartitionKey: fmt.Sprintf("data-%d", offset+1),
		Data:         []byte(strings.Repeat("a", 64)),
	})
	assert_noerror(t, err)
	assert_equal(t, newOffset, offset+1)
	_, newOffset, err = dq.Push(Message{
		PartitionKey: fmt.Sprintf("data-%d", offset+2),
		Data:         []byte(strings.Repeat("a", 64)),
	})
	assert_noerror(t, err)
	assert_equal(t, newOffset, offset+2)
	_, newOffset, err = dq.Push(Message{
		PartitionKey: fmt.Sprintf("data-%d", offset+3),
		Data:         []byte(strings.Repeat("a", 64)),
	})
	assert_noerror(t, err)
	assert_equal(t, newOffset, offset+3)
}

func Test_Consumer_startAtEmptyActiveSegment(t *testing.T) {
	testPath, done := tempDir()
	t.Cleanup(done)

	cfg := Options{
		PartitionCount:   1,
		SegmentSizeBytes: messageSizeBytes(testMessage(10, 64)) * 4,
	}

	dq, err := New(testPath, cfg)
	assert_noerror(t, err)
	defer func() { _ = dq.Close() }()

	var offset uint64
	for x := 0; x < 3; x++ {
		_, offset, err = dq.Push(testMessage(x, 64))
		assert_noerror(t, err)
		assert_equal(t, x, offset)
	}

	index00, err := os.ReadFile(FormatPathForSegment(testPath, 0, 0) + ExtIndex)
	assert_noerror(t, err)
	entries00 := readIndexEntries(bytes.NewReader(index00))
	assert_equal(t, 3, len(entries00))

	c, err := OpenConsumer(testPath, 0, ConsumerOptions{
		StartBehavior: ConsumerStartBehaviorAtOffset,
		StartOffset:   3,
		EndBehavior:   ConsumerEndBehaviorWait,
	})
	assert_noerror(t, err)
	defer func() { _ = c.Close() }()

	gotMessages := make(chan MessageWithOffset, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		for {
			select {
			case err, ok := <-c.Errors():
				if !ok {
					return
				}
				assert_noerror(t, err)
			case msg, ok := <-c.Messages():
				if !ok {
					return
				}
				gotMessages <- msg
			}
		}
	}()
	<-started

	var newOffset uint64
	_, newOffset, err = dq.Push(testMessage(int(offset+1), 64))
	assert_noerror(t, err)
	assert_equal(t, newOffset, offset+1)
	assert_equal(t, 3, newOffset)

	index01, err := os.ReadFile(FormatPathForSegment(testPath, 0, 4) + ExtIndex)
	assert_noerror(t, err)
	entries01 := readIndexEntries(bytes.NewReader(index01))
	assert_equal(t, 0, len(entries01))

	_, newOffset, err = dq.Push(testMessage(int(offset+2), 64))
	assert_noerror(t, err)
	assert_equal(t, newOffset, offset+2)

	index01, err = os.ReadFile(FormatPathForSegment(testPath, 0, 4) + ExtIndex)
	assert_noerror(t, err)
	entries01 = readIndexEntries(bytes.NewReader(index01))
	assert_equal(t, 1, len(entries01))

	gotMessage := <-gotMessages
	assert_equal(t, fmt.Sprintf("data-%03d", offset+2), gotMessage.Message.PartitionKey)

	_, newOffset, err = dq.Push(testMessage(int(offset+3), 64))
	assert_noerror(t, err)
	assert_equal(t, newOffset, offset+3)

	gotMessage = <-gotMessages
	assert_equal(t, fmt.Sprintf("data-%03d", offset+3), gotMessage.Message.PartitionKey)
}

func Test_Consumer_startAtOffset_arbitrary(t *testing.T) {
	testPath, done := tempDir()
	t.Cleanup(done)

	cfg := Options{
		PartitionCount:   1,
		SegmentSizeBytes: 1024,
	}

	dq, err := New(testPath, cfg)
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
		StartBehavior: ConsumerStartBehaviorAtOffset,
		StartOffset:   33,
		EndBehavior:   ConsumerEndBehaviorClose,
	})
	assert_noerror(t, err)
	defer func() { _ = c.Close() }()

	gotMessages := make(chan MessageWithOffset, 30)

doneMessages:
	for {
		select {
		case msg, ok := <-c.Messages():
			if !ok {
				break doneMessages
			}
			gotMessages <- msg
		case err, ok := <-c.Errors():
			if !ok {
				return
			}
			assert_noerror(t, err)
		}
	}
	assert_equal(t, 30, len(gotMessages))

	for x := 0; x < 30; x++ {
		msg := <-gotMessages
		assert_equal(t, fmt.Sprintf("data-%d", x+34), msg.Message.PartitionKey)
	}
}

func Test_Consumer_startFromBeginning_endClose(t *testing.T) {
	testPath, done := tempDir()
	t.Cleanup(done)

	cfg := Options{
		PartitionCount:   1,
		SegmentSizeBytes: 1024,
	}

	dq, err := New(testPath, cfg)
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

func Test_Consumer_endAtOffset(t *testing.T) {
	testPath, done := tempDir()
	t.Cleanup(done)

	cfg := Options{
		PartitionCount:   1,
		SegmentSizeBytes: 1024,
	}

	dq, err := New(testPath, cfg)
	assert_noerror(t, err)
	defer func() { _ = dq.Close() }()

	var offset uint64
	for x := 0; x < 64; x++ {
		_, offset, err = dq.Push(Message{
			PartitionKey: fmt.Sprintf("data-%d", x),
			Data:         []byte(strings.Repeat("a", 512)),
		})
		assert_noerror(t, err)
		assert_equal(t, x, offset)
	}

	c, err := OpenConsumer(testPath, 0, ConsumerOptions{
		StartBehavior: ConsumerStartBehaviorAtOffset,
		StartOffset:   33,
		EndBehavior:   ConsumerEndBehaviorAtOffset,
		EndOffset:     48,
	})
	assert_noerror(t, err)
	defer func() { _ = c.Close() }()

	var messages []MessageWithOffset
	var x = 34
messageloop:
	for {
		select {
		case msg, ok := <-c.Messages():
			if !ok {
				break messageloop
			}
			assert_equal(t, fmt.Sprintf("data-%d", x), msg.Message.PartitionKey)
			messages = append(messages, msg)
			x++
		case err, ok := <-c.Errors():
			if !ok {
				break messageloop
			}
			assert_noerror(t, err)
		}
	}
	assert_equal(t, 15, len(messages))
	assert_equal(t, "data-48", messages[len(messages)-1].Message.PartitionKey)
}

func Test_Consumer_endWait(t *testing.T) {
	testPath, done := tempDir()
	t.Cleanup(done)

	cfg := Options{
		PartitionCount:   1,
		SegmentSizeBytes: 1024,
	}
	dq, err := New(testPath, cfg)
	assert_noerror(t, err)
	defer func() { _ = dq.Close() }()

	publisherPush := make(chan struct{}, 128)
	publisherQuit := make(chan struct{})
	go func() {
		var x int
		for {
			select {
			case <-publisherPush:
				_, _, err := dq.Push(Message{
					PartitionKey: fmt.Sprintf("data-%d", x),
					Data:         []byte(strings.Repeat("a", 512)),
				})
				assert_noerror(t, err)
				x++
			case <-publisherQuit:
				return
			}
		}
	}()
	defer close(publisherQuit)

	c, err := OpenConsumer(testPath, 0, ConsumerOptions{
		StartBehavior: ConsumerStartBehaviorOldest,
		EndBehavior:   ConsumerEndBehaviorWait,
	})
	assert_noerror(t, err)
	defer func() { _ = c.Close() }()

	for x := 0; x < 128; x++ {
		publisherPush <- struct{}{}
		msg, ok := <-c.Messages()
		assert_equal(t, true, ok)
		assert_equal(t, fmt.Sprintf("data-%d", x), msg.Message.PartitionKey)
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
