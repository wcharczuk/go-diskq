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

	c, err := dq.Consume(0, ConsumerOptions{
		StartAtBehavior: ConsumerStartAtBeginning,
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

	c, err := dq.Consume(0, ConsumerOptions{
		StartAtBehavior: ConsumerStartAtBeginning,
		EndBehavior:     ConsumerEndAndClose,
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
		default:
		}
	}
}

func Test_Consumer_startFromActivePartitionLatest(t *testing.T) {
	t.Skip()
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

	err = dq.Sync()
	assert_noerror(t, err)

	entries, err := getPartitionSegmentOffsets(cfg, 0)
	assert_noerror(t, err)
	assert_equal(t, 6, len(entries))

	endOffset, err := getSegmentEndOffset(cfg, 0, entries[len(entries)-1])
	assert_noerror(t, err)
	assert_equal(t, 63, endOffset)

	c, err := dq.Consume(0, ConsumerOptions{
		StartAtBehavior: ConsumerStartAtActiveSegmentLatest,
	})
	assert_noerror(t, err)
	defer func() { _ = c.Close() }()

	begin := make(chan struct{})
	scheduled := make(chan struct{})
	go func() {
		close(scheduled)
		<-begin
		for x := 64; x < 128; x++ {
			_, _, _ = dq.Push(Message{
				PartitionKey: fmt.Sprintf("data-%d", x),
				Data:         []byte(strings.Repeat("a", 64)),
			})
		}
	}()

	entries, err = getPartitionSegmentOffsets(cfg, 0)
	assert_noerror(t, err)
	assert_equal(t, 6, len(entries))

	endOffset, err = getSegmentEndOffset(cfg, 0, entries[len(entries)-1])
	assert_noerror(t, err)
	assert_equal(t, 63, endOffset)

	assert_equal(t, 60, c.workingSegment)

	<-scheduled
	close(begin)
	for x := 0; x < 64; x++ {
		select {
		case err = <-c.Errors():
			assert_noerror(t, err)
		case msg := <-c.Messages():
			assert_equal(t, x+63, msg.Offset, fmt.Sprint(x), msg.Message.PartitionKey)
			assert_equal(t, fmt.Sprintf("data-%d", x+63), msg.Message.PartitionKey, fmt.Sprint(x))
		}
	}
}
