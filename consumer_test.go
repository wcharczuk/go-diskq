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
	defer dq.Close()

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
	defer c.Close()

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

func Test_Consumer_startFromActivePartitionLatest(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             testPath,
		PartitionCount:   1,
		SegmentSizeBytes: 1024,
	}

	dq, err := New(cfg)
	assert_noerror(t, err)
	defer dq.Close()

	var offset uint64
	for x := 0; x < 64; x++ {
		_, offset, err = dq.Push(Message{
			PartitionKey: fmt.Sprintf("data-%d", x),
			Data:         []byte(strings.Repeat("a", 64)),
		})
		assert_noerror(t, err)
		assert_equal(t, x, offset)
	}

	entries, err := getPartitionSegmentOffsets(cfg, 0)
	assert_noerror(t, err)
	assert_equal(t, 6, len(entries))

	c, err := dq.Consume(0, ConsumerOptions{
		StartAtBehavior: ConsumerStartAtActiveSegmentLatest,
	})
	assert_noerror(t, err)
	defer c.Close()

	begin := make(chan struct{})
	go func() {
		<-begin
		for x := 64; x < 128; x++ {
			dq.Push(Message{
				PartitionKey: fmt.Sprintf("data-%d", x),
				Data:         []byte(strings.Repeat("a", 64)),
			})
		}
	}()

	close(begin)
	for x := 0; x < 64; x++ {
		select {
		case err = <-c.Errors():
			assert_noerror(t, err)
		case msg := <-c.Messages():
			assert_equal(t, x+64, msg.Offset)
			assert_equal(t, fmt.Sprintf("data-%d", x+64), msg.Message.PartitionKey)
			fmt.Println("message!")
		}
	}
}
