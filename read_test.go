package diskq

import (
	"fmt"
	"strings"
	"testing"
)

func Test_Read(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Options{
		PartitionCount:   3,
		SegmentSizeBytes: 1024,
	}

	dq, err := New(testPath, cfg)
	assert_noerror(t, err)
	defer func() { _ = dq.Close() }()

	for x := 0; x < 128; x++ {
		_, _, err = dq.Push(Message{
			PartitionKey: fmt.Sprintf("data-%d", x),
			Data:         []byte(strings.Repeat("a", 64)),
		})
		assert_noerror(t, err)
	}

	var messages []MessageWithOffset
	into := func(msg MessageWithOffset) error {
		messages = append(messages, msg)
		return nil
	}
	err = Read(testPath, 0, into)
	assert_noerror(t, err)
	assert_equal(t, 45, len(messages), "we expect a subset of messages in partition 0")
}

func Test_Read_stopsOnError(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Options{
		PartitionCount:   3,
		SegmentSizeBytes: 1024,
	}

	dq, err := New(testPath, cfg)
	assert_noerror(t, err)
	defer func() { _ = dq.Close() }()

	for x := 0; x < 128; x++ {
		_, _, err = dq.Push(Message{
			PartitionKey: fmt.Sprintf("data-%d", x),
			Data:         []byte(strings.Repeat("a", 64)),
		})
		assert_noerror(t, err)
	}

	var messages []MessageWithOffset
	into := func(msg MessageWithOffset) error {
		messages = append(messages, msg)
		return fmt.Errorf("this is just a test")
	}
	err = Read(testPath, 0, into)
	assert_error(t, err)
	assert_equal(t, 1, len(messages))
}
