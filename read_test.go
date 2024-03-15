package diskq

import (
	"fmt"
	"strings"
	"testing"
)

func Test_Read(t *testing.T) {
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

	for x := 0; x < 128; x++ {
		_, _, err = dq.Push(Message{
			PartitionKey: fmt.Sprintf("data-%d", x),
			Data:         []byte(strings.Repeat("a", 64)),
		})
		assert_noerror(t, err)
	}

	var messages []MessageWithOffset
	err = Read(testPath, &messages)
	assert_noerror(t, err)
	assert_equal(t, 128, len(messages))
}
