package diskq

import (
	"encoding/binary"
	"os"
	"testing"
)

func Test_MarkedConsumerGroup(t *testing.T) {
	testPath, done := tempDir()
	t.Cleanup(done)

	dq, err := New(testPath, Options{
		PartitionCount:   3,
		SegmentSizeBytes: messageSizeBytes(testMessage(10, 128)) * 512,
	})
	assert_noerror(t, err)
	defer func() { _ = dq.Close() }()

	for x := 0; x < 256; x++ {
		_, _, err = dq.Push(testMessage(int(x), 128))
		assert_noerror(t, err)
	}

	mcg, err := OpenMarkedConsumerGroup(testPath, "test-group", MarkedConsumerGroupOptions{
		OffsetMarkerOptions: OffsetMarkerOptions{
			AutosyncEveryOffset: 16,
		},
		AutoSetLatestOffset: true,
	})
	assert_noerror(t, err)

	offsetsSeen := make(map[uint32][]uint64)
	var offsetsRead int
	for x := 0; x < 32; x++ {
		select {
		case msg, ok := <-mcg.Messages():
			assert_equal(t, true, ok)
			if len(offsetsSeen[msg.PartitionIndex]) > 0 {
				assert_equal(t, last(offsetsSeen[msg.PartitionIndex]), msg.Offset-1)
			}
			offsetsSeen[msg.PartitionIndex] = append(offsetsSeen[msg.PartitionIndex], msg.Offset)
			offsetsRead++
		case err, ok := <-mcg.Errors():
			assert_equal(t, true, ok)
			assert_noerror(t, err)
		}
	}

	assert_equal(t, 32, offsetsRead)

	err = mcg.Close()
	assert_noerror(t, err)

	offsetRecorded, err := readOffsetMarker(FormatPathForMarkedConsumerGroupOffsetMarker(testPath, "test-group", 0))
	assert_noerror(t, err)
	assert_equal(t, last(offsetsSeen[0]), offsetRecorded)

	offsetRecorded, err = readOffsetMarker(FormatPathForMarkedConsumerGroupOffsetMarker(testPath, "test-group", 1))
	assert_noerror(t, err)
	assert_equal(t, last(offsetsSeen[1]), offsetRecorded)

	offsetRecorded, err = readOffsetMarker(FormatPathForMarkedConsumerGroupOffsetMarker(testPath, "test-group", 2))
	assert_noerror(t, err)
	assert_equal(t, last(offsetsSeen[2]), offsetRecorded)

	mcg, err = OpenMarkedConsumerGroup(testPath, "test-group", MarkedConsumerGroupOptions{
		OffsetMarkerOptions: OffsetMarkerOptions{
			AutosyncEveryOffset: 16,
		},
		AutoSetLatestOffset: true,
	})
	assert_noerror(t, err)
	t.Cleanup(func() { _ = mcg.Close() })

	for x := 0; x < 32; x++ {
		select {
		case msg, ok := <-mcg.Messages():
			assert_equal(t, true, ok)
			if len(offsetsSeen[msg.PartitionIndex]) > 0 {
				assert_equal(t, last(offsetsSeen[msg.PartitionIndex]), msg.Offset-1)
			}
			offsetsSeen[msg.PartitionIndex] = append(offsetsSeen[msg.PartitionIndex], msg.Offset)
			offsetsRead++
		case err, ok := <-mcg.Errors():
			assert_equal(t, true, ok)
			assert_noerror(t, err)
		}
	}
	assert_equal(t, 64, offsetsRead)
}

func readOffsetMarker(path string) (uint64, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	var latestOffset uint64
	if stat.Size() > 0 {
		if err := binary.Read(file, binary.LittleEndian, &latestOffset); err != nil {
			return 0, err
		}
	}
	return latestOffset, nil
}

func last[A any](values []A) (out A) {
	if len(values) == 0 {
		return
	}
	out = values[len(values)-1]
	return
}
