package diskq

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"
	"time"
)

func Test_Segment_CreateSegment(t *testing.T) {
	tempPath, done := tempDir()
	defer done()

	err := os.MkdirAll(FormatPathForPartition(tempPath, 0), 0755)
	assert_noerror(t, err)

	segment, err := createSegment(tempPath, 0, 123)
	assert_noerror(t, err)

	assert_notnil(t, segment)
	assert_equal(t, 123, segment.startOffset)
	assert_equal(t, 123, segment.endOffset)
	assert_notnil(t, segment.data)
	assert_notnil(t, segment.index)
	assert_notnil(t, segment.timeindex)
	assert_notnil(t, segment.encodeBuffer)
}

func Test_Segment_CreateSegment_overwrites(t *testing.T) {
	tempPath, done := tempDir()
	defer done()

	err := os.MkdirAll(FormatPathForPartition(tempPath, 0), 0755)
	assert_noerror(t, err)

	segment, err := createSegment(tempPath, 0, 123)
	assert_noerror(t, err)

	var writtenOffset uint64
	for x := 0; x < 10; x++ {
		writtenOffset, _ = segment.writeUnsafe(testMessage(123+x, 512))
		assert_equal(t, 123+x, writtenOffset)
	}

	_ = segment.Close()

	segment, err = createSegment(tempPath, 0, 123)
	assert_noerror(t, err)

	for x := 0; x < 10; x++ {
		writtenOffset, _ = segment.writeUnsafe(testMessage(123+x, 512))
		assert_equal(t, 123+x, writtenOffset)
	}

	assert_notnil(t, segment)
	assert_equal(t, 123, segment.startOffset)
	assert_equal(t, 133, segment.endOffset)
	assert_notnil(t, segment.data)
	assert_notnil(t, segment.index)
	assert_notnil(t, segment.timeindex)
	assert_notnil(t, segment.encodeBuffer)
}

func Test_Segment_OpenSegment_basic(t *testing.T) {
	t.Skip()
}

func Test_Segment_OpenSegment_notFound(t *testing.T) {
	t.Skip()
}

func Test_Segment_writeUnsafe(t *testing.T) {
	dataBuf := new(bytes.Buffer)
	indexBuf := new(bytes.Buffer)
	timeindexBuf := new(bytes.Buffer)

	s := &Segment{
		data:         dataBuf,
		index:        indexBuf,
		timeindex:    timeindexBuf,
		encodeBuffer: new(bytes.Buffer),
		indexBuf:     make([]byte, SegmentIndexSizeBytes),
		timeindexBuf: make([]byte, SegmentTimeIndexSizeBytes),
	}

	m0 := Message{
		PartitionKey: "test-key-00",
		TimestampUTC: time.Date(2024, 01, 02, 03, 04, 05, 06, time.UTC),
		Data:         []byte("test-data-and-stuff-00"),
	}

	offset, err := s.writeUnsafe(m0)
	assert_noerror(t, err)
	assert_equal(t, 0, offset)

	data := dataBuf.Bytes()
	index := indexBuf.Bytes()
	timeindex := timeindexBuf.Bytes()

	var si SegmentIndex
	err = binary.Read(bytes.NewReader(index), binary.LittleEndian, &si)
	assert_noerror(t, err)
	assert_equal(t, si.GetOffset(), 0)
	assert_equal(t, si.GetOffsetBytes(), 0)
	assert_equal(t, si.GetSizeBytes(), 53)

	var sti SegmentTimeIndex
	err = binary.Read(bytes.NewReader(timeindex), binary.LittleEndian, &sti)
	assert_noerror(t, err)
	assert_equal(t, si.GetOffset(), 0)
	assert_equal(t, false, sti.GetTimestampUTC().IsZero())

	var verify Message
	err = Decode(&verify, bytes.NewReader(data))
	assert_noerror(t, err)
	assert_equal(t, m0.PartitionKey, verify.PartitionKey)
}
