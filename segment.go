package diskq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
)

func CreateSegment(cfg Config, partitionIndex uint32, startOffset uint64) (*Segment, error) {
	intendedPathWithoutExtension := formatPathForSegment(cfg, partitionIndex, startOffset)
	data, err := os.OpenFile(intendedPathWithoutExtension+".data", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	index, err := os.OpenFile(intendedPathWithoutExtension+".index", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	timeindex, err := os.OpenFile(intendedPathWithoutExtension+".timeindex", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	encodeBuffer := new(bytes.Buffer)
	return &Segment{
		startOffset:  startOffset,
		endOffset:    startOffset,
		data:         data,
		index:        index,
		timeindex:    timeindex,
		encodeBuffer: encodeBuffer,
	}, nil
}

func OpenSegment(cfg Config, partitionIndex uint32, startOffset uint64) (*Segment, error) {
	intendedPathWithoutExtension := formatPathForSegment(cfg, partitionIndex, startOffset)
	data, err := os.OpenFile(intendedPathWithoutExtension+".data", os.O_RDWR|os.O_APPEND, 644)
	if err != nil {
		return nil, err
	}
	index, err := os.OpenFile(intendedPathWithoutExtension+".index", os.O_RDWR|os.O_APPEND, 644)
	if err != nil {
		return nil, err
	}
	timeindex, err := os.OpenFile(intendedPathWithoutExtension+".timeindex", os.O_RDWR|os.O_APPEND, 644)
	if err != nil {
		return nil, err
	}
	endOffsetBytes, err := data.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	endIndexBytes, err := index.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	_, err = timeindex.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	endOffset := uint64(endIndexBytes / int64(binary.Size(segmentIndex{})))
	encodeBuffer := new(bytes.Buffer)
	return &Segment{
		data:           data,
		index:          index,
		timeindex:      timeindex,
		startOffset:    startOffset,
		endOffset:      endOffset,
		endOffsetBytes: uint64(endOffsetBytes),
		encodeBuffer:   encodeBuffer,
	}, nil
}

type Segment struct {
	startOffset    uint64
	endOffset      uint64
	endOffsetBytes uint64

	data      io.Writer
	index     io.Writer
	timeindex io.Writer

	encodeBuffer *bytes.Buffer
}

func (s *Segment) writeUnsafe(message Message) (offset uint64, err error) {
	if message.TimestampUTC.IsZero() {
		message.TimestampUTC = time.Now().UTC()
	}
	offset = s.endOffset

	s.encodeBuffer.Reset()
	err = Encode(message, s.encodeBuffer)
	if err != nil {
		return
	}
	messageSizeBytes := uint64(s.encodeBuffer.Len())
	if _, err = s.encodeBuffer.WriteTo(s.data); err != nil {
		return
	}
	if err = binary.Write(s.index, binary.LittleEndian, newSegmentIndex(s.endOffset, s.endOffsetBytes, messageSizeBytes)); err != nil {
		return
	}
	if err = binary.Write(s.timeindex, binary.LittleEndian, newSegmentTimeIndex(s.endOffset, message.TimestampUTC)); err != nil {
		return
	}
	s.encodeBuffer.Reset()
	s.endOffsetBytes += messageSizeBytes
	s.endOffset++
	return
}

func getSegmentOffset(cfg Config, partitionIndex uint32, startOffset, offset uint64) (m Message, ok bool, err error) {
	intendedPathWithoutExtension := formatPathForSegment(cfg, partitionIndex, startOffset)

	var indexHandle *os.File
	indexHandle, err = os.OpenFile(intendedPathWithoutExtension+".index", os.O_RDONLY, 0644)
	if err != nil {
		err = fmt.Errorf("diskq; get segment offset; cannot open index file: %w", err)
		return
	}
	defer indexHandle.Close()

	var dataHandle *os.File
	dataHandle, err = os.OpenFile(intendedPathWithoutExtension+".data", os.O_RDONLY, 0644)
	if err != nil {
		err = fmt.Errorf("diskq; get segment offset; cannot open data file: %w", err)
		return
	}
	defer dataHandle.Close()

	relativeOffset := offset - startOffset
	indexSeekToBytes := int64(segmentIndexSize) * int64(relativeOffset)
	if _, err = indexHandle.Seek(indexSeekToBytes, io.SeekStart); err != nil {
		err = fmt.Errorf("diskq; get segment offset; cannot seek within index file: %w", err)
		return
	}
	var segmentData segmentIndex
	if err = binary.Read(indexHandle, binary.LittleEndian, &segmentData); err != nil {
		err = fmt.Errorf("diskq; get segment offset; cannot read index data: %w", err)
		return
	}
	if _, err = dataHandle.Seek(int64(segmentData.GetOffsetBytes()), io.SeekStart); err != nil {
		err = fmt.Errorf("diskq; get segment offset; cannot seek data file: %w", err)
		return
	}
	data := make([]byte, segmentData.GetSizeBytes())
	if _, err = dataHandle.Read(data); err != nil {
		err = fmt.Errorf("diskq; get segment offset; cannot read data file: %w", err)
		return
	}
	if err = Decode(&m, bytes.NewReader(data)); err != nil {
		err = fmt.Errorf("diskq; get segment offset; cannot decode message from data file: %w", err)
		return
	}
	ok = true
	return
}

func (s *Segment) Close() error {
	maybeClose(s.data)
	maybeClose(s.index)
	maybeClose(s.timeindex)
	return nil
}

func maybeClose(wr io.Writer) {
	if typed, ok := wr.(io.Closer); ok {
		_ = typed.Close()
	}
}
