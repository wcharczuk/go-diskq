package diskq

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"os"
	"time"
)

func CreateSegment(cfg Config, partitionIndex uint32, startOffset uint64) (*Segment, error) {
	intendedPathWithoutExtension := formatPathForSegment(cfg, partitionIndex, startOffset)
	data, err := os.OpenFile(intendedPathWithoutExtension+".data", os.O_RDWR|os.O_CREATE, 644)
	if err != nil {
		return nil, err
	}
	index, err := os.OpenFile(intendedPathWithoutExtension+".index", os.O_RDWR|os.O_CREATE, 644)
	if err != nil {
		return nil, err
	}
	timeindex, err := os.OpenFile(intendedPathWithoutExtension+".timeindex", os.O_RDWR|os.O_CREATE, 644)
	if err != nil {
		return nil, err
	}
	encodeBuffer := new(bytes.Buffer)
	return &Segment{
		data:         data,
		index:        index,
		timeindex:    timeindex,
		encodeBuffer: encodeBuffer,
		encoder:      gob.NewEncoder(encodeBuffer),
	}, nil
}

func OpenSegment(cfg Config, partitionIndex uint32, startOffset uint64) (*Segment, error) {
	intendedPathWithoutExtension := formatPathForSegment(cfg, partitionIndex, startOffset)
	data, err := os.OpenFile(intendedPathWithoutExtension+".data", os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return nil, err
	}
	index, err := os.OpenFile(intendedPathWithoutExtension+".index", os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return nil, err
	}
	timeindex, err := os.OpenFile(intendedPathWithoutExtension+".timeindex", os.O_RDWR|os.O_APPEND, 0)
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
		encoder:        gob.NewEncoder(encodeBuffer),
	}, nil
}

type Segment struct {
	startOffset    uint64
	endOffset      uint64
	endOffsetBytes uint64

	data      *os.File
	index     *os.File
	timeindex *os.File

	encodeBuffer *bytes.Buffer
	encoder      *gob.Encoder
}

func (s *Segment) writeUnsafe(message *Message) (offset uint64, err error) {
	s.encodeBuffer.Reset()
	s.endOffset++
	if message.TimestampUTC.IsZero() {
		message.TimestampUTC = time.Now().UTC()
	}
	message.Offset = s.endOffset
	offset = s.endOffset

	err = s.encoder.Encode(message)
	if err != nil {
		return
	}
	messageSizeBytes := uint64(s.encodeBuffer.Len())
	s.endOffsetBytes += messageSizeBytes
	if _, err = s.encodeBuffer.WriteTo(s.data); err != nil {
		return
	}
	if err = binary.Write(s.index, binary.LittleEndian, newSegmentIndex(s.endOffset, s.endOffsetBytes, messageSizeBytes)); err != nil {
		return
	}
	if err = binary.Write(s.timeindex, binary.LittleEndian, newSegmentTimeIndex(s.endOffset, message.TimestampUTC)); err != nil {
		return
	}
	return
}

func (s *Segment) getOffsetUnsafe(offset uint64) (m Message, ok bool, err error) {
	if s.startOffset < offset || s.endOffset >= offset {
		return
	}

	var indexHandle *os.File
	indexHandle, err = os.Open(s.index.Name())
	if err != nil {
		return
	}
	defer indexHandle.Close()

	var dataHandle *os.File
	dataHandle, err = os.Open(s.data.Name())
	if err != nil {
		return
	}
	defer dataHandle.Close()

	relativeOffset := offset - s.startOffset
	indexSeekToBytes := int64(segmentIndexSize) * int64(relativeOffset)
	if _, err = indexHandle.Seek(indexSeekToBytes, io.SeekStart); err != nil {
		return
	}

	var segmentData segmentIndex
	if err = binary.Read(indexHandle, binary.LittleEndian, &segmentData); err != nil {
		return
	}
	if _, err = dataHandle.Seek(int64(segmentData.GetOffsetBytes()), io.SeekStart); err != nil {
		return
	}

	data := make([]byte, segmentData.GetSizeBytes())
	if _, err = dataHandle.Read(data); err != nil {
		return
	}
	if err = gob.NewDecoder(bytes.NewReader(data)).Decode(&m); err != nil {
		return
	}
	ok = true
	return
}

func (s *Segment) Close() error {
	_ = s.data.Close()
	_ = s.index.Close()
	_ = s.timeindex.Close()
	return nil
}
