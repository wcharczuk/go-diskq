package diskq

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func CreateSegment(cfg Config, partitionIndex uint32, startOffset uint64) (*Segment, error) {
	intendedPathWithoutExtension := filepath.Join(
		cfg.Path,
		formatPartitionIndexForPath(partitionIndex),
		formatStartOffsetForPath(startOffset),
	)
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
	intendedPathWithoutExtension := filepath.Join(
		cfg.Path,
		formatPartitionIndexForPath(partitionIndex),
		formatStartOffsetForPath(startOffset),
	)
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

func formatPartitionIndexForPath(partitionIndex uint32) string {
	return fmt.Sprintf("%06d", partitionIndex)
}

func formatStartOffsetForPath(startOffset uint64) string {
	return fmt.Sprintf("%020d", startOffset)
}

type Segment struct {
	mu sync.Mutex

	startOffset    uint64
	endOffset      uint64
	endOffsetBytes uint64

	data      *os.File
	index     *os.File
	timeindex *os.File

	encodeBuffer *bytes.Buffer
	encoder      *gob.Encoder
}

func (s *Segment) Write(message *Message) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.encodeBuffer.Reset()
	s.endOffset++
	if message.TimestampUTC.IsZero() {
		message.TimestampUTC = time.Now().UTC()
	}
	message.Offset = s.endOffset

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

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.data.Close()
	_ = s.index.Close()
	_ = s.timeindex.Close()
	return nil
}

func newSegmentIndex(offset uint64, offsetBytes uint64, sizeBytes uint64) segmentIndex {
	return segmentIndex{offset, offsetBytes, sizeBytes}
}

type segmentIndex [3]uint64

func newSegmentTimeIndex(offset uint64, timestamp time.Time) segmentTimeIndex {
	return segmentTimeIndex{offset, uint64(timestamp.UnixNano())}
}

type segmentTimeIndex [2]uint64
