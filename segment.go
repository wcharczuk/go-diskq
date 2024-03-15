package diskq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
)

// CreateSegment creates a segment file at a given data path, for a given partition, and for
// a given start offset which will be the effective filename.
func CreateSegment(path string, partitionIndex uint32, startOffset uint64) (*Segment, error) {
	intendedPathWithoutExtension := formatPathForSegment(path, partitionIndex, startOffset)
	data, err := os.OpenFile(intendedPathWithoutExtension+extData, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	index, err := os.OpenFile(intendedPathWithoutExtension+extIndex, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	timeindex, err := os.OpenFile(intendedPathWithoutExtension+extTimeIndex, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &Segment{
		startOffset:              startOffset,
		endOffset:                startOffset,
		data:                     data,
		index:                    index,
		timeindex:                timeindex,
		segmentIndexEncodeBuffer: new(bytes.Buffer),
		encodeBuffer:             new(bytes.Buffer),
	}, nil
}

func OpenSegment(path string, partitionIndex uint32, startOffset uint64) (*Segment, error) {
	intendedPathWithoutExtension := formatPathForSegment(path, partitionIndex, startOffset)
	data, err := os.OpenFile(intendedPathWithoutExtension+extData, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	index, err := os.OpenFile(intendedPathWithoutExtension+extIndex, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	timeindex, err := os.OpenFile(intendedPathWithoutExtension+extTimeIndex, os.O_RDWR|os.O_APPEND, 0644)
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

	endOffset := startOffset + uint64(endIndexBytes/int64(binary.Size(segmentIndex{})))
	return &Segment{
		data:                     data,
		index:                    index,
		timeindex:                timeindex,
		startOffset:              startOffset,
		endOffset:                endOffset,
		endOffsetBytes:           uint64(endOffsetBytes),
		segmentIndexEncodeBuffer: new(bytes.Buffer),
		encodeBuffer:             new(bytes.Buffer),
	}, nil
}

const (
	extData      = ".data"
	extIndex     = ".index"
	extTimeIndex = ".timeindex"
)

type Segment struct {
	startOffset    uint64
	endOffset      uint64
	endOffsetBytes uint64

	data      io.Writer
	index     io.Writer
	timeindex io.Writer

	segmentIndexEncodeBuffer *bytes.Buffer
	encodeBuffer             *bytes.Buffer
}

func (s *Segment) writeUnsafe(message Message) (offset uint64, err error) {
	if message.TimestampUTC.IsZero() {
		message.TimestampUTC = time.Now().UTC()
	}

	offset = s.endOffset

	s.encodeBuffer.Reset()

	// NOTE (wc):
	// 	we can't write to data directly as we need to know
	// 	what the final encoded message size in as bytes to record
	// 	to the offset index.
	err = Encode(message, s.encodeBuffer)
	if err != nil {
		return
	}

	// NOTE (wc):
	//	timeindex writes aren't listened for, so we can just bombs away on them.
	if err = binary.Write(s.timeindex, binary.LittleEndian, newSegmentTimeIndex(s.endOffset, message.TimestampUTC)); err != nil {
		return
	}

	// NOTE (wc): write the data as the (2/3) writes we need to do, saving the index write as last.
	messageSizeBytes := uint64(s.encodeBuffer.Len())
	if _, err = s.encodeBuffer.WriteTo(s.data); err != nil {
		return
	}

	// NOTE (wc):
	// 	we do this in (2) phases to prevent situations where
	// 	an eager consumer will pick up file write notifications on (1/3) or (2/3) of the segment elements being written one at a time
	//	because binary write is careful to not buffer writes (like we specifically need to.)
	s.segmentIndexEncodeBuffer.Reset()
	if err = binary.Write(s.segmentIndexEncodeBuffer, binary.LittleEndian, newSegmentIndex(s.endOffset, s.endOffsetBytes, messageSizeBytes)); err != nil {
		return
	}
	if _, err = s.index.Write(s.segmentIndexEncodeBuffer.Bytes()); err != nil {
		return
	}
	s.encodeBuffer.Reset()
	s.endOffsetBytes += messageSizeBytes
	s.endOffset++
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

func getSegmentOffset(path string, partitionIndex uint32, startOffset, offset uint64) (m Message, ok bool, err error) {
	var indexHandle *os.File
	indexHandle, err = openSegmentFileForRead(path, partitionIndex, startOffset, extIndex)
	if err != nil {
		err = fmt.Errorf("diskq; get segment offset; cannot open index file: %w", err)
		return
	}
	defer func() { _ = indexHandle.Close() }()

	var dataHandle *os.File
	dataHandle, err = openSegmentFileForRead(path, partitionIndex, startOffset, extData)
	if err != nil {
		err = fmt.Errorf("diskq; get segment offset; cannot open data file: %w", err)
		return
	}
	defer func() { _ = dataHandle.Close() }()

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

func openSegmentFileForRead(path string, partitionIndex uint32, startOffset uint64, ext string) (*os.File, error) {
	workingSegmentPath := formatPathForSegment(path, partitionIndex, startOffset)
	return os.OpenFile(workingSegmentPath+ext, os.O_RDONLY, 0644)
}
