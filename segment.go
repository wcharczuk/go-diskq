package diskq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

func createSegment(path string, partitionIndex uint32, startOffset uint64) (*Segment, error) {
	intendedPathWithoutExtension := FormatPathForSegment(path, partitionIndex, startOffset)
	data, err := os.OpenFile(intendedPathWithoutExtension+ExtData, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	index, err := os.OpenFile(intendedPathWithoutExtension+ExtIndex, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	timeindex, err := os.OpenFile(intendedPathWithoutExtension+ExtTimeIndex, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &Segment{
		startOffset:  startOffset,
		endOffset:    startOffset,
		data:         data,
		index:        index,
		timeindex:    timeindex,
		encodeBuffer: new(bytes.Buffer),
		indexBuf:     make([]byte, SegmentIndexSizeBytes),
		timeindexBuf: make([]byte, SegmentTimeIndexSizeBytes),
	}, nil
}

func openSegment(path string, partitionIndex uint32, startOffset uint64) (*Segment, error) {
	intendedPathWithoutExtension := FormatPathForSegment(path, partitionIndex, startOffset)
	data, err := os.OpenFile(intendedPathWithoutExtension+ExtData, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	index, err := os.OpenFile(intendedPathWithoutExtension+ExtIndex, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	timeindex, err := os.OpenFile(intendedPathWithoutExtension+ExtTimeIndex, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	endDataBytes, err := data.Seek(0, io.SeekEnd)
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

	endOffset := startOffset + uint64(endIndexBytes/int64(binary.Size(SegmentIndex{})))
	return &Segment{
		data:         data,
		index:        index,
		timeindex:    timeindex,
		startOffset:  startOffset,
		endOffset:    endOffset,
		endDataBytes: uint64(endDataBytes),
		encodeBuffer: new(bytes.Buffer),

		indexBuf:     make([]byte, SegmentIndexSizeBytes),
		timeindexBuf: make([]byte, SegmentTimeIndexSizeBytes),
	}, nil
}

// Segment represents an individual block of data within a partition.
//
// It is principally responsible for the actual "writing" of data to disk.
type Segment struct {
	mu sync.Mutex

	startOffset  uint64
	endOffset    uint64
	endDataBytes uint64

	data         io.Writer
	index        io.Writer
	indexBuf     []byte
	timeindex    io.Writer
	timeindexBuf []byte

	encodeBuffer *bytes.Buffer
}

func (s *Segment) writeUnsafe(message Message) (offset uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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
	_, err = writeSegmentTimeIndex(s.timeindex, s.timeindexBuf, NewSegmentTimeIndex(s.endOffset, message.TimestampUTC))
	if err != nil {
		return
	}

	// NOTE (wc): write the data as the (2/3) writes we need to do, saving the index write as last.
	messageSizeBytes := uint64(s.encodeBuffer.Len())
	if _, err = s.encodeBuffer.WriteTo(s.data); err != nil {
		return
	}

	_, err = writeSegmentIndex(s.index, s.indexBuf, NewSegmentIndex(s.endOffset, s.endDataBytes, messageSizeBytes))
	if err != nil {
		return
	}
	s.encodeBuffer.Reset()
	s.endDataBytes += messageSizeBytes
	s.endOffset++
	return
}

// Sync calls fsync on the (3) underlying file handles for the segment.
//
// You should almost never need to call this, but it's here in case you do want to.
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := maybeSync(s.index); err != nil {
		return err
	}
	if err := maybeSync(s.timeindex); err != nil {
		return err
	}
	if err := maybeSync(s.data); err != nil {
		return err
	}
	return nil
}

// Close closes the segment.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := maybeClose(s.data); err != nil {
		return err
	}
	if err := maybeClose(s.index); err != nil {
		return err
	}
	if err := maybeClose(s.timeindex); err != nil {
		return err
	}
	return nil
}

func maybeClose(wr io.Writer) (err error) {
	if typed, ok := wr.(io.Closer); ok && typed != nil {
		err = typed.Close()
	}
	return
}

func getSegmentOffset(path string, partitionIndex uint32, startOffset, offset uint64) (m Message, ok bool, err error) {
	var indexHandle *os.File
	indexHandle, err = OpenSegmentFileForRead(path, partitionIndex, startOffset, ExtIndex)
	if err != nil {
		err = fmt.Errorf("diskq; get segment offset; cannot open index file: %w", err)
		return
	}
	defer func() { _ = indexHandle.Close() }()

	var dataHandle *os.File
	dataHandle, err = OpenSegmentFileForRead(path, partitionIndex, startOffset, ExtData)
	if err != nil {
		err = fmt.Errorf("diskq; get segment offset; cannot open data file: %w", err)
		return
	}
	defer func() { _ = dataHandle.Close() }()

	relativeOffset := offset - startOffset
	indexSeekToBytes := int64(SegmentIndexSizeBytes) * int64(relativeOffset)
	if _, err = indexHandle.Seek(indexSeekToBytes, io.SeekStart); err != nil {
		err = fmt.Errorf("diskq; get segment offset; cannot seek within index file: %w", err)
		return
	}
	var segmentData SegmentIndex
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

// OpenSegmentFileForRead opens a segment file with a given extension in "read" mode with the correct flags.
func OpenSegmentFileForRead(path string, partitionIndex uint32, startOffset uint64, ext string) (*os.File, error) {
	workingSegmentPath := FormatPathForSegment(path, partitionIndex, startOffset)
	return os.OpenFile(workingSegmentPath+ext, os.O_RDONLY, 0 /*we aren't going to create the file, so we use a zero permissions mask*/)
}
