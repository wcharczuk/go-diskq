package diskq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Read reads a given diskq at a given path and a given partition index, and calls a given function for
// each message read from the partition.
//
// Read is optimized for reading the oldest offset to the newst offset of the given partition
// and does not wait for new messages to be published.
//
// As a result, read is useful in situations where you want to bootstrap
// a system from the data on disk quickly, as there is no overhead of pushing into channels.
//
// It is parameterized by partition because partitions are strictly ordered, but you cannot
// relate the relative order of many partitions.
//
// If you want to read all partitions of a diskq, you can first enumerate the partitions with
// the helper [GetPartitions], which will return an []uint32 you can pass the elements of as
// the partitionIndex parameter.
func Read(path string, partitionIndex uint32, fn func(MessageWithOffset) error) error {
	partitionIterator, err := newReadPartitionIterator(path, partitionIndex)
	if err != nil {
		return err
	}
	for !partitionIterator.done() {
		if err = partitionIterator.read(fn); err != nil {
			return err
		}
	}
	return nil
}

func newReadPartitionIterator(path string, partitionIndex uint32) (*readPartitionIterator, error) {
	segments, err := GetPartitionSegmentStartOffsets(path, partitionIndex)
	if err != nil {
		return nil, fmt.Errorf("diskq; read; cannot get partition segment offsets: %w", err)
	}
	if len(segments) == 0 {
		return nil, fmt.Errorf("diskq; read; no partition segment offsets returned")
	}
	firstSegment := segments[0]
	indexHandle, err := OpenSegmentFileForRead(path, partitionIndex, firstSegment, ExtIndex)
	if err != nil {
		return nil, fmt.Errorf("diskq; read; cannot open index file for segment: %w", err)
	}
	dataHandle, err := OpenSegmentFileForRead(path, partitionIndex, firstSegment, ExtData)
	if err != nil {
		return nil, fmt.Errorf("diskq; read; cannot open data file for segment: %w", err)
	}
	return &readPartitionIterator{
		path:           path,
		partitionIndex: partitionIndex,
		segments:       segments,
		segmentIndex:   0,
		indexHandle:    indexHandle,
		dataHandle:     dataHandle,
	}, nil
}

type readPartitionIterator struct {
	path           string
	partitionIndex uint32

	segments     []uint64
	segmentIndex int

	indexHandle *os.File
	dataHandle  *os.File

	workingSegmentData SegmentIndex
	messageData        []byte
}

func (rpi *readPartitionIterator) Close() error {
	if rpi.indexHandle != nil {
		_ = rpi.indexHandle.Close()
		rpi.indexHandle = nil
	}
	if rpi.dataHandle != nil {
		_ = rpi.dataHandle.Close()
		rpi.dataHandle = nil
	}
	return nil
}

func (rpi *readPartitionIterator) done() bool {
	return rpi.indexHandle == nil && rpi.dataHandle == nil
}

func (rpi *readPartitionIterator) read(fn func(MessageWithOffset) error) (err error) {
	var done bool
	done, err = rpi.tryReadIndex()
	if err != nil {
		return
	}
	if done {
		done, err = rpi.advanceToNextSegment()
		if err != nil {
			return
		}
		if done {
			_ = rpi.Close()
			return
		}
		return
	}
	rpi.messageData = make([]byte, rpi.workingSegmentData.GetSizeBytes())
	if _, err = rpi.dataHandle.Read(rpi.messageData); err != nil {
		err = fmt.Errorf("diskq; read; cannot read data file for message: %w", err)
		return
	}
	var m Message
	if err = Decode(&m, bytes.NewReader(rpi.messageData)); err != nil {
		err = fmt.Errorf("diskq; read; cannot decode message data from data file: %w", err)
		return
	}
	err = fn(MessageWithOffset{
		PartitionIndex: rpi.partitionIndex,
		Offset:         rpi.workingSegmentData.GetOffset(),
		Message:        m,
	})
	return
}

func (rpi *readPartitionIterator) tryReadIndex() (done bool, err error) {
	err = binary.Read(rpi.indexHandle, binary.LittleEndian, &rpi.workingSegmentData)
	if err != nil && err == io.EOF {
		err = nil
		done = true
	}
	return
}

func (rpi *readPartitionIterator) advanceToNextSegment() (done bool, err error) {
	if rpi.segmentIndex == len(rpi.segments)-1 {
		done = true
		return
	}
	rpi.segmentIndex++
	nextSegment := rpi.segments[rpi.segmentIndex]
	rpi.indexHandle, err = OpenSegmentFileForRead(rpi.path, rpi.partitionIndex, nextSegment, ExtIndex)
	if err != nil {
		err = fmt.Errorf("diskq; read; cannot open index file for segment: %w", err)
		return
	}
	rpi.dataHandle, err = OpenSegmentFileForRead(rpi.path, rpi.partitionIndex, nextSegment, ExtData)
	if err != nil {
		err = fmt.Errorf("diskq; read; cannot open data file for segment: %w", err)
		return
	}
	return
}
