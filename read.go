package diskq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Read reads a given diskq at a given path, and fills a given
// destination slice with messages.
//
// Read is optimized for reading the start offset to the end offset
// of each partition and not waiting for new messages to be published.
//
// As a result, read is useful in situations where you want to bootstrap
// a system from the data on disk quickly.
//
// Messages will be read in rotating order through the partitions, that is
// each partition will read one message at a time, repeating until they're all done reading.
func Read(path string, dst *[]MessageWithOffset) error {
	partitionIndexes, err := getPartitions(path)
	if err != nil {
		return err
	}
	partitionIterators := make([]*readPartitionIterator, len(partitionIndexes))
	for x := 0; x < len(partitionIndexes); x++ {
		partitionIterators[x], err = newReadPartitionIterator(path, partitionIndexes[x])
		if err != nil {
			return err
		}
	}
	var hasActivePartitions bool = true
	for hasActivePartitions {
		hasActivePartitions = false
		for _, p := range partitionIterators {
			if p.done() {
				continue
			}
			hasActivePartitions = true
			if err = p.read(dst); err != nil {
				return err
			}
		}
	}
	return nil
}

func newReadPartitionIterator(path string, partitionIndex uint32) (*readPartitionIterator, error) {
	segments, err := getPartitionSegmentOffsets(path, partitionIndex)
	if err != nil {
		return nil, fmt.Errorf("diskq; read; cannot get partition segment offsets: %w", err)
	}
	if len(segments) == 0 {
		return nil, fmt.Errorf("diskq; read; no partition segment offsets returned")
	}
	firstSegment := segments[0]
	indexHandle, err := openSegmentFileForRead(path, partitionIndex, firstSegment, extIndex)
	if err != nil {
		return nil, fmt.Errorf("diskq; read; cannot open index file for segment: %w", err)
	}
	dataHandle, err := openSegmentFileForRead(path, partitionIndex, firstSegment, extData)
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

	workingSegmentData segmentIndex
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

func (rpi *readPartitionIterator) read(dst *[]MessageWithOffset) (err error) {
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
	*dst = append(*dst, MessageWithOffset{
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
	rpi.indexHandle, err = openSegmentFileForRead(rpi.path, rpi.partitionIndex, nextSegment, extIndex)
	if err != nil {
		err = fmt.Errorf("diskq; read; cannot open index file for segment: %w", err)
		return
	}
	rpi.dataHandle, err = openSegmentFileForRead(rpi.path, rpi.partitionIndex, nextSegment, extData)
	if err != nil {
		err = fmt.Errorf("diskq; read; cannot open data file for segment: %w", err)
		return
	}
	return
}
