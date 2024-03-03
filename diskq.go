package diskq

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sync"
)

func New[A any](cfg Config) (*Diskq[A], error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	data, err := os.OpenFile(cfg.DataPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("new diskq; cannot open data file; %w", err)
	}
	offsets, err := os.OpenFile(cfg.OffsetsPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("new diskq; cannot open offsets file; %w", err)
	}

	offsetCurrentPosition, err := offsets.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("new diskq; cannot find current offsets reader position; %w", err)
	}
	var offsetData OffsetData
	if offsetCurrentPosition > 0 {
		offsetSize := int64(binary.Size(offsetData))
		_, err = offsets.Seek(-offsetSize, io.SeekEnd)
		if err != nil {
			return nil, fmt.Errorf("new diskq; cannot seek to last offset; %w", err)
		}
		err = binary.Read(offsets, binary.LittleEndian, &offsetData)
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				return nil, fmt.Errorf("new diskq; cannot read last offset; %w", err)
			}
		}
	}
	lastOffset := offsetData[0]
	_, err = offsets.Seek(offsetCurrentPosition, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("new diskq; cannot reset offset reader position; %w", err)
	}
	return &Diskq[A]{
		data:         data,
		offsets:      offsets,
		encodeBuffer: new(bytes.Buffer),
		offset:       lastOffset,
	}, nil
}

type OffsetData [3]uint64

type Diskq[A any] struct {
	mu           sync.RWMutex
	data         *os.File
	offsets      *os.File
	encodeBuffer *bytes.Buffer
	offset       uint64
}

func (dq *Diskq[A]) Push(v A) error {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	dq.encodeBuffer.Reset()
	err := gob.NewEncoder(dq.encodeBuffer).Encode(v)
	if err != nil {
		return err
	}
	currentPosition, err := dq.data.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	offsetData := OffsetData{dq.offset, uint64(currentPosition), uint64(dq.encodeBuffer.Len())}
	dq.offset++
	_, err = dq.encodeBuffer.WriteTo(dq.data)
	if err != nil {
		return err
	}
	err = binary.Write(dq.offsets, binary.LittleEndian, offsetData)
	if err != nil {
		return err
	}
	return nil
}

func (dq *Diskq[A]) Vacuum() error {
	return nil
}

func (dq *Diskq[A]) Close() error {
	_ = dq.data.Close()
	_ = dq.offsets.Close()
	return nil
}
