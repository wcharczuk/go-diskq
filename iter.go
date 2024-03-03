package diskq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
)

// NewIter returns a new diskq iterator.
//
// The iterator will start at the beginning of the diskq
// file and for each call to `.Next()` advance to the next element.
//
// Once the iterator is done, the `ok` returned value will be `false`.
func NewIter[A any](cfg Config) (*Iter[A], error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	data, err := os.OpenFile(cfg.DataPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	offsets, err := os.OpenFile(cfg.OffsetsPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	return &Iter[A]{
		data:    data,
		offsets: offsets,
	}, nil
}

// Iter is a diskq iterator.
type Iter[A any] struct {
	data       *os.File
	offsets    *os.File
	readBuffer []byte
}

// Next yields the next value for the iterator.
func (dqi *Iter[A]) Next() (v A, ok bool, err error) {
	var offsetData OffsetData
	err = binary.Read(dqi.offsets, binary.LittleEndian, &offsetData)
	if err != nil {
		if err == io.EOF {
			err = nil
			ok = false
			return
		}
		err = fmt.Errorf("iterator next; reading offset data; %w", err)
		return
	}
	dqi.readBuffer = make([]byte, offsetData[2])
	_, err = dqi.data.Read(dqi.readBuffer)
	if err != nil {
		if err == io.EOF {
			err = nil
			ok = false
			return
		}
		err = fmt.Errorf("iterator next; reading message data; %w", err)
		return
	}
	err = gob.NewDecoder(bytes.NewReader(dqi.readBuffer)).Decode(&v)
	if err != nil {
		err = fmt.Errorf("iterator next; decoding message; %w", err)
		return
	}
	ok = true
	return
}

func (dqi *Iter[A]) Close() error {
	_ = dqi.data.Close()
	_ = dqi.offsets.Close()
	return nil
}

type DiskqNotifer[A any] struct {
	data       *os.File
	dataReader *bufio.Reader
	seekBuffer []byte
	notify     chan A
}

func (dqn *DiskqNotifer[A]) Notify() <-chan A {
	return dqn.notify
}
