package diskq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"sync"
)

func New[A any](dataPath, offsetsPath string) (*Diskq[A], error) {
	data, err := os.OpenFile(dataPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("new diskq; cannot open data file; %w", err)
	}
	offsets, err := os.OpenFile(offsetsPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("new diskq; cannot open offsets file; %w", err)
	}

	offsetCurrentPosition, err := offsets.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("new diskq; cannot find current offsets reader position; %w", err)
	}

	var offsetData OffsetData
	if offsetCurrentPosition > 0 {
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

func GetOffset[A any](dataPath, offsetsPath string, offset int) (v A, ok bool, err error) {
	var data *os.File
	data, err = os.OpenFile(dataPath, os.O_RDONLY, 0)
	if err != nil {
		return
	}
	defer data.Close()

	var offsets *os.File
	offsets, err = os.OpenFile(offsetsPath, os.O_RDONLY, 0)
	if err != nil {
		return
	}
	defer offsets.Close()

	var offsetData OffsetData
	seekAt := binary.Size(offsetData) * offset

	_, err = offsets.Seek(int64(seekAt), io.SeekStart)
	if err != nil {
		return
	}

	err = binary.Read(offsets, binary.LittleEndian, &offsetData)
	if err != nil {
		return
	}

	_, err = data.Seek(int64(offsetData[1]), io.SeekStart)
	if err != nil {
		return
	}

	readBuffer := make([]byte, int(offsetData[2]))
	_, err = data.Read(readBuffer)
	if err != nil {
		return
	}

	err = gob.NewDecoder(bytes.NewReader(readBuffer)).Decode(&v)
	if err != nil {
		return
	}
	ok = true
	return
}

func NewIter[A any](dataPath, offsetsPath string) (*Iter[A], error) {
	data, err := os.OpenFile(dataPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	offsets, err := os.OpenFile(offsetsPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	return &Iter[A]{
		data:    data,
		offsets: offsets,
	}, nil
}

type Iter[A any] struct {
	data       *os.File
	offsets    *os.File
	readBuffer []byte
}

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
