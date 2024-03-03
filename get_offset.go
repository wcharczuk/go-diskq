package diskq

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"os"
)

// GetOffset takes a given diskq config, and an offset, and returns the value at that offset.
func GetOffset[A any](cfg Config, offset int) (v A, ok bool, err error) {
	if err = cfg.Validate(); err != nil {
		return
	}
	var data *os.File
	data, err = os.OpenFile(cfg.DataPath, os.O_RDONLY, 0)
	if err != nil {
		return
	}
	defer data.Close()

	var offsets *os.File
	offsets, err = os.OpenFile(cfg.OffsetsPath, os.O_RDONLY, 0)
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
