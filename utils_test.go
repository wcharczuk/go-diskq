package diskq

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func Test_getSegmentOffsets(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             testPath,
		SegmentSizeBytes: 1024,
	}

	p, err := createPartition(cfg, 01)
	assert_noerror(t, err)
	assert_notnil(t, p)

	for x := 0; x < 32; x++ {
		_, err = p.Write(Message{
			Data: []byte(strings.Repeat("a", 64)),
		})
		assert_noerror(t, err)
	}

	offsets, err := getPartitionSegmentOffsets(cfg.Path, 01)
	assert_noerror(t, err)
	assert_equal(t, 3, len(offsets))
	assert_equal(t, []uint64{0, 13, 26}, offsets)
}

func Test_getSegmentEndOffset(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             testPath,
		SegmentSizeBytes: 1024,
	}

	p, err := createPartition(cfg, 01)
	assert_noerror(t, err)
	assert_notnil(t, p)

	for x := 0; x < 32; x++ {
		_, err = p.Write(Message{
			Data: []byte(strings.Repeat("a", 64)),
		})
		assert_noerror(t, err)
	}
	offsets, err := getPartitionSegmentOffsets(cfg.Path, 01)
	assert_noerror(t, err)

	offset, err := getSegmentEndOffset(cfg.Path, 01, offsets[len(offsets)-1])
	assert_noerror(t, err)
	assert_equal(t, 31, offset)
}

func Test_getSegmentEndOffset_empty(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             testPath,
		SegmentSizeBytes: 1024,
	}

	segmentPath := formatPathForSegment(cfg.Path, 1, 0)
	_ = os.MkdirAll(filepath.Dir(segmentPath), 0755)

	f, err := os.Create(segmentPath + extIndex)
	assert_noerror(t, err)
	assert_noerror(t, f.Close())

	offset, err := getSegmentEndOffset(cfg.Path, 01, 0)
	assert_noerror(t, err)
	assert_equal(t, 0, offset)
}

func Test_getSegmentEndTimestamp(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             testPath,
		SegmentSizeBytes: 32 * 1024 * 1024, // 32mb
	}

	p, err := createPartition(cfg, 00)
	assert_noerror(t, err)
	assert_notnil(t, p)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 10, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 11, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	err = p.closeActiveSegmentUnsafe()
	assert_noerror(t, err)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 12, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 13, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	err = p.closeActiveSegmentUnsafe()
	assert_noerror(t, err)

	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 14, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)
	_, err = p.Write(Message{
		TimestampUTC: time.Date(2024, 01, 02, 12, 15, 10, 9, time.UTC),
		Data:         []byte("test-data"),
	})
	assert_noerror(t, err)

	offsets, err := getPartitionSegmentOffsets(cfg.Path, 0)
	assert_noerror(t, err)
	assert_equal(t, 3, len(offsets))

	endTimestamp, err := getSegmentEndTimestamp(cfg.Path, 0, offsets[1])
	assert_noerror(t, err)
	assert_equal(t, time.Date(2024, 01, 02, 12, 12, 10, 9, time.UTC), endTimestamp)
}

func readIndexEntries(r io.Reader) (output []segmentIndex) {
	for {
		var si segmentIndex
		err := binary.Read(r, binary.LittleEndian, &si)
		if err == io.EOF {
			return
		}
		output = append(output, si)
	}
}

func messageSizeBytes(m Message) int64 {
	data := new(bytes.Buffer)
	_ = Encode(m, data)
	return int64(data.Len())
}