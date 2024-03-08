package diskq

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func tempDir() (string, func()) {
	dir := filepath.Join(os.TempDir(), UUIDv4().String())
	_ = os.Mkdir(dir, 0755)
	return dir, func() {
		_ = os.RemoveAll(dir)
	}
}

func Test_Diskq_create(t *testing.T) {
	tempPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             tempPath,
		PartitionCount:   3,
		SegmentSizeBytes: 1024, // 1kb
	}

	dq, err := New(cfg)
	assert_noerror(t, err)
	for x := 0; x < 100; x++ {
		m := Message{
			PartitionKey: UUIDv4().String(),
			Data:         []byte(fmt.Sprintf("data-%06d", x)),
		}
		offset, err := dq.Push(&m)
		assert_noerror(t, err)
		assert_equal(t, offset, m.Offset)
	}

	dirEntries, err := os.ReadDir(tempPath)
	assert_noerror(t, err)
	assert_equal(t, 3, len(dirEntries))
}

func Test_Diskq_expandsSegments(t *testing.T) {
	tempPath, done := tempDir()
	defer done()

	cfg := Config{
		Path:             tempPath,
		PartitionCount:   3,
		SegmentSizeBytes: 1024, // 1kb
	}

	dq, err := New(cfg)
	assert_noerror(t, err)
	for x := 0; x < 11; x++ {
		// each message ends up being about 62b
		m := Message{
			PartitionKey: "one",
			Data:         []byte(strings.Repeat("a", 32)),
		}
		offset, err := dq.Push(&m)
		assert_noerror(t, err)
		assert_equal(t, offset, m.Offset)
	}

	dirEntries, err := os.ReadDir(tempPath)
	assert_noerror(t, err)
	assert_equal(t, 3, len(dirEntries))

	partitionIndex := dq.partitionForMessage(&Message{PartitionKey: "one"}).index

	partitionDirEntries, err := os.ReadDir(formatPathForPartition(cfg, partitionIndex))
	assert_noerror(t, err)
	assert_equal(t, 3, len(partitionDirEntries))

	for x := 0; x < 12; x++ {
		m := Message{
			PartitionKey: "one",
			Data:         []byte(strings.Repeat("a", 32)),
		}
		offset, err := dq.Push(&m)
		assert_noerror(t, err)
		assert_equal(t, offset, m.Offset)
	}

	partitionDirEntries, err = os.ReadDir(formatPathForPartition(cfg, partitionIndex))
	assert_noerror(t, err)
	assert_equal(t, 6, len(partitionDirEntries))
}
