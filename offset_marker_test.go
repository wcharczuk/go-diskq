package diskq

import (
	"path/filepath"
	"testing"
)

func Test_OffsetMarker_basic(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	offsetPath := filepath.Join(testPath, UUIDv4().String())

	marker, found, err := OpenOrCreateOffsetMarker(offsetPath, OffsetMarkerOptions{})
	assert_noerror(t, err)
	assert_equal(t, false, found)
	assert_equal(t, 0, marker.hasSetLatestOffset)
	assert_nil(t, marker.Errors())
	assert_equal(t, 0, marker.LatestOffset())

	marker.SetLatestOffset(2)

	assert_equal(t, 2, marker.LatestOffset())

	err = marker.Sync()
	assert_noerror(t, err)

	err = marker.Close()
	assert_noerror(t, err)

	marker, found, err = OpenOrCreateOffsetMarker(offsetPath, OffsetMarkerOptions{})
	assert_noerror(t, err)
	assert_equal(t, true, found)
	assert_equal(t, 1, marker.hasSetLatestOffset)
	assert_nil(t, marker.Errors())
	assert_equal(t, 2, marker.LatestOffset())
}

func Test_OffsetMarker_everyOffset(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	offsetPath := filepath.Join(testPath, UUIDv4().String())

	marker, found, err := OpenOrCreateOffsetMarker(offsetPath, OffsetMarkerOptions{
		AutosyncEveryOffset: 5,
	})
	assert_noerror(t, err)
	assert_equal(t, false, found)
	assert_equal(t, 0, marker.hasSetLatestOffset)
	assert_notnil(t, marker.Errors())
	assert_equal(t, 0, marker.LatestOffset())

	err = marker.Sync()
	assert_noerror(t, err)

	stat, err := marker.file.Stat()
	assert_noerror(t, err)
	assert_equal(t, 0, stat.Size())

	for x := 0; x < 10; x++ {
		marker.SetLatestOffset(uint64(x))
		assert_equal(t, 1, marker.hasSetLatestOffset)
	}

	assert_equal(t, 9, marker.LatestOffset())

	err = marker.Close()
	assert_noerror(t, err)

	marker, found, err = OpenOrCreateOffsetMarker(offsetPath, OffsetMarkerOptions{
		AutosyncEveryOffset: 5,
	})
	assert_noerror(t, err)
	assert_equal(t, true, found)
	assert_notnil(t, marker.Errors())
	assert_equal(t, 9, marker.LatestOffset())
}
