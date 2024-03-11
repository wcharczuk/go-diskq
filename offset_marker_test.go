package diskq

import (
	"path/filepath"
	"testing"
)

func Test_OffsetMarker_basic(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	offsetPath := filepath.Join(testPath, UUIDv4().String())

	marker, found, err := NewOffsetMarker(offsetPath, OffsetMarkerOptions{})
	assert_noerror(t, err)
	assert_equal(t, false, found)
	assert_nil(t, marker.Errors())
	assert_equal(t, 0, marker.Latest())

	marker.Record(2)

	assert_equal(t, 2, marker.Latest())

	err = marker.Sync()
	assert_noerror(t, err)

	err = marker.Close()
	assert_noerror(t, err)

	marker, found, err = NewOffsetMarker(offsetPath, OffsetMarkerOptions{})
	assert_noerror(t, err)
	assert_equal(t, true, found)
	assert_nil(t, marker.Errors())
	assert_equal(t, 2, marker.Latest())
}

func Test_OffsetMarker_everyOffset(t *testing.T) {
	testPath, done := tempDir()
	defer done()

	offsetPath := filepath.Join(testPath, UUIDv4().String())

	marker, found, err := NewOffsetMarker(offsetPath, OffsetMarkerOptions{
		AutosyncEveryOffset: 5,
	})
	assert_noerror(t, err)
	assert_equal(t, false, found)
	assert_notnil(t, marker.Errors())
	assert_equal(t, 0, marker.Latest())

	for x := 0; x < 10; x++ {
		marker.Record(uint64(x))
	}

	assert_equal(t, 9, marker.Latest())

	err = marker.Close()
	assert_noerror(t, err)

	marker, found, err = NewOffsetMarker(offsetPath, OffsetMarkerOptions{
		AutosyncEveryOffset: 5,
	})
	assert_noerror(t, err)
	assert_equal(t, true, found)
	assert_notnil(t, marker.Errors())
	assert_equal(t, 9, marker.Latest())
}
