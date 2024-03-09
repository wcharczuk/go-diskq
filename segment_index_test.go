package diskq

import "testing"

func Test_SegmentIndex(t *testing.T) {
	si := newSegmentIndex(2, 3, 4)
	assert_equal(t, 2, si.GetOffset())
	assert_equal(t, 3, si.GetOffsetBytes())
	assert_equal(t, 4, si.GetSizeBytes())
}
