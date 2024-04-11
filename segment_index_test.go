package diskq

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func Test_SegmentIndex(t *testing.T) {
	si := NewSegmentIndex(2, 3, 4)
	assert_equal(t, 2, si.GetOffset())
	assert_equal(t, 3, si.GetOffsetBytes())
	assert_equal(t, 4, si.GetSizeBytes())
}

func Test_writeSegmentIndex_readSegmentIndex(t *testing.T) {
	buf := new(bytes.Buffer)
	bufBuf := make([]byte, SegmentIndexSizeBytes)
	seg := NewSegmentIndex(123, 456, 789)

	n, err := writeSegmentIndex(buf, bufBuf, seg)
	assert_noerror(t, err)
	assert_equal(t, SegmentIndexSizeBytes, n)

	var control SegmentIndex
	err = binary.Read(bytes.NewReader(buf.Bytes()), binary.LittleEndian, &control)
	assert_noerror(t, err)

	assert_equal(t, 123, control.GetOffset())
	assert_equal(t, 456, control.GetOffsetBytes())
	assert_equal(t, 789, control.GetSizeBytes())

	var verify SegmentIndex
	verifyBuf := make([]byte, SegmentIndexSizeBytes)
	err = readSegmentIndex(bytes.NewReader(buf.Bytes()), verifyBuf, &verify)
	assert_noerror(t, err)

	assert_equal(t, 123, verify.GetOffset())
	assert_equal(t, 456, verify.GetOffsetBytes())
	assert_equal(t, 789, verify.GetSizeBytes())
}
