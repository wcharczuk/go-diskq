package diskq

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func Test_writeSegmentTimeIndex(t *testing.T) {
	buf := new(bytes.Buffer)
	bufBuf := make([]byte, SegmentTimeIndexSizeBytes)
	seg := NewSegmentTimeIndex(123, time.Date(2024, 01, 02, 03, 04, 05, 06, time.UTC))

	n, err := writeSegmentTimeIndex(buf, bufBuf, seg)
	assert_noerror(t, err)
	assert_equal(t, SegmentTimeIndexSizeBytes, n)

	var verify SegmentTimeIndex
	err = binary.Read(bytes.NewReader(buf.Bytes()), binary.LittleEndian, &verify)
	assert_noerror(t, err)

	assert_equal(t, 123, verify.GetOffset())
	assert_equal(t, time.Date(2024, 01, 02, 03, 04, 05, 06, time.UTC), verify.GetTimestampUTC())
}
