package diskq

import (
	"bytes"
	"testing"
	"time"
)

func Test_Encode_Decode(t *testing.T) {
	m := Message{
		PartitionKey: "test-key",
		TimestampUTC: time.Date(2024, 01, 02, 03, 04, 05, 06, time.UTC),
		Data:         []byte("test-data-and-stuff"),
	}

	data := new(bytes.Buffer)
	err := Encode(m, data)
	assert_noerror(t, err)

	dataBytes := data.Bytes()

	var verify Message
	err = Decode(&verify, bytes.NewReader(dataBytes))
	assert_noerror(t, err)

	assert_equal(t, m.PartitionKey, verify.PartitionKey)
	assert_equal(t, m.TimestampUTC, verify.TimestampUTC)
	assert_equal(t, string(m.Data), string(verify.Data))
}

func Test_Encode_Decode_unicodePartitionKey(t *testing.T) {
	m := Message{
		PartitionKey: "💩",
		TimestampUTC: time.Date(2024, 01, 02, 03, 04, 05, 06, time.UTC),
		Data:         []byte("test-data-and-stuff"),
	}

	data := new(bytes.Buffer)
	err := Encode(m, data)
	assert_noerror(t, err)

	dataBytes := data.Bytes()

	var verify Message
	err = Decode(&verify, bytes.NewReader(dataBytes))
	assert_noerror(t, err)

	assert_equal(t, m.PartitionKey, verify.PartitionKey)
	assert_equal(t, m.TimestampUTC, verify.TimestampUTC)
	assert_equal(t, string(m.Data), string(verify.Data))
}
