package diskq

import (
	"fmt"
	"io"
	"time"
)

// Encode writes a message out into a writer.
func Encode(m Message, wr io.Writer) (err error) {
	partitionKeyBytes := []byte(m.PartitionKey)
	partitionKeyLen := uint32(len(partitionKeyBytes))
	partitionKeyLenData := []byte{
		byte(partitionKeyLen),
		byte(partitionKeyLen >> 8),
		byte(partitionKeyLen >> 16),
		byte(partitionKeyLen >> 24),
	}
	_, err = wr.Write(partitionKeyLenData)
	if err != nil {
		return
	}
	_, err = wr.Write(partitionKeyBytes)
	if err != nil {
		return
	}
	timestampNanos := m.TimestampUTC.UnixNano()
	timestampNanosData := []byte{
		byte(timestampNanos),
		byte(timestampNanos >> 8),
		byte(timestampNanos >> 16),
		byte(timestampNanos >> 24),
		byte(timestampNanos >> 32),
		byte(timestampNanos >> 40),
		byte(timestampNanos >> 48),
		byte(timestampNanos >> 56),
	}
	_, err = wr.Write(timestampNanosData)
	if err != nil {
		return
	}
	dataLen := uint64(len(m.Data))
	dataLenData := []byte{
		byte(dataLen),
		byte(dataLen >> 8),
		byte(dataLen >> 16),
		byte(dataLen >> 24),
		byte(dataLen >> 32),
		byte(dataLen >> 40),
		byte(dataLen >> 48),
		byte(dataLen >> 56),
	}
	_, err = wr.Write(dataLenData)
	if err != nil {
		return
	}
	_, err = wr.Write(m.Data)
	return
}

// Decode reads a message out of the reader.
func Decode(m *Message, r io.Reader) (err error) {
	partitionKeySizeBytesData := make([]byte, 4)
	_, err = r.Read(partitionKeySizeBytesData)
	if err != nil {
		err = fmt.Errorf("decode; cannot read partition key size: %w", err)
		return
	}
	partitionKeySizeBytes := uint32(partitionKeySizeBytesData[0]) |
		uint32(partitionKeySizeBytesData[1])<<8 |
		uint32(partitionKeySizeBytesData[2])<<16 |
		uint32(partitionKeySizeBytesData[3])<<24

	partitionKeyData := make([]byte, partitionKeySizeBytes)
	_, err = r.Read(partitionKeyData)
	if err != nil {
		err = fmt.Errorf("decode; cannot read partition key: %w", err)
		return
	}
	m.PartitionKey = string(partitionKeyData)

	timestampNanosBytes := make([]byte, 8)
	_, err = r.Read(timestampNanosBytes)
	if err != nil {
		err = fmt.Errorf("decode; cannot read timestamp: %w", err)
		return
	}
	timestampNanos := int64(timestampNanosBytes[0]) |
		int64(timestampNanosBytes[1])<<8 |
		int64(timestampNanosBytes[2])<<16 |
		int64(timestampNanosBytes[3])<<24 |
		int64(timestampNanosBytes[4])<<32 |
		int64(timestampNanosBytes[5])<<40 |
		int64(timestampNanosBytes[6])<<48 |
		int64(timestampNanosBytes[7])<<56

	m.TimestampUTC = time.Unix(0, timestampNanos).UTC()

	dataSizeBytesData := make([]byte, 8)
	_, err = r.Read(dataSizeBytesData)
	if err != nil {
		err = fmt.Errorf("decode; cannot read data size: %w", err)
		return
	}
	dataSizeBytes := uint64(dataSizeBytesData[0]) |
		uint64(dataSizeBytesData[1])<<8 |
		uint64(dataSizeBytesData[2])<<16 |
		uint64(dataSizeBytesData[3])<<24 |
		uint64(dataSizeBytesData[4])<<32 |
		uint64(dataSizeBytesData[5])<<40 |
		uint64(dataSizeBytesData[6])<<48 |
		uint64(dataSizeBytesData[7])<<56

	m.Data = make([]byte, dataSizeBytes)
	_, err = r.Read(m.Data)
	if err != nil {
		err = fmt.Errorf("decode; cannot read data: %w", err)
	}
	return
}
