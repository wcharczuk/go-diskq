package diskq

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

func Encode(m Message, wr io.Writer) (err error) {
	partitionKeyBytes := []byte(m.PartitionKey)
	err = binary.Write(wr, binary.LittleEndian, uint32(len(partitionKeyBytes)))
	if err != nil {
		return
	}
	_, err = wr.Write(partitionKeyBytes)
	if err != nil {
		return
	}
	timestampNanos := m.TimestampUTC.UnixNano()
	err = binary.Write(wr, binary.LittleEndian, timestampNanos)
	if err != nil {
		return
	}
	err = binary.Write(wr, binary.LittleEndian, uint64(len(m.Data)))
	if err != nil {
		return
	}
	_, err = wr.Write(m.Data)
	return
}

func Decode(m *Message, r io.Reader) (err error) {
	var partitionKeySizeBytes uint32
	err = binary.Read(r, binary.LittleEndian, &partitionKeySizeBytes)
	if err != nil {
		err = fmt.Errorf("decode; cannot read partition key size: %w", err)
		return
	}

	partitionKeyData := make([]byte, partitionKeySizeBytes)
	_, err = r.Read(partitionKeyData)
	if err != nil {
		err = fmt.Errorf("decode; cannot read partition key: %w", err)
		return
	}
	m.PartitionKey = string(partitionKeyData)
	var timestampNanos int64
	err = binary.Read(r, binary.LittleEndian, &timestampNanos)
	if err != nil {
		err = fmt.Errorf("decode; cannot read timestamp: %w", err)
		return
	}

	m.TimestampUTC = time.Unix(0, timestampNanos).UTC()

	var dataSizeBytes uint64
	err = binary.Read(r, binary.LittleEndian, &dataSizeBytes)
	if err != nil {
		err = fmt.Errorf("decode; cannot read data size: %w", err)
		return
	}
	m.Data = make([]byte, dataSizeBytes)
	_, err = r.Read(m.Data)
	if err != nil {
		err = fmt.Errorf("decode; cannot read data: %w", err)
	}
	return
}
