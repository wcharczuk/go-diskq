package diskq

import "time"

type Message struct {
	Offset       uint64
	PartitionKey string
	TimestampUTC time.Time
	Data         []byte
}
