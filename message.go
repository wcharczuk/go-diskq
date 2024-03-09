package diskq

import "time"

type Message struct {
	PartitionKey string
	TimestampUTC time.Time
	Data         []byte
}
