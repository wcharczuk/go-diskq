package diskq

import "time"

type Message struct {
	Offset       uint64
	PartitionKey string
	Timestamp    time.Time
	Data         []byte
}
