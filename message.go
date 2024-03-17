package diskq

import "time"

type Message struct {
	PartitionKey string
	TimestampUTC time.Time
	Data         []byte
}

// MessageWithOffset is a special wrapping type for messages
// that adds the partition index and the offset of messages
// read by consumers.
type MessageWithOffset struct {
	Message
	PartitionIndex uint32
	Offset         uint64
}
