package diskq

import "time"

// Message is a single element of data written to a diskq.
//
// It includes a PartitionKey used to assign it to a partition,
// and a TimestampUTC to hold how the message should be ordered
// within the time index of the partition.
//
// When you push a new message, the PartitionKey will be assigned
// a uuidv4 if unset, and the TimestampUTC will be set to the current
// time in utc.
type Message struct {
	// PartitionKey is used to assign the message to a partition.
	//
	// The key will be hashed and then bucketed by the
	// number of active partitions.
	//
	// If unset, it will be assigned a uuidv4.
	PartitionKey string
	// TimestampUTC holds the timestamp that is used in the timeindex.
	//
	// If unset it will be assigned the current time in UTC.
	TimestampUTC time.Time
	// Data is the message's contents.
	Data []byte
}

// MessageWithOffset is a special wrapping type for messages
// that adds the partition index and the offset of messages
// read by consumers.
type MessageWithOffset struct {
	Message
	// PartitionIndex is the partition the message was read from.
	PartitionIndex uint32
	// Offset is the offset within the partition the message was read from.
	Offset uint64
}
