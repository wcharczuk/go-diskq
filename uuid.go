package diskq

import (
	"crypto/rand"
	"encoding/hex"
)

// UUID is a unique id.
type UUID [16]byte

func UUIDv4() (output UUID) {
	_, _ = rand.Read(output[:])
	output[6] = (output[6] & 0x0f) | 0x40 // Version 4
	output[8] = (output[8] & 0x3f) | 0x80 // Variant is 10
	return
}

var zero UUID

// IsZero returns if the identifier is unset.
func (id UUID) IsZero() bool {
	return id == zero
}

// String returns the full hex representation of the id.
func (id UUID) String() string {
	var buf [32]byte
	hex.Encode(buf[:], id[:])
	return string(buf[:])
}

// Short returns the short hex representation of the id.
//
// In practice this is the last ~8 bytes of the identifier.
func (id UUID) Short() string {
	var buf [8]byte
	hex.Encode(buf[:], id[12:])
	return string(buf[:])
}
