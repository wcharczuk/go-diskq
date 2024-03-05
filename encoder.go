package diskq

// Encoder is a type that can encode and decode messages.
type Encoder[A any] interface {
	Encode(A) ([]byte, error)
	Deocde([]byte) (A, error)
}
