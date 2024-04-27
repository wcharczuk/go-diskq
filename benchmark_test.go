package diskq

import (
	"bytes"
	"testing"
)

func Benchmark_Producer(b *testing.B) {
	testPath, done := tempDir()
	b.Cleanup(done)

	dq, err := New(testPath, Options{})
	if err != nil {
		b.Errorf("diskq create error: %v", err)
		b.FailNow()
	}
	b.Cleanup(func() { _ = dq.Close() })
	for x := 0; x < b.N; x++ {
		_, _, _ = dq.Push(testMessage(x, 32))
	}
}

func Benchmark_MessageCodec_Encode(b *testing.B) {
	buf := new(bytes.Buffer)
	for x := 0; x < b.N; x++ {
		_ = Encode(testMessage(x, 256), buf)
	}
}

func Benchmark_MessageCodec_Decode(b *testing.B) {
	buf := new(bytes.Buffer)
	for x := 0; x < b.N; x++ {
		_ = Encode(testMessage(x, 256), buf)
	}
	reader := bytes.NewReader(buf.Bytes())
	b.ResetTimer()
	for x := 0; x < b.N; x++ {
		var m Message
		_ = Decode(&m, reader)
	}
}

func Benchmark_Consumer(b *testing.B) {
	benchmarkConsumer(b, 128)
}

func benchmarkConsumer(b *testing.B, messageCount int) {
	testPath, done := tempDir()
	b.Cleanup(done)

	dq, err := New(testPath, Options{
		PartitionCount:   1,
		SegmentSizeBytes: 1 << 40,
	})
	if err != nil {
		b.Errorf("diskq create error: %v", err)
		b.FailNow()
	}
	b.Cleanup(func() { _ = dq.Close() })
	for x := 0; x < messageCount*b.N; x++ {
		_, _, _ = dq.Push(testMessage(x, 32))
	}

	b.ResetTimer()
	c, err := OpenConsumer(testPath, 0, ConsumerOptions{
		StartBehavior: ConsumerStartBehaviorOldest,
		EndBehavior:   ConsumerEndBehaviorClose,
	})
	if err != nil {
		b.Errorf("diskq create error: %v", err)
		b.FailNow()
	}
	for {
		select {
		case _, ok := <-c.Messages():
			if !ok {
				return
			}
		case err, ok := <-c.Errors():
			if !ok {
				return
			}
			if err != nil {
				b.Errorf("diskq consumer error: %v", err)
				b.FailNow()
			}
		}
	}
}
