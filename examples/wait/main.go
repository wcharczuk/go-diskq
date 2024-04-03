package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/wcharczuk/go-diskq"
)

func main() {
	testPath, done := tempDir()
	defer done()
	fmt.Println("using testPath", testPath)

	cfg := diskq.Options{
		PartitionCount:   1,
		SegmentSizeBytes: 1024,
	}
	dq, err := diskq.New(testPath, cfg)
	maybeFatal(err)
	defer func() { _ = dq.Close() }()

	publisherPush := make(chan struct{}, 128)
	publisherQuit := make(chan struct{})
	go func() {
		var x int
		for {
			select {
			case <-publisherPush:
				_, _, err := dq.Push(diskq.Message{
					PartitionKey: fmt.Sprintf("data-%d", x),
					Data:         []byte(strings.Repeat("a", 512)),
				})
				if err != nil {
					fmt.Println("push err", err.Error())
				} else {
					fmt.Println("pushed", fmt.Sprintf("data-%d", x))
				}
				x++
			case <-publisherQuit:
				fmt.Println("publisher quitting")
				return
			}
		}
	}()
	defer close(publisherQuit)

	c, err := diskq.OpenConsumer(testPath, 0, diskq.ConsumerOptions{
		StartBehavior: diskq.ConsumerStartBehaviorOldest,
		EndBehavior:   diskq.ConsumerEndBehaviorWait,
	})
	defer func() { _ = c.Close() }()
	maybeFatal(err)

	for x := 0; x < 128; x++ {
		publisherPush <- struct{}{}
		msg, ok := <-c.Messages()
		if !ok {
			fmt.Println("messages closed!")
			return
		}
		if msg.Message.PartitionKey != fmt.Sprintf("data-%d", x) {
			maybeFatal(fmt.Errorf("expected %s, got %s", fmt.Sprintf("data-%d", x), msg.Message.PartitionKey))
		}
		fmt.Println("received", msg.Message.PartitionKey)
	}
}

func maybeFatal(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}

func tempDir() (string, func()) {
	dir := filepath.Join(os.TempDir(), diskq.UUIDv4().String())
	_ = os.Mkdir(dir, 0755)
	return dir, func() {
		_ = os.RemoveAll(dir)
	}
}
