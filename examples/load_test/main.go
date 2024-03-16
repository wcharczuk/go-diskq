package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/wcharczuk/go-diskq"
)

func main() {
	tempPath, done := tempDir()
	defer done()

	cfg := diskq.Config{
		Path: tempPath,
	}

	fmt.Printf("using temp path: %s\n", tempPath)

	dq, err := diskq.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	defer func() { _ = dq.Close() }()

	messagesPublished := [3]uint64{}
	messagesProcessed := [3]uint64{}

	go func() {
		for {
			partition, _, _ := dq.Push(diskq.Message{
				PartitionKey: fmt.Sprintf("message-%d", messagesPublished),
				Data:         []byte("data"),
			})
			messagesPublished[partition]++
		}
	}()

	c0, err := diskq.OpenConsumer(cfg.Path, 0, diskq.ConsumerOptions{
		StartBehavior: diskq.ConsumerStartBehaviorOldest,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	go func() {
		for {
			_, ok := <-c0.Messages()
			if !ok {
				fmt.Println("c0 channel closed")
				return
			}
			messagesProcessed[0]++
		}
	}()

	c1, err := diskq.OpenConsumer(cfg.Path, 1, diskq.ConsumerOptions{
		StartBehavior: diskq.ConsumerStartBehaviorOldest,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	go func() {
		for {
			_, ok := <-c1.Messages()
			if !ok {
				fmt.Println("c1 channel closed")
				return
			}
			messagesProcessed[1]++
		}
	}()

	c2, _ := diskq.OpenConsumer(cfg.Path, 2, diskq.ConsumerOptions{
		StartBehavior: diskq.ConsumerStartBehaviorOldest,
	})
	go func() {
		for {
			_, ok := <-c2.Messages()
			if !ok {
				fmt.Println("c2 channel closed")
				return
			}
			messagesProcessed[2]++
		}
	}()

	var last = time.Now()
	lastMessagesPublished := [3]uint64{}
	lastMessagesProcessed := [3]uint64{}

	go func() {
		for range time.Tick(5 * time.Second) {
			delta := time.Since(last)
			for x := 0; x < 3; x++ {
				publishedRate := float64(messagesPublished[x]-lastMessagesPublished[x]) / (float64(delta / time.Second))
				processedRate := float64(messagesProcessed[x]-lastMessagesProcessed[x]) / (float64(delta / time.Second))
				fmt.Printf("partition %d messages sent=%0.2f/sec proccessed=%0.2f/sec\n", x, publishedRate, processedRate)
				lastMessagesPublished[x] = messagesPublished[x]
				lastMessagesProcessed[x] = messagesProcessed[x]
			}
			last = time.Now()
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	select {
	case <-sig:
		_ = c0.Close()
		_ = c1.Close()
		_ = c2.Close()
		return
	case err = <-c0.Errors():
		fmt.Fprintln(os.Stderr, err.Error())
		return
	case err = <-c1.Errors():
		fmt.Fprintln(os.Stderr, err.Error())
		return
	case err = <-c2.Errors():
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
}

func tempDir() (string, func()) {
	dir := filepath.Join(os.TempDir(), diskq.UUIDv4().String())
	_ = os.Mkdir(dir, 0755)
	return dir, func() {
		fmt.Println("removing temp dir", dir)
		_ = os.RemoveAll(dir)
	}
}
