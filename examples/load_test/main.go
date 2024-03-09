package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/wcharczuk/go-diskq"
)

func main() {
	tempPath, done := tempDir()
	defer done()

	cfg := diskq.Config{
		Path: tempPath,
	}

	dq, err := diskq.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}

	var messagesPublished, messagesProcessed uint64
	go func() {
		for {
			atomic.AddUint64(&messagesPublished, 1)
			dq.Push(diskq.Message{
				PartitionKey: fmt.Sprintf("message-%d", messagesPublished),
				Data:         []byte("data"),
			})
		}
	}()

	c0, err := dq.Consume(0, diskq.ConsumerOptions{
		StartAtBehavior: diskq.ConsumerStartAtBeginning,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	go func() {
		for {
			select {
			case _, ok := <-c0.Messages():
				if !ok {
					fmt.Println("c0 channel closed")
					return
				}
				atomic.AddUint64(&messagesProcessed, 1)
			}
		}
	}()

	c1, err := dq.Consume(1, diskq.ConsumerOptions{
		StartAtBehavior: diskq.ConsumerStartAtBeginning,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	go func() {
		for {
			select {
			case _, ok := <-c1.Messages():
				if !ok {
					fmt.Println("c1 channel closed")
					return
				}
				atomic.AddUint64(&messagesProcessed, 1)
			}
		}
	}()

	c2, _ := dq.Consume(2, diskq.ConsumerOptions{
		StartAtBehavior: diskq.ConsumerStartAtBeginning,
	})
	go func() {
		for {
			select {
			case _, ok := <-c2.Messages():
				if !ok {
					fmt.Println("c2 channel closed")
					return
				}
				atomic.AddUint64(&messagesProcessed, 1)
			}
		}
	}()

	var last time.Time = time.Now()
	var lastMessagesPublished, lastMessagesProcessed uint64 = messagesPublished, messagesProcessed
	go func() {
		for range time.Tick(5 * time.Second) {
			delta := time.Since(last)
			publishedRate := float64(messagesPublished-lastMessagesPublished) / (float64(delta / time.Second))
			processedRate := float64(messagesProcessed-lastMessagesProcessed) / (float64(delta / time.Second))
			fmt.Printf("messages sent=%0.2f/sec proccessed=%0.2f/sec\n", publishedRate, processedRate)
			last = time.Now()
			lastMessagesPublished = messagesPublished
			lastMessagesProcessed = messagesProcessed
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	select {
	case <-sig:
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
