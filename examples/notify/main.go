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

	var messagesPublished uint64
	go func() {
		for range time.Tick(5 * time.Second) {
			atomic.AddUint64(&messagesPublished, 1)
			partition, offset, err := dq.Push(diskq.Message{
				PartitionKey: fmt.Sprintf("message-%d", messagesPublished),
				Data:         []byte("data"),
			})
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
			}
			fmt.Printf("-> published message; partition=%d offset=%d\n", partition, offset)
		}
	}()

	c0, err := dq.Consume(0, diskq.ConsumerOptions{
		StartAtBehavior: diskq.ConsumerStartAtBeginning,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	defer c0.Close()

	c1, err := dq.Consume(1, diskq.ConsumerOptions{
		StartAtBehavior: diskq.ConsumerStartAtBeginning,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	defer c1.Close()

	c2, err := dq.Consume(2, diskq.ConsumerOptions{
		StartAtBehavior: diskq.ConsumerStartAtBeginning,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	defer c2.Close()

	go func() {
		fmt.Println("c0 listening for messages")
		for {
			select {
			case msg, ok := <-c0.Messages():
				if !ok {
					fmt.Println("c0 channel closed")
					return
				}
				fmt.Println("<- c0 received", msg.Message.PartitionKey)
			}
		}
	}()
	go func() {
		fmt.Println("c1 listening for messages")
		for {
			select {
			case msg, ok := <-c1.Messages():
				if !ok {
					fmt.Println("c1 channel closed")
					return
				}
				fmt.Println("<- c1 received", msg.Message.PartitionKey)
			}
		}
	}()
	go func() {
		fmt.Println("c2 listening for messages")
		for {
			select {
			case msg, ok := <-c2.Messages():
				if !ok {
					fmt.Println("c2 channel closed")
					return
				}
				fmt.Println("<- c2 received", msg.Message.PartitionKey)
			}
		}
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt)
	select {
	case <-shutdown:
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
