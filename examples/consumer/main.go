package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/wcharczuk/go-diskq"
)

var flagPath = flag.String("path", "", "The data path (if unset, a temporary dir will be created")
var flagPartition = flag.Int("partition", 0, "The partition to consume")
var flagProcessingDelay = flag.Duration("processing-delay", 0, "The delay to inject into processing")

func main() {
	flag.Parse()

	var path string
	if *flagPath != "" {
		path = *flagPath
	} else {
		var done func()
		path, done = tempDir()
		defer done()

	}

	fmt.Printf("using data path: %s\n", path)

	cfg := diskq.Config{
		Path: path,
	}

	consumer, err := diskq.OpenConsumer(cfg, uint32(*flagPartition), diskq.ConsumerOptions{
		StartAtBehavior: diskq.ConsumerStartAtBeginning,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	defer func() { _ = consumer.Close() }()

	go func() {
		fmt.Printf("consumer %d listening for messages\n", *flagPartition)
		for {
			msg, ok := <-consumer.Messages()
			if !ok {
				fmt.Println("consumer channel closed")
				return
			}

			fmt.Printf("<- consumer %d received %v, age %v\n", *flagPartition, msg.Message.PartitionKey, time.Now().UTC().Sub(msg.TimestampUTC))
			if *flagProcessingDelay > 0 {
				time.Sleep(*flagProcessingDelay)
			}
			fmt.Printf("<- consumer %d finished processing %v\n", *flagPartition, msg.Message.PartitionKey)
		}
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt)
	select {
	case <-shutdown:
		return
	case err = <-consumer.Errors():
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
