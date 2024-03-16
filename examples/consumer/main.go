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
var flagStartBehavior = flag.String("start-behavior", diskq.ConsumerStartBehaviorOldest.String(), "The start behavior (one of 'beginning', 'at-offset', 'active-start', 'active-latest')")
var flagStartAtOffset = flag.Int("start-at-offset", 0, "The offset to start reading from (if the start behavior is 'at offset')")
var flagEndBehavior = flag.String("end-behavior", diskq.ConsumerEndBehaviorClose.String(), "The end behavior (one of 'wait', 'at-offset', 'close')")
var flagEndAtOffset = flag.Int("end-at-offset", 0, "The offset to end reading from (if the end behavior is 'at-offset')")
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

	startBehavior, err := diskq.ParseConsumerStartBehavior(*flagStartBehavior)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}

	endBehavior, err := diskq.ParseConsumerEndBehavior(*flagEndBehavior)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}

	marker, markerFound, err := diskq.NewOffsetMarker(filepath.Join(path, fmt.Sprintf("consumer-%03d", *flagPartition)), diskq.OffsetMarkerOptions{
		AutosyncInterval: time.Second,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	defer func() { _ = marker.Close() }()

	consumerOptions := diskq.ConsumerOptions{
		StartBehavior: startBehavior,
		StartOffset:   uint64(*flagStartAtOffset),
		EndBehavior:   endBehavior,
		EndOffset:     uint64(*flagEndAtOffset),
	}
	if markerFound {
		fmt.Println("using existing marker offset:", marker.Latest())
		consumerOptions.StartBehavior = diskq.ConsumerStartBehaviorAtOffset
		consumerOptions.StartOffset = marker.Latest()
	}

	consumer, err := diskq.OpenConsumer(path, uint32(*flagPartition), consumerOptions)
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

			fmt.Printf("<- consumer %d received %v, age %v\n", *flagPartition, msg.Offset, time.Now().UTC().Sub(msg.TimestampUTC))
			if *flagProcessingDelay > 0 {
				time.Sleep(*flagProcessingDelay)
			}
			marker.Record(msg.Offset)
			fmt.Printf("<- consumer %d finished processing %v\n", *flagPartition, msg.Offset)
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
