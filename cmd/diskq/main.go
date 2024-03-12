package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/signal"

	"github.com/wcharczuk/go-diskq"
)

var flagPath = flag.String("path", "", "The data path (if unset, a temporary dir will be created")
var flagPartition = flag.Int("partition", 0, "The partition to consume")
var flagStartBehavior = flag.String("start-behavior", "beginning", "The start behavior (one of 'beginning', 'at-offset', 'active-start', 'active-latest')")
var flagStartAtOffset = flag.Int("start-at-offset", 0, "The offset to start reading from (if the start behavior is 'at offset')")
var flagEndBehavior = flag.String("end-behavior", "wait", "The end behavior (one of 'wait', 'at-offset', 'close')")
var flagEndAtOffset = flag.Int("end-at-offset", 0, "The offset to end reading from (if the end behavior is 'at-offset')")

func main() {
	flag.Parse()

	partitions, err := getPartitionCount(*flagPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}

	cfg := diskq.Config{
		Path:           *flagPath,
		PartitionCount: partitions,
	}

	var startBehavior = diskq.ConsumerStartAtBeginning
	switch *flagStartBehavior {
	case "beginning":
		startBehavior = diskq.ConsumerStartAtBeginning
	case "at-offset":
		startBehavior = diskq.ConsumerStartAtOffset
	case "active-start":
		startBehavior = diskq.ConsumerStartAtActiveSegmentStart
	case "active-latest":
		startBehavior = diskq.ConsumerStartAtActiveSegmentLatest
	}

	var endBehavior = diskq.ConsumerEndAndWait
	switch *flagEndBehavior {
	case "wait":
		endBehavior = diskq.ConsumerEndAndWait
	case "at-offset":
		endBehavior = diskq.ConsumerEndAtOffset
	case "close":
		endBehavior = diskq.ConsumerEndAndClose
	}
	consumerOptions := diskq.ConsumerOptions{
		StartAtBehavior: startBehavior,
		StartAtOffset:   uint64(*flagStartAtOffset),
		EndBehavior:     endBehavior,
		EndAtOffset:     uint64(*flagEndAtOffset),
	}
	consumer, err := diskq.OpenConsumer(cfg, uint32(*flagPartition), consumerOptions)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	defer func() { _ = consumer.Close() }()

	go func() {
		for {
			msg, ok := <-consumer.Messages()
			if !ok {
				return
			}
			fmt.Println(string(msg.Data))
		}
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt)
	select {
	case <-shutdown:
		return
	case err, _ = <-consumer.Errors():
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		}
		return
	}
}

func getPartitionCount(path string) (count uint32, err error) {
	var entries []fs.DirEntry
	entries, err = os.ReadDir(path)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			count++
		}
	}
	return
}
