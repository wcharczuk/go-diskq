package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/wcharczuk/go-diskq"
)

var flagPath = flag.String("path", "", "The data path (if unset, a temporary dir will be created")
var flagPartition = flag.Int("partition", -1, "A specific partition to consume (-1 for all)")
var flagStartBehavior = flag.String("start", "beginning", "The start behavior (one of 'beginning', 'at-offset', 'active-start', 'active-latest')")
var flagStartAtOffset = flag.Int("start-offset", 0, "The offset to start reading from (if the start behavior is 'at offset')")
var flagEndBehavior = flag.String("end", "close", "The end behavior (one of 'wait', 'at-offset', 'close')")
var flagEndAtOffset = flag.Int("end-offset", 0, "The offset to end reading from (if the end behavior is 'at-offset')")

func main() {
	flag.Parse()

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

	if *flagPartition >= 0 {
		consumer, err := diskq.OpenConsumer(*flagPath, uint32(*flagPartition), consumerOptions)
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

	consumerGroup, err := diskq.OpenConsumerGroup(*flagPath, func(_ uint32) diskq.ConsumerOptions { return consumerOptions })
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	defer func() { _ = consumerGroup.Close() }()
	go func() {
		for {
			msg, ok := <-consumerGroup.Messages()
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
	case err, _ = <-consumerGroup.Errors():
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		}
		return
	}
}
