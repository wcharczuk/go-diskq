package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/urfave/cli/v3"
	"github.com/wcharczuk/go-diskq"
)

var flagPath = flag.String("path", "", "The data path (if unset, a temporary dir will be created")
var flagPartition = flag.Int("partition", -1, "A specific partition to consume (-1 for all)")
var flagStartBehavior = flag.String("start", "beginning", "The start behavior (one of 'beginning', 'at-offset', 'active-start', 'active-latest')")
var flagStartAtOffset = flag.Int("start-offset", 0, "The offset to start reading from (if the start behavior is 'at offset')")
var flagEndBehavior = flag.String("end", "close", "The end behavior (one of 'wait', 'at-offset', 'close')")
var flagEndAtOffset = flag.Int("end-offset", 0, "The offset to end reading from (if the end behavior is 'at-offset')")

func main() {
	root := &cli.Command{
		Name: "diskq",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "path",
				Usage: "The path to the stream data",
			},
		},
	}
	if err := root.Run(context.Background(), os.Args); err != nil {
		maybeFatal(err)
	}
}

func maybeFatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func commandRead() *cli.Command {
	return &cli.Command{
		Name:  "read",
		Usage: "Read raw message data from a stream and print to standard out",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:    "partition",
				Aliases: []string{"p"},
				Value:   -1,
				Usage:   "A specific partition to read from (-1 will read from all partitions)",
			},
			&cli.StringFlag{
				Name:  "start",
				Value: "beginning",
				Usage: "The consumer start behavior (one of 'beginning', 'at-offset', 'active-start', 'active-latest')",
			},
			&cli.UintFlag{
				Name:  "start-at",
				Usage: "The start at offset if the start behavior is `at-offset`",
			},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			return nil
		},
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
			signal.Reset(os.Interrupt)
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
	defer func() {
		_ = consumerGroup.Close()
	}()
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
		signal.Reset(os.Interrupt)
		return
	case err, _ = <-consumerGroup.Errors():
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		}
		return
	}
}
