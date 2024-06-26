package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/urfave/cli/v3"
	"github.com/wcharczuk/go-diskq"
)

var globalFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "path",
		Usage:    "The path to the stream data",
		Required: true,
	},
	&cli.BoolFlag{
		Name:     "debug",
		Usage:    "If debug output should be shown",
		Required: false,
	},
}

func main() {
	root := &cli.Command{
		Name:  "diskq",
		Usage: "Interact with diskq streams on disk.",
		Commands: []*cli.Command{
			commandRead(),
			commandCreate(),
			commandWrite(),
			commandVacuum(),
			commandStats(),
		},
	}
	if err := root.Run(context.Background(), os.Args); err != nil {
		maybeFatal(err)
	}
}

func debugf(cmd *cli.Command, format string, args ...any) {
	if cmd.Bool("debug") {
		slog.Info(format, args...)
	}
}

func commandCreate() *cli.Command {
	return &cli.Command{
		Name:  "create",
		Usage: "Create an empty diskq stream.",
		Flags: append(globalFlags,
			&cli.IntFlag{
				Name:  "segment-size-bytes",
				Usage: "The size of segments in bytes.",
				Value: diskq.DefaultSegmentSizeBytes,
			},
			&cli.IntFlag{
				Name:  "partition-count",
				Usage: "The number of diskq partitions.",
				Value: diskq.DefaultPartitionCount,
			},
		),
		Action: func(_ context.Context, cmd *cli.Command) error {
			dq, err := diskq.New(cmd.String("path"), diskq.Options{
				PartitionCount:   uint32(cmd.Int("partition-count")),
				SegmentSizeBytes: int64(cmd.Int("segment-size-bytes")),
			})
			if err != nil {
				return err
			}
			defer func() { _ = dq.Close() }()
			fmt.Printf("success! diskq created at %q with partitions=%d segment-size-bytes=%d\n", cmd.String("path"), uint32(cmd.Int("partition-count")), int64(cmd.Int("segment-size-bytes")))
			return nil
		},
	}
}

func commandWrite() *cli.Command {
	return &cli.Command{
		Name:  "write",
		Usage: "Write message data read from STDIN to a given stream.",
		Flags: append(globalFlags,
			&cli.IntFlag{
				Name:  "segment-size-bytes",
				Usage: "The size of segments in bytes.",
				Value: diskq.DefaultSegmentSizeBytes,
			},
			&cli.IntFlag{
				Name:  "partition-count",
				Usage: "The number of diskq partitions.",
				Value: diskq.DefaultPartitionCount,
			},
			&cli.StringFlag{
				Name:  "partition-key",
				Usage: "The partition key to use for the message.",
			},
			&cli.TimestampFlag{
				Name:  "timestamp",
				Usage: "The timestamp to use for the message.",
			},
		),
		Action: func(_ context.Context, cmd *cli.Command) error {
			options, found, err := diskq.MaybeReadOptions(cmd.String("path"))
			if err != nil {
				return err
			}
			if !found {
				options.PartitionCount = uint32(cmd.Int("partition-count"))
				options.SegmentSizeBytes = int64(cmd.Int("segment-size-bytes"))
			}
			dq, err := diskq.New(cmd.String("path"), options)
			if err != nil {
				return err
			}
			defer func() { _ = dq.Close() }()
			message := diskq.Message{
				PartitionKey: cmd.String("partition-key"),
				TimestampUTC: cmd.Timestamp("timestamp"),
			}
			message.Data, err = io.ReadAll(os.Stdin)
			if err != nil {
				return err
			}
			partition, offset, err := dq.Push(message)
			if err != nil {
				return err
			}
			fmt.Printf("success! wrote message to partition=%d offset=%d\n", partition, offset)
			return nil
		},
	}
}

func commandVacuum() *cli.Command {
	return &cli.Command{
		Name:  "vacuum",
		Usage: "Vacuum a given stream, that is delete segments out of retention.",
		Flags: append(globalFlags,
			&cli.IntFlag{
				Name:  "max-bytes",
				Usage: "The maximum stream size in bytes",
				Value: 1 << 30,
			},
			&cli.DurationFlag{
				Name:  "max-age",
				Usage: "The maximum stream age as a duration",
				Value: 48 * time.Hour,
			},
		),
		Action: func(_ context.Context, cmd *cli.Command) error {
			options, found, err := diskq.MaybeReadOptions(cmd.String("path"))
			if err != nil {
				return err
			}
			if !found {
				options.PartitionCount = uint32(cmd.Int("partition-count"))
				options.SegmentSizeBytes = int64(cmd.Int("segment-size-bytes"))
				options.RetentionMaxBytes = cmd.Int("max-bytes")
				options.RetentionMaxAge = cmd.Duration("max-age")
			}
			dq, err := diskq.New(cmd.String("path"), options)
			if err != nil {
				return err
			}
			defer func() { _ = dq.Close() }()
			if err := dq.Vacuum(); err != nil {
				return nil
			}
			fmt.Println("vacuum ok!")
			return nil
		},
	}
}

func commandStats() *cli.Command {
	return &cli.Command{
		Name:  "stats",
		Usage: "Show stats about a given stream as json.",
		Flags: globalFlags,
		Action: func(_ context.Context, cmd *cli.Command) error {
			stats, err := diskq.GetStats(cmd.String("path"))
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(stats)
		},
	}
}

func commandRead() *cli.Command {
	return &cli.Command{
		Name:  "read",
		Usage: "Read raw message data from a stream and print to standard out.",
		Flags: append(globalFlags,
			&cli.IntFlag{
				Name:    "partition",
				Aliases: []string{"p"},
				Value:   -1,
				Usage:   "A specific partition index to read from (-1 will read from all partitions).",
			},
			&cli.StringFlag{
				Name:  "start",
				Value: diskq.ConsumerStartBehaviorOldest.String(),
				Usage: fmt.Sprintf("The consumer start behavior (one of %q, %q, %q, %q).",
					diskq.ConsumerStartBehaviorOldest.String(),
					diskq.ConsumerStartBehaviorAtOffset.String(),
					diskq.ConsumerStartBehaviorActiveSegmentOldest.String(),
					diskq.ConsumerStartBehaviorNewest.String(),
				),
			},
			&cli.UintFlag{
				Name: "start-offset",
				Usage: fmt.Sprintf("The start at offset if the start behavior is %q.",
					diskq.ConsumerStartBehaviorAtOffset.String(),
				),
			},
			&cli.StringFlag{
				Name:  "end",
				Value: diskq.ConsumerEndBehaviorClose.String(),
				Usage: fmt.Sprintf("The consumer end behavior (one of %q, %q, %q).",
					diskq.ConsumerEndBehaviorClose.String(),
					diskq.ConsumerEndBehaviorAtOffset.String(),
					diskq.ConsumerEndBehaviorWait.String(),
				),
			},
			&cli.UintFlag{
				Name: "end-offset",
				Usage: fmt.Sprintf("The end at offset if the end behavior is %q.",
					diskq.ConsumerEndBehaviorAtOffset.String(),
				),
			},
			&cli.BoolFlag{
				Name:    "verbose",
				Aliases: []string{"v"},
				Value:   false,
				Usage:   "If we should show the 'full' message output including the partition key, the offset, and timestamp as JSON.",
			},
		),
		Action: func(_ context.Context, cmd *cli.Command) error {
			startBehavior, err := diskq.ParseConsumerStartBehavior(cmd.String("start"))
			if err != nil {
				return err
			}
			endBehavior, err := diskq.ParseConsumerEndBehavior(cmd.String("end"))
			if err != nil {
				return err
			}
			flagPath := cmd.String("path")
			flagPartition := cmd.Int("partition")
			flagVerbose := cmd.Bool("verbose")
			consumerGroup, err := diskq.OpenConsumerGroup(flagPath, diskq.ConsumerGroupOptions{
				ShouldConsume: func(partitionIndex uint32) bool {
					if flagPartition == -1 {
						return true
					}
					return partitionIndex == uint32(flagPartition)
				},
				OptionsForConsumer: func(_ uint32) (diskq.ConsumerOptions, error) {
					return diskq.ConsumerOptions{
						StartBehavior: startBehavior,
						EndBehavior:   endBehavior,
					}, nil
				},
				PartitionScanInterval: 500 * time.Millisecond,
			})
			if err != nil {
				return err
			}
			go func() {
				var msg diskq.MessageWithOffset
				var ok bool
				for {
					msg, ok = <-consumerGroup.Messages()
					if !ok {
						return
					}
					readPrint(msg, flagVerbose)
				}
			}()

			shutdown := make(chan os.Signal, 1)
			signal.Notify(shutdown, os.Interrupt)
			select {
			case <-shutdown:
				debugf(cmd, "diskq; shutting down")
				signal.Reset(os.Interrupt)
				_ = consumerGroup.Close()
				return nil
			case err, ok := <-consumerGroup.Errors():
				_ = consumerGroup.Close()
				if !ok {
					return nil
				}
				return err
			}
		},
	}
}

func readPrint(msg diskq.MessageWithOffset, verbose bool) {
	if verbose {
		data, _ := json.Marshal(msg)
		fmt.Println(string(data))
	} else {
		fmt.Println(string(msg.Data))
	}
}

func maybeFatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
