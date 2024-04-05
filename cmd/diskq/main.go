package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
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
		Flags: append(globalFlags, &cli.IntFlag{
			Name: "segment-max-size-bytes",
		}),
		Action: func(_ context.Context, cmd *cli.Command) error {
			dq, err := diskq.New(cmd.String("path"), diskq.Options{})
			if err != nil {
				return err
			}
			var message diskq.Message
			if err := json.NewDecoder(os.Stdin).Decode(&message); err != nil {
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
			dq, err := diskq.New(cmd.String("path"), diskq.Options{
				RetentionMaxBytes: cmd.Int("max-bytes"),
				RetentionMaxAge:   cmd.Duration("max-age"),
			})
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
				Usage:   "A specific partition to read from (-1 will read from all partitions)",
			},
			&cli.StringFlag{
				Name:  "start",
				Value: diskq.ConsumerStartBehaviorOldest.String(),
				Usage: fmt.Sprintf("The consumer start behavior (one of %q, %q, %q, %q)",
					diskq.ConsumerStartBehaviorOldest.String(),
					diskq.ConsumerStartBehaviorAtOffset.String(),
					diskq.ConsumerStartBehaviorActiveSegmentOldest.String(),
					diskq.ConsumerStartBehaviorNewest.String(),
				),
			},
			&cli.UintFlag{
				Name: "start-offset",
				Usage: fmt.Sprintf("The start at offset if the start behavior is %q",
					diskq.ConsumerStartBehaviorAtOffset.String(),
				),
			},
			&cli.StringFlag{
				Name:  "end",
				Value: diskq.ConsumerEndBehaviorClose.String(),
				Usage: fmt.Sprintf("The consumer end behavior (one of %q, %q, %q)",
					diskq.ConsumerEndBehaviorClose.String(),
					diskq.ConsumerEndBehaviorAtOffset.String(),
					diskq.ConsumerEndBehaviorWait.String(),
				),
			},
			&cli.UintFlag{
				Name: "end-offset",
				Usage: fmt.Sprintf("The end at offset if the end behavior is %q",
					diskq.ConsumerEndBehaviorAtOffset.String(),
				),
			},
			&cli.StringFlag{
				Name:    "offset-marker-path",
				Aliases: []string{"omp"},
				Value:   "",
				Usage:   "A directory to hold offset markers for consumers to enable resuming work.",
			},
			&cli.DurationFlag{
				Name:  "offset-marker-interval",
				Value: 5 * time.Second,
				Usage: "The interval to automatically flush offset marker offsets.",
			},
			&cli.BoolFlag{
				Name:    "verbose",
				Aliases: []string{"v"},
				Value:   false,
				Usage:   "If we should show the 'full' message output including the partition key, the offset, and timestamp.",
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

			if flagPartition >= 0 {
				consumerOptions := diskq.ConsumerOptions{
					StartBehavior: startBehavior,
					StartOffset:   cmd.Uint("start-offset"),
					EndBehavior:   endBehavior,
					EndOffset:     cmd.Uint("end-offset"),
				}
				offsetMarkerPath := cmd.String("offset-marker-path")
				var om diskq.OffsetMarker
				var found bool
				if offsetMarkerPath != "" {
					om, found, err = diskq.NewOffsetMarker(offsetMarkerPath, diskq.OffsetMarkerOptions{
						AutosyncInterval: cmd.Duration("offset-marker-interval"),
					})
					if err != nil {
						return err
					}
					defer func() { _ = om.Close() }()
					if found {
						debugf(cmd, "diskq; resuming at offset", "partition", flagPartition, "offset", om.Offset())
						consumerOptions.StartBehavior = diskq.ConsumerStartBehaviorAtOffset
						consumerOptions.StartOffset = om.Offset()
					} else {
						debugf(cmd, "diskq; skipping resuming at offset", "partition", flagPartition)
					}
				}

				consumer, err := diskq.OpenConsumer(flagPath, uint32(flagPartition), consumerOptions)
				if err != nil {
					return err
				}
				defer func() { _ = consumer.Close() }()

				go func() {
					for {
						msg, ok := <-consumer.Messages()
						if !ok {
							return
						}
						if om != nil {
							om.AddOffset(msg.Offset)
						}
						readPrint(msg, flagVerbose)
					}
				}()
				shutdown := make(chan os.Signal, 1)
				signal.Notify(shutdown, os.Interrupt)
				select {
				case <-shutdown:
					signal.Reset(os.Interrupt)
					return nil
				case err, ok := <-consumer.Errors():
					if !ok {
						return nil
					}
					return err
				}
			}

			var consumerGroupOptions diskq.ConsumerGroupOptions
			offsetMarkerPath := cmd.String("offset-marker-path")
			var offsetMarkersMu sync.Mutex
			offsetMarkers := make(map[uint32]diskq.OffsetMarker)
			if offsetMarkerPath != "" {
				if err := os.MkdirAll(offsetMarkerPath, 0755); err != nil {
					return err
				}
				consumerGroupOptions.OnCreateConsumer = func(partitionIndex uint32) (diskq.ConsumerOptions, error) {
					path := filepath.Join(offsetMarkerPath, diskq.FormatPartitionIndexForPath(partitionIndex))
					om, found, err := diskq.NewOffsetMarker(path, diskq.OffsetMarkerOptions{
						AutosyncInterval: cmd.Duration("offset-marker-interval"),
					})
					if err != nil {
						return diskq.ConsumerOptions{}, err
					}
					offsetMarkersMu.Lock()
					offsetMarkers[partitionIndex] = om
					offsetMarkersMu.Unlock()
					consumerOptions := diskq.ConsumerOptions{
						StartBehavior: startBehavior,
						StartOffset:   cmd.Uint("start-offset"),
						EndBehavior:   endBehavior,
						EndOffset:     cmd.Uint("end-offset"),
					}
					if found {
						debugf(cmd, "diskq; resuming at offset", "partition", partitionIndex, "offset", om.Offset())
						consumerOptions.StartBehavior = diskq.ConsumerStartBehaviorAtOffset
						consumerOptions.StartOffset = om.Offset()
					} else {
						debugf(cmd, "diskq; skipping resuming at offset", "partition", partitionIndex)
					}
					return consumerOptions, nil
				}
				consumerGroupOptions.OnCloseConsumer = func(partitionIndex uint32) error {
					offsetMarkersMu.Lock()
					defer offsetMarkersMu.Unlock()
					if om, ok := offsetMarkers[partitionIndex]; ok {
						debugf(cmd, "diskq; closing offset marker", "partition", partitionIndex, "offset", om.Offset())
						if err := om.Close(); ok {
							return err
						}
						delete(offsetMarkers, partitionIndex)
					}
					return nil
				}
			} else {
				debugf(cmd, "diskq; starting consumer group with static consumer options")
				consumerGroupOptions = diskq.ConsumerGroupOptionsFromConsumerOptions(diskq.ConsumerOptions{
					StartBehavior: startBehavior,
					StartOffset:   cmd.Uint("start-offset"),
					EndBehavior:   endBehavior,
					EndOffset:     cmd.Uint("end-offset"),
				})
			}

			consumerGroup, err := diskq.OpenConsumerGroup(flagPath, consumerGroupOptions)
			if err != nil {
				return err
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
					if offsetMarkerPath != "" {
						offsetMarkersMu.Lock()
						offsetMarkers[msg.PartitionIndex].AddOffset(msg.Offset)
						offsetMarkersMu.Unlock()
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
				return nil
			case err, ok := <-consumerGroup.Errors():
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
