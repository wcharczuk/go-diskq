package main

import (
	"context"
	"encoding/json"
	"fmt"
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
}

func main() {
	root := &cli.Command{
		Name:  "diskq",
		Usage: "Interact with diskq streams on disk.",
		Commands: []*cli.Command{
			commandRead(),
			commandWrite(),
			commandVacuum(),
			commandStats(),
		},
	}
	if err := root.Run(context.Background(), os.Args); err != nil {
		maybeFatal(err)
	}
}

func commandWrite() *cli.Command {
	return &cli.Command{
		Name:  "write",
		Usage: "Write message data read from STDIN to a given stream.",
		Flags: globalFlags,
		Action: func(_ context.Context, cmd *cli.Command) error {
			dq, err := diskq.New(diskq.Config{
				Path: cmd.String("path"),
			})
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
			fmt.Printf("success! partition=%d offset=%d\n", partition, offset)
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
			dq, err := diskq.New(diskq.Config{
				Path:              cmd.String("path"),
				RetentionMaxBytes: cmd.Int("max-bytes"),
				RetentionMaxAge:   cmd.Duration("max-age"),
			})
			defer func() { _ = dq.Close() }()
			if err != nil {
				return err
			}
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

			consumerOptions := diskq.ConsumerOptions{
				StartBehavior: startBehavior,
				StartOffset:   cmd.Uint("start-offset"),
				EndBehavior:   endBehavior,
				EndOffset:     cmd.Uint("end-offset"),
			}

			flagPath := cmd.String("path")
			flagPartition := cmd.Int("partition")

			if flagPartition >= 0 {
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
						fmt.Println(string(msg.Data))
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

			consumerGroup, err := diskq.OpenConsumerGroup(flagPath, func(_ uint32) diskq.ConsumerOptions { return consumerOptions })
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
					fmt.Println(string(msg.Data))
				}
			}()
			shutdown := make(chan os.Signal, 1)
			signal.Notify(shutdown, os.Interrupt)
			select {
			case <-shutdown:
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

func maybeFatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
