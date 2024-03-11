package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/wcharczuk/go-diskq"
)

var flagPath = flag.String("path", "", "The data path (if unset, a temporary dir will be created")
var flagPartitions = flag.Int("partitions", 3, "The number of partitions to use (defaults to 3)")
var flagInterval = flag.Duration("interval", 5*time.Second, "The mocked publish interval")

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
		Path:           path,
		PartitionCount: uint32(*flagPartitions),
	}

	dq, err := diskq.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	defer dq.Close()

	var messagesPublished uint64
	var errors = make(chan error, 1)
	go func() {
		for range time.Tick(*flagInterval) {
			atomic.AddUint64(&messagesPublished, 1)
			partition, offset, err := dq.Push(diskq.Message{
				PartitionKey: fmt.Sprintf("message-%d", messagesPublished),
				Data:         []byte("data"),
			})
			if err != nil {
				errors <- err
				return
			}
			fmt.Printf("-> published message; partition=%d offset=%d partition_key=%s\n", partition, offset, fmt.Sprintf("message-%d", messagesPublished))
		}
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt)
	select {
	case <-shutdown:
		return
	case err = <-errors:
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
