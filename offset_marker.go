package diskq

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// NewOffsetMarker creates a new offset marker.
//
// Offset markers record consumer progress to a given file exclusively, and will attempt to read a latest offset
// from that path if it already exists.
func NewOffsetMarker(path string, options OffsetMarkerOptions) (OffsetMarker, bool, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, false, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, false, err
	}
	var latestOffset uint64
	var found bool
	if stat.Size() > 0 {
		if err := binary.Read(file, binary.LittleEndian, &latestOffset); err != nil {
			return nil, false, err
		}
		found = true
	}
	om := &offsetMarker{
		options:      options,
		latestOffset: latestOffset,
		file:         file,
	}
	if om.options.AutosyncInterval > 0 || options.AutosyncEveryOffset > 0 {
		if om.options.AutosyncInterval > 0 {
			om.ticker = time.NewTicker(om.options.AutosyncInterval)
		}
		om.started = make(chan struct{})
		om.done = make(chan struct{})
		om.exited = make(chan struct{})
		om.errors = make(chan error, 1)
		om.everyOffset = make(chan struct{})
		go om.autosync()
		<-om.started
	}
	return om, found, nil
}

// OffsetMarker is the exported interface for offset markers to facilitate mocking.
type OffsetMarker interface {
	io.Closer

	// Latest returns the last offset seen as passed to `Record`.
	Latest() uint64

	// Record adds a new latest offset to the offset marker.
	Record(uint64)

	// Sync writes the latest offset to disk.
	Sync() error

	// Errors returns a channel of errors generated by automatic
	Errors() <-chan error
}

// OffsetMarkerOptions are options for autosyncing an offset marker.
type OffsetMarkerOptions struct {
	// AutosyncInterval enable autosync, specifically such that
	// every given interval in time the marker will record the lastet
	// offset recorded to the disk file.
	AutosyncInterval time.Duration
	// AutosyncEveryOffset enable autosync, specifically such that
	// every N offsets recorded the marker will sync the latest offset
	// to the disk file.
	AutosyncEveryOffset uint64
}

type offsetMarker struct {
	closeMu      sync.Mutex
	syncMu       sync.Mutex
	options      OffsetMarkerOptions
	latestOffset uint64
	offsetSeen   uint64
	file         *os.File
	errors       chan error
	ticker       *time.Ticker
	everyOffset  chan struct{}
	started      chan struct{}
	done         chan struct{}
	exited       chan struct{}
}

func (om *offsetMarker) Latest() uint64 {
	return atomic.LoadUint64(&om.latestOffset)
}

func (om *offsetMarker) Record(offset uint64) {
	atomic.StoreUint64(&om.latestOffset, offset)
	if om.options.AutosyncEveryOffset > 0 {
		if atomic.AddUint64(&om.offsetSeen, 1) >= om.options.AutosyncEveryOffset {
			select {
			case <-om.done:
				break
			case om.everyOffset <- struct{}{}:
			default:
			}
			atomic.StoreUint64(&om.offsetSeen, 0)
		}
	}
}

func (om *offsetMarker) Sync() error {
	om.syncMu.Lock()
	defer om.syncMu.Unlock()
	_, err := om.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	if err := binary.Write(om.file, binary.LittleEndian, atomic.LoadUint64(&om.latestOffset)); err != nil {
		return err
	}
	return om.file.Sync()
}

func (om *offsetMarker) Errors() <-chan error {
	return om.errors
}

func (om *offsetMarker) Close() error {
	om.closeMu.Lock()
	defer om.closeMu.Unlock()

	// sync one last time.
	if err := om.Sync(); err != nil {
		return err
	}

	if om.done != nil {
		close(om.done)
		<-om.exited
		om.done = nil
	}

	if om.errors != nil {
		close(om.errors)
		om.errors = nil
	}
	if om.everyOffset != nil {
		close(om.everyOffset)
		om.everyOffset = nil
	}
	if om.ticker != nil {
		om.ticker.Stop()
		om.ticker = nil
	}
	if om.file != nil {
		_ = om.file.Close()
		om.file = nil
	}
	return nil
}

func (om *offsetMarker) autosync() {
	if om.started != nil {
		close(om.started)
		om.started = nil
	}
	defer func() {
		if om.exited != nil {
			close(om.exited)
			om.exited = nil
		}
	}()

	if om.options.AutosyncInterval > 0 && om.options.AutosyncEveryOffset > 0 {
		var ok bool
		for {
			select {
			case <-om.done:
				return
			case _, ok = <-om.everyOffset:
				if !ok {
					return
				}
				if err := om.Sync(); err != nil {
					select {
					case <-om.done:
						return
					case om.errors <- err:
					}
					return
				}
			case _, ok = <-om.ticker.C:
				if !ok {
					return
				}
				if err := om.Sync(); err != nil {
					select {
					case <-om.done:
						return
					case om.errors <- err:
					}
					return
				}
			}
		}
	} else if om.options.AutosyncEveryOffset > 0 {
		var ok bool
		for {
			select {
			case <-om.done:
				return
			case _, ok = <-om.everyOffset:
				if !ok {
					return
				}
				if err := om.Sync(); err != nil {
					select {
					case <-om.done:
						return
					case om.errors <- err:
					}
					return
				}
			}
		}
	} else if om.options.AutosyncInterval > 0 {
		var ok bool
		for {
			select {
			case <-om.done:
				return
			case _, ok = <-om.ticker.C:
				if !ok {
					return
				}
				if err := om.Sync(); err != nil {
					select {
					case <-om.done:
						return
					case om.errors <- err:
					}
					return
				}
			}
		}
	}
}
