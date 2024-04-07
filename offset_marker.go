package diskq

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// OpenOrCreateOffsetMarker opens or creates a new offset marker.
//
// It returns both the offset marker struct itself, and a bool `found` if the offset marker already
// existed on disk. You can use this bool to control if and how you apply the offset marker to
// a consumer you're looking to start.
//
// The path parameter is required and should point to the specific path of the file that will be
// created to store the latest offset as a file. It should be unique to the specific consumer
// (i.e. unique to the diskq path and partition index) of the consumer it tracks.
//
// To store the latest offset processed, call `SetLatestOffset` on the offset marker.

// The offset isn't written to disk immediately; instead depending on the autosync options
// provided, it will write on a ticker with a given duration (e.g. every 500 milliseconds)
// or after N latest offsets are set.
//
// The offset is also written to disk regardless of the autosync options when the offset marker is closed.
//
// You can also call `Sync` on the offset marker yourself to write the latest offset to disk on demand
// though it is discouraged to do this in favor of using autosync.
func OpenOrCreateOffsetMarker(path string, options OffsetMarkerOptions) (*OffsetMarker, bool, error) {
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
	om := &OffsetMarker{
		options:      options,
		latestOffset: latestOffset,
		file:         file,
	}
	if found {
		atomic.StoreUint32(&om.hasSetLatestOffset, 1)
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

// OffsetMarkerOptions are options for autosyncing an offset marker.
type OffsetMarkerOptions struct {
	// AutosyncInterval enable autosync, specifically such that
	// every given interval in time the marker will record the lastet
	// offset recorded to the disk file.
	//
	// If AutosyncInterval is unset, the offset marker will not
	// create a ticker to sync the latest offset to disk.
	AutosyncInterval time.Duration
	// AutosyncEveryOffset enable autosync, specifically such that
	// every N offsets recorded the marker will sync the latest offset
	// to the disk file.
	//
	// If AutosyncEveryOffset is unset, this autosync type will be skipped.
	AutosyncEveryOffset uint64
}

// OffsetMarker is a file backed store for the latest offset
// seen by a consumer.
//
// It is meant to be a durable mechanism to resume work at the last
// committed offset for a given partition.
//
// OffsetMarkers do not write to disk immediately, they have configurable
// parameters for when to sync to disk in the background, reducing the
// number of writes to the file.
//
// You can alternatively call `Sync` directly on this struct, which will
// write the latest offset to disk.
type OffsetMarker struct {
	closeMu            sync.Mutex
	syncMu             sync.Mutex
	options            OffsetMarkerOptions
	hasSetLatestOffset uint32
	latestOffset       uint64
	offsetSeen         uint64
	file               *os.File
	errors             chan error
	ticker             *time.Ticker
	everyOffset        chan struct{}
	started            chan struct{}
	done               chan struct{}
	exited             chan struct{}
}

// LatestOffset returns the latest offset for the offset marker.
func (om *OffsetMarker) LatestOffset() uint64 {
	return atomic.LoadUint64(&om.latestOffset)
}

// SetLatestOffset sets the latest offset for the offset marker.
//
// Note that setting the latest offset _does not write the offset to disk_, instead
// autosync will write the latest offset to disk on a ticker or after N latest offsets are set.
//
// You can also call `Sync` to write the latest offset to disk on demand, but it is recommended
// that you just let autosync write it to disk for you to save excessive writes.
//
// The latest offset will be written to disk on Close regardless of the autosync settings.
func (om *OffsetMarker) SetLatestOffset(offset uint64) {
	atomic.StoreUint32(&om.hasSetLatestOffset, 1)
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

// Sync writes the latest offset to disk on demand.
//
// In practice you should not call this function, instead
// it is better to configure autosync settings on the offset marker options
// to write the latest offset to disk automatically.
func (om *OffsetMarker) Sync() error {
	om.syncMu.Lock()
	defer om.syncMu.Unlock()

	// skip writing if we haven't seen a latest offset.
	if atomic.LoadUint32(&om.hasSetLatestOffset) == 0 {
		return nil
	}

	_, err := om.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	if err := binary.Write(om.file, binary.LittleEndian, atomic.LoadUint64(&om.latestOffset)); err != nil {
		return err
	}
	return om.file.Sync()
}

// Errors returns a channel that will contain errors from writing
// to the offset marker file.
//
// It is important that you check these errors!
func (om *OffsetMarker) Errors() <-chan error {
	return om.errors
}

// Close will close the offset marker, flushing the last seen offset marker
// to disk and closing any other file handles or tickers.
//
// Once an offset marker is closed it cannot be reused.
func (om *OffsetMarker) Close() error {
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

func (om *OffsetMarker) autosync() {
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
