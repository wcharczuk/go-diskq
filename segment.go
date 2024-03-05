package diskq

import "os"

// Segment represents a specific block of the log.
type Segment struct {
	startOffset   uint64
	endOffset     uint64
	dataPath      string
	indexPath     string
	timeIndexPath string

	data      *os.File
	index     *os.File
	timeIndex *os.File
}
