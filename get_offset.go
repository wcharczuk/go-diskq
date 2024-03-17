package diskq

// GetOffset finds and decodes a message by offset for a given partition and returns it.
func GetOffset(path string, partitionIndex uint32, offset uint64) (m Message, ok bool, err error) {
	startOffset, ok, err := getPartitionSegmentForOffsetUnsafe(path, partitionIndex, offset)
	if err != nil || !ok {
		return
	}
	m, ok, err = getSegmentOffset(path, partitionIndex, startOffset, offset)
	return
}

func getPartitionSegmentForOffsetUnsafe(path string, partitionIndex uint32, offset uint64) (startOffset uint64, ok bool, err error) {
	var entries []uint64
	entries, err = GetPartitionSegmentStartOffsets(path, partitionIndex)
	if err != nil {
		return
	}
	startOffset, ok = GetSegmentStartOffsetForOffset(entries, offset)
	return
}
