`diskq`
=======

![Diagram](https://github.com/wcharczuk/go-diskq/blob/main/_assets/hero.png)

## Caveats

`go-diskq` is currently in pre-release testing, and should not be used in production.

# Goals

The goal of `go-diskq` is to provide a single node equivalent of Kafka, similar to what sqlite is to Postgres. `go-diskq` is a library implementation of a streaming system which writes to a local disk.

It supports high throughput writing and reading, such that the process that is producing messages can be decoupled from processes that read messages, and consumption can be triggered through filesystem events.

A consumer will be notified of a new message on a stream in under a millisecond (often in single digit microseconds depending on the platform), making this useful for realtime applications.

# Stream file organization

```
${DATA_PATH}/
  owner
  parts/
    000000/
      00000000000000000000.data
      00000000000000000000.index
      00000000000000000000.timeindex
    000001/
      00000000000000000000.data
      00000000000000000000.index
      00000000000000000000.timeindex
      ...
    000002/
      00000000000000000000.data
      00000000000000000000.index
      00000000000000000000.timeindex
      ...
```

A `diskq` stream is rooted in a single directory.

Within the directory there is an `owner` file if there is a currrent active producer so that we don't allow another producer to be created. The owner file will contain a single UUID that corresponds to the `*diskq.Diskq` instance that is the active producer for that stream.

In addition to the `owner` file there is a `parts` directory that contains sub-directory for each partition, named as a six-zero-padded integer corresponding to the partition index (e.g. `000003`.)

Within each partition sub-directory there are a number of triplets of files, each triplet corresponding to a "segment":
- A `.data` file which contains binary representations of each message (more on this representation below.)
- A `.index` file that contains a stream of triplets of uint64 values: `[offset|bytes_offset_from_start|message_size_bytes]`
- A `.timeindex` file that contains a stream of pairs of uint64 values: `[offset|timestamp_nanos]`

Each triplet of files for a segment is has a prefix corresponding to the twenty-zero-padded integer of the first offset of that segment, e.g. `00000000000000000025` for a segment that starts with the `25` offset for the partition.

The last segment of a partiton is referred to as the "active" segment, and is the segment that is currently being written to.

For the message data itself, the binary encoding of the message is as follows:
- A varuint for the size of the partition key in bytes.
- A byte array of that given size holding the partition key data.
- A uint64 timestamp in nanos for the message timestamp.
- A varuint for the size of the data int bytes.
- A byte array of that given size holding the partition key data.

As a result a messages minimum size in bytes is typically ~2+1+3+2 or 8 bytes in the data file.

# `diskq` cli

Included in the repository is a cli tool to read from disk data directories, force vacuuming of partitions, display stats about a stream, and write new offsets to a stream.
