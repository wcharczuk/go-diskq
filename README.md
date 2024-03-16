`diskq`
=======

![Diagram](https://github.com/wcharczuk/go-diskq/blob/main/_assets/hero.png)

## Caveats

`go-diskq` is currently in pre-release testing, and should not be used in production.

# About

The goal of `go-diskq` is to provide a single node equivalent of Kafka. Think what sqlite is to something like Postgres, a library complete implementation of Kafka which writes to a local disk.

More specifically the requirements of `diskq` were:
- Be entirely self-contained and defer to the filesystem as the source of truth.
- Durable event producers that recorded events to disk for each event to prevent data loss on process shutdown.
- Event consumers can be decoupled from the producer, that is run in a separate process, and receive messages exclusively through events in the filesystem.
- Allow consumers to mark progress using offset markers, and resume from where they left off.
- Have sub-millisecond latency from producing an event, to consuming that event.
- Allow for `vacuum` operations, where events beyond a given data size limit, or age limit, can be culled.

# File organization

A `diskq` stream is rooted in a single directory.

Within the directory there is an `owner` file if there is a currrent active producer so that we don't allow another producer to be created. The owner file will contain a single UUID that corresponds to the `*diskq.Diskq` instance that is the active producer for that stream.

In addition to the `owner` file there is a `partitions` directory that contains sub-directory for each partition, named as a zero-padded integer corresponding to the partition index (e.g. `000003`.)

Within each partition sub-directory there are a number of triplets of files, each triplet corresponding to a "segment":
- A `.data` file which contains binary representations of each message (more on this representation below.)
- A `.index` file that contains a stream of triplets of uint64 values: `[offset|bytes_offset_from_start|message_size_bytes]`
- A `.timeindex` file that contains a stream of pairs of uint64 values: `[offset|timestamp_nanos]`

Each triplet of files for a segment is named after the first offset of that segment, e.g. `00025` for a segment that starts with the `25` offset for the partition.

The last segment of a partiton is referred to as the "active" segment, and is the segment that is currently being written to.

For the message data itself, the binary encoding of the message is as follows:
- A varuint for the size of the partition key in bytes.
- A byte array of that given size holding the partition key data.
- A uint64 timestamp in nanos for the message timestamp.
- A varuint for the size of the data int bytes.
- A byte array of that given size holding the partition key data.

As a result a messages minimum size in bytes is typically ~2+1+3+2 or 8 bytes in the data file.

# `diskq` cli

Included in the repository is a cli tool to read from disk data directories.
