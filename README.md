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

Within the directory there is an `owner` file if there is a currrent active producer so that we don't allow another producer to be created.

As well there is a `partitions` directory that contains sub-directory for each partition.

Within each partition there are a number of triplets of files.
- a data file which contains binary representations of each message
- an index file that contains a triplet of uint64 values, the offset, the offset in bytes in the datafile the offset exists, and the size in bytes of the message.
- a timeindex file that contains a pair of the offset, and the timestamp as a unix timestamp in nanos that represents the timestamp of the message.

The binary encoding of the message itself is broken up into
- a string partition key with a size prefix as bytes (i.e. [size][partition_key_bytes})
- a timestamp in nanos as uint64
- the message data as bytes with a size prefix (i.e. [size][data_bytes])

# `diskq` cli

Included in the repository is a cli tool to read from disk data directories.
