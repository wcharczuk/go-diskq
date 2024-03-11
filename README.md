`diskq`
=======

## Caveats

`go-diskq` is currently in pre-release testing, and should not be used in production.

# About

The goal of `go-diskq` is to provide something like sqlite, but for Kafka.

Specifically the requirements were:
- Create durable event producers that recorded events to disk for each event to prevent dataloss on process shutdown.
- Create event consumers that can be decoupled from the producer, that is run in a separate process, and receive messages exclusively through events in the filesystem.
- Have sub-millisecond latency from producing an event, to consuming that event.
- Allow for `vacuum` operations, where events beyond a given data size limit, or age limit, or both, can be culled.
