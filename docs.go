/*
Package diskq provides a local filesystem streaming system similar to Kafka.

A simplified example might be:

	    producer, _ := diskq.New("streams", diskq.Options{})
		producer.Push(diskq.Message{Data:[]byte("hello world!")})

Here we both initialize and open for writing a diskq producer.

We then push a message into it, randomly assigning it to one of 3 partitions.

We can then consume messages from outside the producing process simply by listening
for filesystem write events.
*/
package diskq
