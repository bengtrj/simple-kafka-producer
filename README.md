# Simple kafka producer
***Goal:** be able to produce events from a file into Kafka, in order to run integration tests.*
It utilizes code from https://github.com/confluentinc/confluent-kafka-go

#  What it does?
This is a simple `go` Kafka client that reads a file called `events.log` with the structure below and uses the `key` and `message` to produce an event:

```
key
message
key
message
.
.
.
key
message
```

***Note:** you can refer to the file format in the provided example file: `events.log`*

In short, it expects a file with an key in one line, and the message associated in the next line.
It produces the messages into kafka running on `localhost` default port, although you can easily change it editing `main.go`.

# Running
Usage

```
$ simple-kafka-producer topic file
```

where:
- `topic` is the name of the kafka topic to publish events
- `file` is the path to the file where the events to produce are