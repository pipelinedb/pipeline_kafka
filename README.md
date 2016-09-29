# pipeline_kafka ![Build Status](https://img.shields.io/circleci/token/db1a70c164cd6d96544d8eb38b279c48dea24709/project/pipelinedb/pipeline_kafka/master.svg?style=flat-square)

PipelineDB extension for Kafka support

## Requirements

You'll need to have [librdkafka](https://github.com/edenhill/librdkafka) installed on your system to build the extension. The easiest way to install it is to built it from source.

```
git clone -b 0.8 https://github.com/edenhill/librdkafka.git ~/librdkafka
cd ~/librdkafka
./configure --prefix=/usr
make
sudo make install
```

You also need PipelineDB installed and `pg_config` to be on your `PATH`.

# Installing

First build and install the extension by running:

```
./configure
make
make install
```

`pipeline_kafka` internally uses shared memory to sync state between background workers, so it must be preloaded as a shared library. You can do so by adding the following line to your `pipelinedb.conf` file. If you're already loading some shared libraries, then simply add `pipeline_kafka` as a comma-separated list.

```
shared_preload_libraries = pipeline_kafka
```

Now you can load the extension into a database:

```
=# CREATE EXTENSION pipeline_kafka;
CREATE EXTENSION
```

## Usage

`pipeline_kafka` needs to know about any Kafka brokers it can connect to. Brokers can be added with `kafka_add_broker`:

```
=# SELECT pipeline_kafka.add_broker('localhost:9092');
 add_broker
------------
 success
(1 row)
```

### Consuming Messages

Assuming the existence of a stream named `topic_stream` that a continuous view is reading from, you can now begin ingesting data from Kafka (**note**: only static streams can consume data from Kafka):

```
=# SELECT pipeline_kafka.consume_begin('kafka_topic', 'topic_stream');
 consume_begin
---------------
 success
(1 row)
```

This will launch a background worker that will read from the Kafka topic `kafka_topic`, and use `COPY` to write the messages to `topic_stream`. You can specifiy the format that `COPY` should expect, as well as a delimiter, quote and escape character:

```
=# SELECT pipeline_kafka.consume_begin('kafka_topic', 'topic_stream', format := 'text', delimiter := E'\t');
 consume_begin
---------------
 success
(1 row)
```

Additionally, a `parallelism` argument can be specified that will evenly assign a subset of the topic's partitions to each consumer process:

```
=# SELECT pipeline_kafka.consume_begin('kafka_topic', 'topic_stream', parallelism := 4);
 consume_begin
---------------
 success
(1 row)
```


To stop the worker, run:

```
=# SELECT pipeline_kafka.consume_end('kafka_topic', 'topic_stream');
 consume_end
-------------
 success
(1 row)
```

All existing consumers can be started or stopped at once by not passing any arguments to the previous functions:

```
=# SELECT pipeline_kafka.consume_begin();
 consume_begin
---------------
 success
(1 row)

=# SELECT pipeline_kafka.consume_end();
 consume_end
-------------
 success
(1 row)
```

### Producing Messages

You can also produce messages into Kafka topics.

```
=# SELECT pipeline_kafka.produce_message('kafka_topic', 'hello world!');
 produce_message
-----------------
 success
(1 row
```

You can also specify a parition key or an explicit partition to produce the message into.

```
=# SELECT pipeline_kafka.produce_message('kafka_topic', 'hello world!', partition := 2);
 produce_message
-----------------
 success
(1 row
```

`pipeline_kafka` also comes with a trigger function that can be used to produced JSON serialized tuples into Kafka topics. The trigger function can be only be used by `AFTER [INSERT | UPDATE]` and `FOR EACH ROW` triggers.

```
=# CREATE TRIGGER tg
     AFTER UPDATE ON t FOR EACH ROW WHEN (x = 1)
     EXECUTE PROCEDURE pipeline_kafka.emit_tuple('kafka_topic');
CREATE TRIGGER
```

## Documentation

You find the documentation for `pipeline_kafka` [here](http://docs.pipelinedb.com/integrations.html#kafka).
