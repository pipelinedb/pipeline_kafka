-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pipeline_kafka" to load this file. \quit

-- Consumers added with pipeline_kafka.consume_begin
CREATE TABLE pipeline_kafka.consumers (
  id          serial  PRIMARY KEY,
  topic       text    NOT NULL,
  relation    text    NOT NULL,
  group_id		text,
  format      text    NOT NULL,
  delimiter   text,
  quote       text,
  escape      text,
  batchsize   integer NOT NULL,
  maxbytes    integer NOT NULL,
  parallelism integer NOT NULL,
  timeout     integer NOT NULL,
  shard_id    integer NOT NULL,
  num_shards  integer NOT NULL,
  UNIQUE (topic, relation, shard_id),
  UNIQUE (group_id)
);

CREATE TABLE pipeline_kafka.offsets (
  consumer_id integer NOT NULL REFERENCES pipeline_kafka.consumers (id),
  partition   integer NOT NULL,
  "offset"    bigint  NOT NULL,
  PRIMARY KEY (consumer_id, partition)
);

CREATE INDEX offsets_customer_idx ON pipeline_kafka.offsets (consumer_id);

-- Brokers added with pipeline_kafka.add_broker
CREATE TABLE pipeline_kafka.brokers (
  host text PRIMARY KEY
);

CREATE FUNCTION pipeline_kafka.consume_begin (
  topic        text,
  relation     text,
  group_id     text    DEFAULT NULL,
  format       text    DEFAULT 'text',
  delimiter    text    DEFAULT E'\t',
  quote        text    DEFAULT NULL,
  escape       text    DEFAULT NULL,
  batchsize    integer DEFAULT 10000,
  maxbytes     integer DEFAULT 32000000, -- 32mb
  parallelism  integer DEFAULT 1,
  timeout      integer DEFAULT 250,
  start_offset bigint  DEFAULT NULL,
  shard_id     integer DEFAULT 0,
  num_shards   integer DEFAULT 1
)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_consume_begin'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipeline_kafka.consume_end (
  topic    text,
  relation text
)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_consume_end'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipeline_kafka.consume_begin()
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_consume_begin_all'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipeline_kafka.consume_end()
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_consume_end_all'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipeline_kafka.produce_message (
  topic     text,
  message   bytea,
  partition integer DEFAULT NULL,
  key       bytea   DEFAULT NULL
)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_produce_msg'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipeline_kafka.emit_tuple()
RETURNS trigger
AS 'MODULE_PATHNAME', 'kafka_emit_tuple'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipeline_kafka.add_broker (
  host text
)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_add_broker'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipeline_kafka.remove_broker (
  host text
)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_remove_broker'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipeline_kafka.consume_begin_stream_partitioned (
  topic        text,
  group_id     text    DEFAULT NULL,
  format       text    DEFAULT 'text',
  delimiter    text    DEFAULT E'\t',
  quote        text    DEFAULT NULL,
  escape       text    DEFAULT NULL,
  batchsize    integer DEFAULT 10000,
  maxbytes     integer DEFAULT 32000000, -- 32mb
  parallelism  integer DEFAULT 1,
  timeout      integer DEFAULT 250,
  start_offset bigint  DEFAULT NULL
)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_consume_begin_stream_partitioned'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipeline_kafka.consume_end_stream_partitioned (
  topic    text
)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_consume_end_stream_partitioned'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipeline_kafka.distributed_consume_begin (
  topic        text,
  relation     text,
  group_id     text    DEFAULT NULL,
  format       text    DEFAULT 'text',
  delimiter    text    DEFAULT E'\t',
  quote        text    DEFAULT NULL,
  escape       text    DEFAULT NULL,
  batchsize    integer DEFAULT 10000,
  maxbytes     integer DEFAULT 32000000, -- 32mb
  parallelism  integer DEFAULT 1,
  timeout      integer DEFAULT 250,
  start_offset bigint  DEFAULT NULL,
  shard_id     integer DEFAULT 0,
  num_shards   integer DEFAULT 1
)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_distributed_consume_begin'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipeline_kafka.topic_watermarks (topic text, partition integer)
RETURNS TABLE (partition integer, low_watermark bigint, high_watermark bigint)
AS 'MODULE_PATHNAME', 'kafka_topic_watermarks'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipeline_kafka.consumer_has_group_lock (consumer_id integer)
RETURNS boolean
AS 'MODULE_PATHNAME', 'kafka_consumer_has_group_lock'
LANGUAGE C IMMUTABLE;

CREATE VIEW pipeline_kafka.consumer_lag AS
SELECT c.id AS consumer_id, c.group_id, c.topic, o.partition, o.offset + 1 AS offset, w.high_watermark,
w.high_watermark - ("offset" + 1) AS lag FROM pipeline_kafka.consumers c
  JOIN pipeline_kafka.offsets o ON c.id = o.consumer_id
  JOIN pipeline_kafka.topic_watermarks(c.topic, o.partition) w ON w.partition = o.partition;

CREATE VIEW pipeline_kafka.active_consumer_lag AS
SELECT c.id AS consumer_id, c.group_id, c.topic, o.partition, o.offset + 1 AS offset, w.high_watermark,
w.high_watermark - ("offset" + 1) AS lag FROM pipeline_kafka.consumers c
  JOIN pipeline_kafka.offsets o ON c.id = o.consumer_id AND pipeline_kafka.consumer_has_group_lock(c.id)
  JOIN pipeline_kafka.topic_watermarks(c.topic, o.partition) w ON w.partition = o.partition;