-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pipeline_kafka" to load this file. \quit

-- Consumers added with pipeline_kafka.consume_begin
CREATE TABLE pipeline_kafka.consumers (
  id          serial  PRIMARY KEY,
  relation    text    NOT NULL,
  topic       text    NOT NULL,
  format      text    NOT NULL,
  delimiter   text,
  quote       text,
  escape      text,
  batchsize   integer NOT NULL,
  parallelism integer NOT NULL,
  timeout     integer NOT NULL,
  UNIQUE (relation, topic)
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
  format       text    DEFAULT 'text',
  delimiter    text    DEFAULT E'\t',
  quote        text    DEFAULT NULL,
  escape       text    DEFAULT NULL,
  batchsize    integer DEFAULT 1000,
  parallelism  integer DEFAULT 1,
  timeout      integer DEFAULT 250,
  start_offset bigint  DEFAULT NULL
)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_consume_begin_tr'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipeline_kafka.consume_end (
  topic    text,
  relation text
)
RETURNS text
AS 'MODULE_PATHNAME', 'kafka_consume_end_tr'
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
