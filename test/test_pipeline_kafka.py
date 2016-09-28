from base import pipeline, kafka, clean_db


def test_basic(pipeline, kafka, clean_db):
  """
  Consume several topics into several streams and verify that
  all resulting data is correct
  """


def test_consume_stream_partitioned(pipeline, kafka, clean_db):
  """
  Verify that messages with a stream name as their partition key
  are properly mapped to streams
  """


def test_consume_text(pipeline, kafka, clean_db):
  """
  Interpret consumed messages as text
  """


def test_consume_json(pipeline, kafka, clean_db):
  """
  Interpret consumed messages as JSON
  """


def test_offsets(pipeline, kafka, clean_db):
  """
  Verify that offsets are properly maintained across consumer restarts
  """


def test_parallelism(pipeline, kafka, clean_db):
  """
  Verify that consumers are properly parallelized over topic partitions
  """
