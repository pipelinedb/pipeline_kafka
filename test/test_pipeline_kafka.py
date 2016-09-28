from base import pipeline, clean_db, eventually
import time


def test_basic(pipeline, clean_db):
  """
  Produce and consume several topics into several streams and verify that
  all resulting data is correct
  """
  pipeline.create_stream('stream', x='integer')
  pipeline.create_cv('basic', 'SELECT x, COUNT(*) FROM stream GROUP BY x')

  # Let the topic get created lazily
  pipeline.produce('topic', '0')
  time.sleep(2)

  pipeline.consume_begin('topic', 'stream')

  for n in range(1000):
    pipeline.produce('topic', str(n))

  def messages_consumed():
    rows = pipeline.execute('SELECT sum(count) FROM basic')
    assert rows and rows[0][0] == 1000

    rows = pipeline.execute('SELECT count(*) FROM basic')
    assert rows and rows[0][0] == 1000

  assert eventually(messages_consumed)


def test_consume_stream_partitioned(pipeline, clean_db):
  """
  Verify that messages with a stream name as their partition key
  are properly mapped to streams
  """


def test_consume_text(pipeline, clean_db):
  """
  Interpret consumed messages as text
  """


def test_consume_json(pipeline, clean_db):
  """
  Interpret consumed messages as JSON
  """


def test_offsets(pipeline, clean_db):
  """
  Verify that offsets are properly maintained across consumer restarts
  """


def test_parallelism(pipeline, clean_db):
  """
  Verify that consumers are properly parallelized over topic partitions
  """
