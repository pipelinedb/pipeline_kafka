from base import kafka, pipeline, clean_db, eventually, PipelineDB
from kazoo.client import KazooClient
import json
import subprocess
import threading
import time


def test_basic(pipeline, kafka, clean_db):
  """
  Produce and consume several topics into several streams and verify that
  all resulting data is correct
  """
  pipeline.create_stream('stream', x='integer')
  pipeline.create_cv('basic', 'SELECT x, COUNT(*) FROM stream GROUP BY x')

  kafka.create_topic('test_basic')
  pipeline.consume_begin('test_basic', 'stream')

  producer = kafka.get_producer('test_basic')
  for n in range(1000):
    producer.produce(str(n))

  def messages_consumed():
    rows = pipeline.execute('SELECT sum(count) FROM basic')
    assert rows and rows[0][0] == 1000

    rows = pipeline.execute('SELECT count(*) FROM basic')
    assert rows and rows[0][0] == 1000

  assert eventually(messages_consumed)


def test_consumers(pipeline, kafka, clean_db):
  """
  Verify that offsets are properly maintained when storing them locally across consumer restarts
  """
  pipeline.create_stream('stream', x='integer')
  pipeline.create_cv('basic', 'SELECT x, COUNT(*) FROM stream GROUP BY x')

  kafka.create_topic('test_consumers', partitions=4)
  pipeline.consume_begin('test_consumers', 'stream', parallelism=4)

  producer = kafka.get_producer('test_consumers')
  for n in range(1000):
    producer.produce(str(n))

  def messages_consumed0():
    rows = pipeline.execute('SELECT sum(count) FROM basic')
    assert rows and rows[0][0] == 1000

    rows = pipeline.execute('SELECT count(*) FROM basic')
    assert rows and rows[0][0] == 1000

  assert eventually(messages_consumed0)
  pipeline.consume_end()

  # Now verify offsets
  rows = pipeline.execute('SELECT * FROM pipeline_kafka.offsets ORDER BY partition')
  assert len(rows) == 4

  # Consumer IDs should be the same
  assert all(r[0] == rows[0][0] for r in rows)

  # Partitions
  assert rows[0][1] == 0
  assert rows[1][1] == 1
  assert rows[2][1] == 2
  assert rows[3][1] == 3

  # Offsets should sum to the number of (messages_produced - num_partitions)
  assert sum(r[2] for r in rows) == 996

  # Now produce more message while we're not consuming
  for n in range(1000):
    producer.produce(str(n))

  pipeline.consume_begin('test_consumers', 'stream', parallelism=4)
  time.sleep(2)

  # Verify count
  def messages_consumed1():
    rows = pipeline.execute('SELECT sum(count) FROM basic')
    assert rows and rows[0][0] == 2000

    rows = pipeline.execute('SELECT count(*) FROM basic')
    assert rows and rows[0][0] == 1000

  assert eventually(messages_consumed1)
  pipeline.consume_end()

  rows = pipeline.execute('SELECT * FROM pipeline_kafka.offsets ORDER BY partition')
  assert len(rows) == 4

  # Consumer IDs should be the same
  assert all(r[0] == rows[0][0] for r in rows)

  # Partitions
  assert rows[0][1] == 0
  assert rows[1][1] == 1
  assert rows[2][1] == 2
  assert rows[3][1] == 3

  # Offsets should sum to the number of (messages_produced - num_partitions)
  assert sum(r[2] for r in rows) == 1996


def test_consume_stream_partitioned(pipeline, kafka, clean_db):
  """
  Verify that messages with a stream name as their partition key
  are properly mapped to streams
  """
  for n in range(4):
    pipeline.create_stream('stream%d' % n, x='integer')
    pipeline.create_cv('cv%d' % n, 'SELECT x, COUNT(*) FROM stream%d GROUP BY x' % n)

  kafka.create_topic('stream_partitioned_topic')
  pipeline.consume_begin_stream_partitioned('stream_partitioned_topic')

  def produce(producer, stream):
    for n in range(100):
      producer.produce(str(n), partition_key=stream)

  threads = []
  for n in range(4):
    stream = 'stream%d' % n
    producer = kafka.get_producer('stream_partitioned_topic')
    t = threading.Thread(target=produce, args=(producer, stream))
    t.daemon = True
    t.start()
    threads.append(t)

  map(lambda t: t.join(), threads)

  def messages_partitioned():
    for n in range(4):
      rows = pipeline.execute('SELECT sum(count) FROM cv%d' % n)
      assert rows and rows[0][0] == 100

      rows = pipeline.execute('SELECT count(*) FROM cv%d' % n)
      assert rows and rows[0][0] == 100

  assert eventually(messages_partitioned)


def test_consume_text(pipeline, kafka, clean_db):
  """
  Interpret consumed messages as text
  """
  pipeline.create_stream('comma_stream', x='integer', y='integer', z='integer')
  pipeline.create_cv('comma_cv', 'SELECT x, y, z FROM comma_stream')
  kafka.create_topic('test_consume_text_comma')

  pipeline.create_stream('tab_stream', x='integer', y='integer', z='integer')
  pipeline.create_cv('tab_cv', 'SELECT x, y, z FROM tab_stream')
  kafka.create_topic('test_consume_text_tab')

  pipeline.consume_begin('test_consume_text_comma', 'comma_stream', delimiter=',')
  pipeline.consume_begin('test_consume_text_tab', 'tab_stream', delimiter='\t')

  producer = kafka.get_producer('test_consume_text_comma')
  for n in range(100):
    message = ','.join(map(str, [n, n, n]))
    producer.produce(message)

  producer = kafka.get_producer('test_consume_text_tab')
  for n in range(100):
    message = '\t'.join(map(str, [n, n, n]))
    producer.produce(message)

  def delimited_messages():
    rows = pipeline.execute('SELECT * from comma_cv ORDER BY x')
    assert len(rows) == 100
    for i, row in enumerate(rows):
      assert row == (i, i, i)

    rows = pipeline.execute('SELECT * from tab_cv ORDER BY x')
    assert len(rows) == 100
    for i, row in enumerate(rows):
      assert row == (i, i, i)

  assert eventually(delimited_messages)


def test_consume_json(pipeline, kafka, clean_db):
  """
  Interpret consumed messages as JSON
  """
  pipeline.create_stream('json_stream', payload='json')
  pipeline.create_cv('count_cv', "SELECT (payload->>'x')::integer AS x, COUNT(*) FROM json_stream GROUP BY x")
  pipeline.create_cv('unpack_cv', "SELECT (payload#>>'{nested,k}')::text AS k, json_array_elements_text(payload->'array') FROM json_stream")
  kafka.create_topic('test_consume_json')

  pipeline.consume_begin('test_consume_json', 'json_stream', format='json')
  producer = kafka.get_producer('test_consume_json')

  for n in range(200):
    x = n % 100
    payload = {'x': x, 'nested': {'k': 'v%d' % x}, 'array': [x, x, x]}
    producer.produce(json.dumps(payload))

  def count_cv():
    rows = pipeline.execute('SELECT sum(count) FROM count_cv')
    assert rows[0][0] == 200

    rows = pipeline.execute('SELECT count(*) FROM count_cv')
    assert rows[0][0] == 100

  assert eventually(count_cv)

  def unpack_cv():
    # The JSON array gets unpacked for this CV so there are 3 rows for every message
    rows = pipeline.execute('SELECT count(*) FROM unpack_cv')
    assert rows[0][0] == 600

    rows = pipeline.execute('SELECT count(distinct k) FROM unpack_cv')
    assert rows[0][0] == 100

    rows = pipeline.execute('SELECT * FROM unpack_cv ORDER BY k')
    for row in rows:
      k, arr_el = row
      assert k.replace('v', '') == arr_el

  assert eventually(unpack_cv)


def test_grouped_consumer(pipeline, kafka, clean_db):
  """
  Verify that consumers with a group.id store offsets in Kafka and consume
  partitions correctly.
  """
  pipeline.create_stream('stream0', x='integer')
  pipeline.create_stream('stream1', x='integer')
  pipeline.create_cv('group0', "SELECT x, COUNT(*) FROM stream0 GROUP BY x")
  pipeline.create_cv('group1', "SELECT x, COUNT(*) FROM stream1 GROUP BY x")

  kafka.create_topic('topic0', partitions=4)
  kafka.create_topic('topic1', partitions=4)

  pipeline.consume_begin('topic0', 'stream0', group_id='group0')
  pipeline.consume_begin('topic1', 'stream1', group_id='group1')

  producer0 = kafka.get_producer('topic0')
  producer1 = kafka.get_producer('topic1')

  for n in range(1, 101):
    producer0.produce(str(n))
    producer1.produce(str(n))

  def counts():
    rows = pipeline.execute('SELECT sum(count) FROM group0')
    assert rows[0][0] == 100

    rows = pipeline.execute('SELECT sum(count) FROM group1')
    assert rows[0][0] == 100

    rows = pipeline.execute('SELECT count(*) FROM group0')
    assert rows[0][0] == 100

    rows = pipeline.execute('SELECT count(*) FROM group1')
    assert rows[0][0] == 100

  assert eventually(counts)

  pipeline.consume_end()

  # Verify offsets aren't stored locally
  rows = pipeline.execute('SELECT * FROM pipeline_kafka.offsets')
  assert not rows

  # Now produce some more
  for n in range(1, 101):
    producer0.produce(str(-n))
    producer1.produce(str(-n))

  pipeline.consume_begin('topic0', 'stream0', group_id='group0')
  pipeline.consume_begin('topic1', 'stream1', group_id='group1')

  # And verify that only the second batch of messages was read
  def counts_after_restart():
    rows = pipeline.execute('SELECT sum(x) FROM group0')
    assert rows[0][0] == 0

    rows = pipeline.execute('SELECT sum(x) FROM group1')
    assert rows[0][0] == 0

    rows = pipeline.execute('SELECT sum(count) FROM group0')
    assert rows[0][0] == 200

    rows = pipeline.execute('SELECT sum(count) FROM group1')
    assert rows[0][0] == 200

    rows = pipeline.execute('SELECT count(*) FROM group0')
    assert rows[0][0] == 200

    rows = pipeline.execute('SELECT count(*) FROM group1')
    assert rows[0][0] == 200

  assert eventually(counts_after_restart)

  # Verify that offsets still aren't stored locally
  rows = pipeline.execute('SELECT * FROM pipeline_kafka.offsets')
  assert not rows

  # Verify that we can still begin consuming from a specific offset
  pipeline.create_cv('group2', "SELECT x FROM stream0")

  pipeline.consume_end()
  # Skip one row per partition
  pipeline.consume_begin('topic0', 'stream0', group_id='group2', start_offset=1)

  def from_offset():
    rows = pipeline.execute('SELECT count(*) FROM group2')
    assert rows[0][0] == 196

  assert eventually(from_offset)

  # Verify that offsets still aren't stored locally
  rows = pipeline.execute('SELECT * FROM pipeline_kafka.offsets')
  assert not rows


def test_grouped_consumer_failover(kafka):
  """
  Verify that if one instance of a grouped consumer is stopped or fails,
  exactly one becomes the new active consumer.
  """
  kafka.create_topic('topic0')
  kafka.create_topic('topic1')

  pdb0 = PipelineDB()
  pdb1 = PipelineDB()

  pdb0.run()
  pdb1.run()

  pdb0.execute("SELECT pipeline_kafka.add_broker('localhost:9092')")
  pdb0.execute("SELECT pipeline_kafka.add_broker('localhost:8092')")
  pdb1.execute("SELECT pipeline_kafka.add_broker('localhost:9092')")
  pdb1.execute("SELECT pipeline_kafka.add_broker('localhost:8092')")

  pdb0.create_stream('stream0', x='integer')
  pdb0.create_stream('stream1', x='integer')
  pdb0.create_cv('group0', "SELECT x, COUNT(*) FROM stream0 GROUP BY x")
  pdb0.create_cv('group1', "SELECT x, COUNT(*) FROM stream1 GROUP BY x")

  pdb1.create_stream('stream0', x='integer')
  pdb1.create_stream('stream1', x='integer')
  pdb1.create_cv('group0', "SELECT x, COUNT(*) FROM stream0 GROUP BY x")
  pdb1.create_cv('group1', "SELECT x, COUNT(*) FROM stream1 GROUP BY x")

  pdb0.consume_begin('topic0', 'stream0', group_id='group0')
  pdb0.consume_begin('topic1', 'stream1', group_id='group1')

  # Let pdb0 take the lock
  time.sleep(5)

  pdb1.consume_begin('topic0', 'stream0', group_id='group0')
  pdb1.consume_begin('topic1', 'stream1', group_id='group1')

  producer0 = kafka.get_producer('topic0')
  producer1 = kafka.get_producer('topic1')

  for n in range(0, 100):
    producer0.produce(str(n))
    producer1.produce(str(n))

  def counts0():
    rows = pdb0.execute('SELECT sum(count) FROM group0')
    assert rows[0][0] == 100

    rows = pdb0.execute('SELECT sum(count) FROM group1')
    assert rows[0][0] == 100

    rows = pdb1.execute('SELECT sum(count) FROM group0')
    assert rows[0][0] is None

    rows = pdb1.execute('SELECT sum(count) FROM group1')
    assert rows[0][0] is None

  assert eventually(counts0)

  # Stop one consumer so the other one takes the lock
  pdb0.consume_end()
  time.sleep(1)

  pdb0.consume_begin('topic0', 'stream0', group_id='group0')
  pdb0.consume_begin('topic1', 'stream1', group_id='group1')

  for n in range(0, 100):
    producer0.produce(str(n))
    producer1.produce(str(n))

  def counts1():
    rows = pdb0.execute('SELECT sum(count) FROM group0')
    assert rows[0][0] == 100

    rows = pdb0.execute('SELECT sum(count) FROM group1')
    assert rows[0][0] == 100

    # Only this DB should have consumed messages this time
    rows = pdb1.execute('SELECT sum(count) FROM group0')
    assert rows[0][0] == 100

    rows = pdb1.execute('SELECT sum(count) FROM group1')
    assert rows[0][0] == 100

  assert eventually(counts1)

  pdb1.consume_end()
  pdb0.consume_end()

  pdb0.destroy()
  pdb1.destroy()


def test_grouped_consumer_session_loss(kafka):
  """
  Verify that even if a consumer group initially acquires the group lock,
  it must continuously verify that it still has the lock before actually
  consuming messages.
  """
  kafka.create_topic('topic')

  pdb0 = PipelineDB()
  pdb1 = PipelineDB()

  pdb0.run()
  pdb1.run()

  pdb0.execute("SELECT pipeline_kafka.add_broker('localhost:9092')")
  pdb0.execute("SELECT pipeline_kafka.add_broker('localhost:8092')")
  pdb1.execute("SELECT pipeline_kafka.add_broker('localhost:9092')")
  pdb1.execute("SELECT pipeline_kafka.add_broker('localhost:8092')")

  pdb0.create_stream('stream', x='integer')
  pdb0.create_cv('count', "SELECT x, COUNT(*) FROM stream GROUP BY x")

  pdb1.create_stream('stream', x='integer')
  pdb1.create_cv('count', "SELECT x, COUNT(*) FROM stream GROUP BY x")

  pdb0.consume_begin('topic', 'stream', group_id='session')

  # Let pdb0 take the lock
  time.sleep(5)

  pdb1.consume_begin('topic', 'stream', group_id='session')

  producer = kafka.get_producer('topic')

  for n in range(0, 100):
    producer.produce(str(n))

  def before_session_loss():
    rows = pdb0.execute('SELECT sum(count) FROM count')
    assert rows[0][0] == 100

    rows = pdb1.execute('SELECT sum(count) FROM count')
    assert rows[0][0] is None

  assert eventually(before_session_loss)

  # Delete pdb0's lock znode to simulate a session loss
  zk = KazooClient(hosts='localhost:2181')
  zk.start()
  children = zk.get_children('/pipeline_kafka/session')
  lock = sorted(children)[0]
  zk.delete('/pipeline_kafka/session/%s' % lock)
  zk.stop()
  time.sleep(20)

  # pdb1 was next in line to hold the lock, so it should consume
  # the remainder of the messages
  for n in range(0, 100):
    producer.produce(str(n))

  def after_session_loss():
    rows = pdb0.execute('SELECT sum(count) FROM count')
    assert rows[0][0] == 100

    rows = pdb1.execute('SELECT sum(count) FROM count')
    assert rows[0][0] == 100

  assert eventually(after_session_loss)

  pdb1.consume_end()
  pdb0.consume_end()
  time.sleep(10)

  pdb0.destroy()
  pdb1.destroy()
