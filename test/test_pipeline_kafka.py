from base import kafka, pipeline, clean_db, eventually
import json
import threading
import time


def test_basic(pipeline, kafka, clean_db):
  """
  Produce and consume several topics into several streams and verify that
  all resulting data is correct
  """
  pipeline.create_stream('stream0', x='integer')
  pipeline.create_cv('basic', 'SELECT x, COUNT(*) FROM stream0 GROUP BY x')

  kafka.create_topic('test_basic')
  pipeline.consume_begin('test_basic', 'stream0')

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
  pipeline.create_stream('stream0', x='integer')
  pipeline.create_cv('basic', 'SELECT x, COUNT(*) FROM stream0 GROUP BY x')

  kafka.create_topic('test_consumers', partitions=4)
  pipeline.consume_begin('test_consumers', 'stream0', parallelism=4)

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

  pipeline.consume_begin('test_consumers', 'stream0', parallelism=4)
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
  pipeline.consume_end()


def test_consume_stream_partitioned_safety(pipeline, kafka, clean_db):
  '''
  Produce without a key or non-existent stream key and make sure
  thing work properly
  '''
  pipeline.create_stream('stream0', x='integer')
  pipeline.create_cv('cv', 'SELECT count(*) FROM stream0')

  kafka.create_topic('stream_partitioned_topic_safe')
  pipeline.consume_begin_stream_partitioned('stream_partitioned_topic_safe')


  producer = kafka.get_producer('stream_partitioned_topic_safe')
  for n in range(100):
    producer.produce(str(n), partition_key='')
    producer.produce(str(n), partition_key='invalid')
    producer.produce(str(n), partition_key='stream0')

  def messages_partitioned():
    rows = pipeline.execute('SELECT count FROM cv')
    assert rows and rows[0][0] == 100

  assert eventually(messages_partitioned)
  pipeline.consume_end()


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
      assert tuple([row[0], row[1], row[2]]) == (i, i, i)

    rows = pipeline.execute('SELECT * from tab_cv ORDER BY x')
    assert len(rows) == 100
    for i, row in enumerate(rows):
      assert tuple([row[0], row[1], row[2]]) == (i, i, i)

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
      k, arr_el = row[0], row[1]
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

  # Verify offsets are also stored locally
  rows = pipeline.execute('SELECT * FROM pipeline_kafka.offsets')
  assert rows

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

  # Verify that we can still begin consuming from a specific offset
  pipeline.create_cv('group2', "SELECT x FROM stream0")

  pipeline.consume_end()
  # Skip one row per partition
  pipeline.consume_begin('topic0', 'stream0', group_id='group2', start_offset=1)

  def from_offset():
    rows = pipeline.execute('SELECT count(*) FROM group2')
    assert rows[0][0] == 196

  assert eventually(from_offset)


def test_produce(pipeline, kafka, clean_db):
  """
  Tests pipeline_kafka.emit_tuple and pipeline_kafka.produce_message
  """
  pipeline.create_stream('stream0', payload='json')
  pipeline.create_cv('cv', 'SELECT payload FROM stream0')
  pipeline.create_table('t', x='integer', y='integer')
  pipeline.execute("""CREATE TRIGGER tg AFTER INSERT ON t
    FOR EACH ROW EXECUTE PROCEDURE pipeline_kafka.emit_tuple('topic')
    """)

  kafka.create_topic('topic', partitions=4)
  pipeline.consume_begin('topic', 'stream0')

  for i in range(100):
    pipeline.insert('t', ('x', 'y'), [(i, 2 * i)])

  def messages():
    rows = pipeline.execute('SELECT * FROM cv')
    assert len(rows) == 100

  assert eventually(messages)


def test_lag(pipeline, kafka, clean_db):
  """
  Verify that consumer lag is properly tracked
  """
  kafka.create_topic('lag_topic0', partitions=8)
  kafka.create_topic('lag_topic1', partitions=4)

  pipeline.create_stream('stream0', x='integer')
  pipeline.create_cv('lag0', 'SELECT count(*) FROM stream0')

  pipeline.create_stream('stream1', x='integer')
  pipeline.create_cv('lag1', 'SELECT count(*) FROM stream1')

  pipeline.consume_begin('lag_topic0', 'stream0')
  pipeline.consume_begin('lag_topic1', 'stream1')

  producer = kafka.get_producer('lag_topic0')
  for n in range(100):
    producer.produce(str(n), partition_key=str(n))

  producer = kafka.get_producer('lag_topic1')
  for n in range(100):
    producer.produce(str(n), partition_key=str(n))

  def counts0():
    rows = pipeline.execute('SELECT count FROM lag0')
    assert rows[0][0] == 100

    rows = pipeline.execute('SELECT count FROM lag1')
    assert rows[0][0] == 100

  assert eventually(counts0)

  pipeline.consume_end()

  # Now verify there is no reported lag
  rows = pipeline.execute('SELECT sum(lag) FROM pipeline_kafka.consumer_lag')
  assert rows
  assert rows[0][0] == 0

  # Now only start one consumer back up
  pipeline.consume_begin('lag_topic0', 'stream0')

  producer = kafka.get_producer('lag_topic0')
  for n in range(100):
    producer.produce(str(n), partition_key=str(n))

  producer = kafka.get_producer('lag_topic1')
  for n in range(100):
    producer.produce(str(n), partition_key=str(n))

  def counts1():
    rows = pipeline.execute('SELECT count FROM lag0')
    assert rows[0][0] == 200

    rows = pipeline.execute('SELECT count FROM lag1')
    assert rows[0][0] == 100

  assert eventually(counts1)

  # lag_topic0 should have no lag
  rows = pipeline.execute("SELECT sum(lag) FROM pipeline_kafka.consumer_lag WHERE topic = 'lag_topic0'")
  assert rows
  assert rows[0][0] == 0

  # lag_topic1 should have lag now since we didn't start its consumer
  rows = pipeline.execute("SELECT sum(lag) FROM pipeline_kafka.consumer_lag WHERE topic = 'lag_topic1'")
  assert rows
  assert rows[0][0] == 100

  pipeline.consume_end()


def test_null_offsets(pipeline, kafka, clean_db):
  """
  Verify that offsets are stored as NULL if a consumer hasn't consumed any messages yet
  """
  kafka.create_topic('null_topic', partitions=4)
  pipeline.create_stream('null_stream', x='integer')
  pipeline.create_cv('null0', 'SELECT count(*) FROM null_stream')

  pipeline.consume_begin('null_topic', 'null_stream', group_id='null_offsets')

  # Write to a single partition so that only one partition's offsets are updated
  producer = kafka.get_producer('null_topic')
  producer.produce('1', partition_key='key')

  time.sleep(10)
  pipeline.consume_end()

  rows = pipeline.execute('SELECT * FROM pipeline_kafka.offsets WHERE "offset" IS NULL')
  assert len(rows) == 3
