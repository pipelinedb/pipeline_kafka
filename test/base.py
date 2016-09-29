from pykafka import KafkaClient
from pykafka.exceptions import LeaderNotAvailable
import atexit
import getpass
import os
import psycopg2
import pytest
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time


BOOTSTRAPPED_BASE = './.pdbbase'
INSTALL_FORMAT = './.pdb-%d'
CONNSTR_TEMPLATE = 'postgres://%s@localhost:%d/pipeline'
PG_CONFIG = os.environ.get('PG_CONFIG', 'pg_config')
KAFKA_URL = 'http://apache.claz.org/kafka/0.8.2.2/kafka_2.9.1-0.8.2.2.tgz'


class PipelineDB(object):
  def __init__(self, data_dir='data', port=5432, sync_insert=False):
    """
    Bootstraps the PipelineDB instance. Note that instead of incurring the
    cost of actually bootstrapping each instance we copy a clean,
    bootstrapped directory to our own directory, creating it once for
    other tests to use if it doesn't already exist.
    """
    self.sync_insert = sync_insert
    self.port = port
    do_initdb = not os.path.exists(BOOTSTRAPPED_BASE)

    # Get the bin dir of our installation
    out, err = subprocess.Popen([PG_CONFIG, '--bindir'],
                                stdout=subprocess.PIPE).communicate()
    self.bin_dir = out.strip()
    self.server = os.path.join(self.bin_dir, 'pipelinedb')
    self.ctl = os.path.join(self.bin_dir, 'pipeline-ctl')

    if do_initdb:
      out, err = subprocess.Popen([os.path.join(self.bin_dir, 'pipeline-init'),
                                   '-D', BOOTSTRAPPED_BASE]).communicate()

    # Copy the bootstrapped install to our working directory
    self.data_dir = os.path.join(self.tmp_dir, data_dir)
    shutil.copytree(BOOTSTRAPPED_BASE, self.data_dir)
    self.running = False

  def run(self, params=None):
    """
    Runs a test instance of PipelineDB within our temporary directory on
    a free port
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    _, port = sock.getsockname()
    self.port = port

    # Let's hope someone doesn't take our port before we try to bind
    # PipelineDB to it
    sock.close()

    default_params = {
      'stream_insert_level': 'sync_commit',
      'continuous_query_num_combiners': 2,
      'continuous_query_num_workers': 2,
      'anonymous_update_checks': 'off',
      'continuous_query_max_wait': 5,
      'shared_preload_libraries': 'pipeline_kafka'
    }

    cmd = [self.server, '-D', self.data_dir, '-p', str(self.port)]

    default_params.update(params or {})
    for key, value in default_params.iteritems():
      cmd.extend(['-c', '%s=%s' % (key, value)])

    self.proc = subprocess.Popen(cmd, stderr=subprocess.PIPE)

    # Wait for PipelineDB to start up
    while True:
      line = self.proc.stderr.readline()
      sys.stderr.write(line)
      if ('database system is ready to accept connections' in line or
        'continuous query process "worker0 [pipeline]" running with pid' in line):
        break
      elif ('database system is shut down' in line or
        (line == '' and self.proc.poll() != None)):
        raise Exception('Failed to start up PipelineDB')

    # Add log tailer
    def run():
      while True:
        if not self.proc:
          break
        line = self.proc.stderr.readline()
        if line == '' and self.proc and self.proc.poll() != None:
          return
        sys.stderr.write(line)
    threading.Thread(target=run).start()

    connstr = CONNSTR_TEMPLATE % (getpass.getuser(), self.port)

    # Wait to connect to PipelineDB
    for i in xrange(10):
      try:
        self.conn = psycopg2.connect(connstr)
        break
      except OperationalError:
        time.sleep(0.1)
    else:
      raise Exception('Failed to connect to PipelineDB')

    cur = self.conn.cursor()
    cur.execute('CREATE EXTENSION IF NOT EXISTS pipeline_kafka')
    self.conn.commit()

    # Wait for bgworkers to start
    for i in xrange(10):
      try:
        out = subprocess.check_output('ps aux | grep "\[pipeline\]" | grep -e "worker[0-9]" -e "combiner[0-9]"',
                        shell=True).split('\n')
      except subprocess.CalledProcessError:
        out = []

      # Pick out PIDs that are greater than the PID of the postmaster we fired above.
      # This way any running PipelineDB instances are ignored.
      out = filter(lambda s: s.strip(), out)
      out = map(lambda s: int(s.split()[1]), out)
      out = filter(lambda p: p > self.proc.pid, out)

      if len(out) == (default_params['continuous_query_num_workers'] +
              default_params['continuous_query_num_combiners']):
        break
      time.sleep(0.5)
    else:
      raise Exception('Background workers failed to start up')

    time.sleep(0.2)

  def stop(self):
    """
    Stops the PipelineDB instance
    """
    if self.conn:
      self.conn.close()
    if self.proc:
      self.proc.send_signal(signal.SIGINT)
      self.proc.wait()
      self.proc = None

  def destroy(self):
    """
    Cleans up resources used by this PipelineDB instance
    """
    self.stop()
    shutil.rmtree(self.tmp_dir)

  @property
  def tmp_dir(self):
    """
    Returns the temporary directory that this instance is based within,
    finding a new one of it hasn't already
    """
    if hasattr(self, '_tmp_dir'):
      return self._tmp_dir

    # Get all the indexed install dirs so we can created a new one with
    # highest index + 1. Install dirs are of the form: ./.pdb-<n>.
    index = max([int(l.split('-')[1]) for l in os.listdir('.')
           if l.startswith('.pdb-')] or [-1]) + 1
    self._tmp_dir = INSTALL_FORMAT % index
    return self._tmp_dir

  def drop_all(self):
    """
    Drop all continuous queries and streams
    """
    for transform in self.execute('SELECT schema, name FROM pipeline_transforms()'):
      self.execute('DROP CONTINUOUS TRANSFORM %s.%s CASCADE' % transform)
    for view in self.execute('SELECT schema, name FROM pipeline_views()'):
      self.execute('DROP CONTINUOUS VIEW %s.%s CASCADE' % view)
    for stream in self.execute('SELECT schema, name FROM pipeline_streams()'):
      self.execute('DROP STREAM %s.%s CASCADE' % stream)

  def create_cv(self, name, stmt, **kw):
    """
    Create a continuous view
    """
    opts = ', '.join(['%s=%r' % (k, v) for k, v in kw.items()])

    if kw:
      result = self.execute('CREATE CONTINUOUS VIEW %s WITH (%s) AS %s' % (name, opts, stmt))
    else:
      result = self.execute('CREATE CONTINUOUS VIEW %s AS %s' % (name, stmt))
    return result

  def create_ct(self, name, stmt, trigfn):
    """
    Create a continuous transform
    """
    result = self.execute(
      'CREATE CONTINUOUS TRANSFORM %s AS %s THEN EXECUTE PROCEDURE %s' %
      (name, stmt, trigfn))
    return result

  def create_table(self, name, **cols):
    """
    Create a table
    """
    cols = ', '.join(['%s %s' % (k, v) for k, v in cols.iteritems()])
    self.execute('CREATE TABLE %s (%s)' % (name, cols))

  def create_stream(self, name, **cols):
    """
    Create a stream
    """
    cols = ', '.join(['%s %s' % (k, v) for k, v in cols.iteritems()])
    self.execute('CREATE STREAM %s (%s)' % (name, cols))

  def drop_table(self, name):
    """
    Drop a table
    """
    self.execute('DROP TABLE %s' % name)

  def drop_stream(self, name):
    """
    Drop a stream
    """
    self.execute('DROP STREAM %s' % name)

  def drop_cv(self, name):
    """
    Drop a continuous view
    """
    return self.execute('DROP CONTINUOUS VIEW %s' % name)

  def execute(self, stmt):
    """
    Execute a raw SQL statement
    """
    if not self.conn:
      return None
    cur = self.conn.cursor()
    cur.execute(stmt)

    if cur.rowcount < 0:
      return []

    return cur.fetchall()

  def insert(self, target, desc, rows):
    """
    Insert a batch of rows
    """
    header = ', '.join(desc)
    values = []
    for r in rows:
      if len(r) == 1:
        values.append('(%s)' % r[0])
      else:
        values.append(str(r))
    values = ', '.join(values)
    values = values.replace('None', 'null')
    return self.execute('INSERT INTO %s (%s) VALUES %s' % (target, header, values))

  def insert_batches(self, target, desc, rows, batch_size):
    """
    Insert a batch of rows, spreading them across randomly selected nodes
    """
    batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

    for i, batch in enumerate(batches):
      self.insert(target, desc, batch)
      time.sleep(0.5)

  def begin(self):
    """
    Begin a transaction
    """
    return self.execute('BEGIN')

  def commit(self):
    """
    Commit a transaction
    """
    return self.execute('COMMIT')

  def get_conn_string(self):
    """
    Get the connection string for this database
    """
    connstr = (CONNSTR_TEMPLATE % (getpass.getuser(), self.port))
    return connstr

  def get_bin_dir(self):
    return self.bin_dir

  def produce(self, topic, message):
    """
    Produce the message(s) into the given topic
    """
    q = """
    SELECT pipeline_kafka.produce_message('%s', '%s')
    """ % (topic, message)
    self.execute(q)

  def consume_begin(self, topic, stream, format='text', delimiter='\t',
                    quote=None, escape=None, batchsize=1000,
                    maxbytes=32000000, parallelism=1, start_offset=None):
    """
    Begin consuming with the given parameters
    """
    params = {
      'delimiter': delimiter,
      'quote': quote,
      'format': format,
      'escape': escape,
      'batchsize': batchsize,
      'maxbytes': maxbytes,
      'parallelism': parallelism,
      'start_offset': start_offset,
    }
    args = [repr(topic), repr(stream)]
    args.extend(['%s := %s' % (k, v and repr(v).replace("'\\", "E'\\") or 'NULL') for k, v in params.items()])
    args = ', '.join(args)

    q = 'SELECT pipeline_kafka.consume_begin(%s)' % args
    self.execute(q)
    self.commit()
    time.sleep(1)

  def consume_end(self):
    """
    Stop all consumers
    """
    self.execute('SELECT pipeline_kafka.consume_end()')
    time.sleep(1)

  def consume_begin_stream_partitioned(self, topic, format='text', delimiter='\t',
                    quote=None, escape=None, batchsize=1000,
                    maxbytes=32000000, parallelism=1, start_offset=None):
    """
    Begin consuming a stream-partitioned topic with the given parameters
    """
    params = {
      'delimiter': delimiter,
      'quote': quote,
      'format': format,
      'escape': escape,
      'batchsize': batchsize,
      'maxbytes': maxbytes,
      'parallelism': parallelism,
      'start_offset': start_offset
    }
    args = [repr(topic)]
    args.extend(['%s := %s' % (k, v and repr(v).replace("'\\", "E'\\") or 'NULL') for k, v in params.items()])
    args = ', '.join(args)

    q = 'SELECT pipeline_kafka.consume_begin_stream_partitioned(%s)' % args
    self.execute(q)
    self.commit()
    time.sleep(1)


class KafkaCluster(object):

  def __init__(self, bin_dir='/opt/kafka_2.10-0.8.2.2/bin'):
    self.bin_dir = bin_dir
    self.topics = []
    self.brokers = ['localhost:9092', 'localhost:8092']
    self.client = KafkaClient(hosts=','.join(self.brokers))

    # We need to dynamically create and delete topics, which is surprisingly
    # difficult to do with the available clients. So we just use the Kafka binaries
    # directly for this :(
    if not os.path.exists('.kafka'):
      p = subprocess.Popen(['wget', KAFKA_URL])
      stdout, stderr = p.communicate()
      p = subprocess.Popen('tar -xf kafka* && rm *.tgz && mv kafka* .kafka', shell=True)
      stdout, stderr = p.communicate()

    self.bin_dir = os.path.abspath('./.kafka/bin')

  def delete_topic(self, name):
    cmd = [os.path.join(self.bin_dir, 'kafka-topics.sh'),
           '--delete', '--topic', name,
           '--zookeeper', 'localhost:2181']
    p = subprocess.Popen(cmd)
    stdout, stderr = p.communicate()

  def create_topic(self, name, partitions=1, replication_factor=1):
    self.delete_topic(name)
    cmd = [os.path.join(self.bin_dir, 'kafka-topics.sh'),
           '--create', '--topic', name,
           '--config', 'min.insync.replicas=%d' % replication_factor,
           '--zookeeper', 'localhost:2181',
           '--partitions', str(partitions),
           '--replication-factor', str(replication_factor)]
    p = subprocess.Popen(cmd)
    stdout, stderr = p.communicate()
    self.topics.append(name)

  def delete_topics(self):
    for topic in self.topics:
      self.delete_topic(topic)

  def get_producer(self, topic, sync=False):
    start = time.time()
    while time.time() - start < 60:
      try:
        self.client = KafkaClient(hosts=','.join(self.brokers))
        break
      except LeaderNotAvailable:
        time.sleep(1)
    topic = self.client.topics[topic]
    producer = topic.get_producer(sync=sync)
    atexit.register(lambda p: p.stop() if p._running else None, producer)

    return producer


@pytest.fixture
def clean_db(request):
  """
  Called for every test so each test gets a clean db
  """
  pdb = request.module.pipeline
  request.addfinalizer(pdb.drop_all)
  request.addfinalizer(pdb.consume_end)

  pdb.execute('DROP EXTENSION pipeline_kafka')
  pdb.execute('CREATE EXTENSION pipeline_kafka')
  pdb.execute("SELECT pipeline_kafka.add_broker('localhost:9092')")
  pdb.execute("SELECT pipeline_kafka.add_broker('localhost:8092')")


@pytest.fixture(scope='module')
def pipeline(request):
  """
  Builds and returns a running PipelineDB instance based out of a test
  directory within the current directory. This is called once per test
  module, so it's shared between tests even though underlying databases
  are recreated for each test.
  """
  pdb = PipelineDB()
  request.addfinalizer(pdb.destroy)

  # Attach it to the module so we can access it with test-scoped fixtures
  request.module.pipeline = pdb
  pdb.run()

  return pdb


@pytest.fixture(scope='module')
def kafka(request):
  """
  Attaches to the running Dockerized Kafka cluster and supports
  creating topics, which are all deleted when this fixture goes out of scope.
  """
  kafka = KafkaCluster()
  request.addfinalizer(kafka.delete_topics)

  return kafka


def eventually(fn, timeout=120, interval=0.01):
  """
  Keep calling fn until it returns true or a timeout is reached, sleeping
  for the given interval after each failed attempt
  """
  elapsed = 0
  start = time.time()
  while elapsed < timeout:
    try:
      fn()
      return True
    except:
      if elapsed >= timeout:
        raise
      time.sleep(interval)
    elapsed = time.time() - start

  return False
