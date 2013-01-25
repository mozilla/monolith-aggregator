import atexit
import os
import os.path
import random
import shutil
import subprocess
import tempfile
import time

from unittest2 import TestCase

ES_PROCESS = None
# find the top-level repo path and our elasticsearch install in it
HERE = os.path.abspath(os.path.dirname(__file__))
ROOT_DIR = os.path.abspath(os.path.join(HERE, os.pardir, os.pardir))
ES_DIR = os.path.join(ROOT_DIR, 'elasticsearch')


def get_global_es():
    """Get or start a new isolated ElasticSearch process.
    """
    global ES_PROCESS
    if ES_PROCESS is None:
        ES_PROCESS = ESProcess()
        ES_PROCESS.start()
        atexit.register(lambda proc: proc.stop(), ES_PROCESS)
    return ES_PROCESS


class ESProcess(object):
    """Start a new ElasticSearch process, isolated in a temporary
    directory. By default it's configured to listen on localhost and
    a random port between 9201 and 9298. The internal cluster transport
    port is the port number plus 1.
    """

    def __init__(self, host='localhost', port_base=9200):
        self.host = host
        self.port = port_base + random.randint(1, 98)
        self.address = 'http://%s:%s' % (self.host, self.port)
        self.working_path = None
        self.process = None
        self.running = False
        self.client = None

    def start(self):
        """Start a new ES process and wait until it's ready.
        """
        self.working_path = tempfile.mkdtemp()
        bin_path = os.path.join(self.working_path, "bin")
        config_path = os.path.join(self.working_path, "config")
        conf_path = os.path.join(config_path, "elasticsearch.yml")
        log_path = os.path.join(self.working_path, "logs")
        log_conf_path = os.path.join(config_path, "logging.yml")
        data_path = os.path.join(self.working_path, "data")

        # create temporary directory structure
        for path in (bin_path, config_path, log_path, data_path):
            if not os.path.exists(path):
                os.mkdir(path)

        # copy ES startup scripts
        es_bin_dir = os.path.join(ES_DIR, 'bin')
        shutil.copy(os.path.join(es_bin_dir, 'elasticsearch'), bin_path)
        shutil.copy(os.path.join(es_bin_dir, 'elasticsearch.in.sh'), bin_path)

        # write configuration file
        with open(conf_path, "w") as config:
            config.write("""
cluster.name: test
node.name: "test_1"
index.number_of_shards: 1
index.number_of_replicas: 0
http.port: {port}
transport.tcp.port: {tport}
discovery.zen.ping.multicast.enabled: false
path.conf: {config_path}
path.work: {work_path}
path.plugins: {work_path}
path.data: {data_path}
path.logs: {log_path}
""".format(port=self.port, tport=self.port + 1, work_path=self.working_path,
           config_path=config_path, data_path=data_path, log_path=log_path))

        # write log file
        with open(log_conf_path, "w") as config:
            config.write("""
rootLogger: INFO, console, file

logger:
  action: DEBUG

appender:
  console:
    type: console
    layout:
      type: consolePattern
      conversionPattern: "[%d{ISO8601}][%-5p][%-25c] %m%n"

  file:
    type: dailyRollingFile
    file: ${path.logs}/${cluster.name}.log
    datePattern: "'.'yyyy-MM-dd"
    layout:
      type: pattern
      conversionPattern: "[%d{ISO8601}][%-5p][%-25c] %m%n"
""")

        # setup environment, copy from base process
        environ = os.environ.copy()
        # configure explicit ES_INCLUDE, to prevent fallback to
        # system-wide locations like /usr/share, /usr/local/, ...
        environ['ES_INCLUDE'] = os.path.join(bin_path, 'elasticsearch.in.sh')
        lib_dir = os.path.join(ES_DIR, 'lib')
        # let the process find our jar files first
        environ['ES_CLASSPATH'] = ('{dir}/elasticsearch-*:{dir}/*:'
            '{dir}/sigar/*:$ES_CLASSPATH'.format(dir=lib_dir))

        self.process = subprocess.Popen(
            args=[bin_path + "/elasticsearch", "-f",
                  "-Des.config=" + conf_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=environ
        )
        self.running = True
        from aggregator.plugins.es import ExtendedClient
        self.client = ExtendedClient(self.address)
        self.wait_until_ready()

    def stop(self):
        """Stop the ES process and removes the temporary directory.
        """
        self.process.terminate()
        self.running = False
        self.process.wait()
        shutil.rmtree(self.working_path, ignore_errors=True)

    def wait_until_ready(self):
        now = time.time()
        while time.time() - now < 60:
            try:
                # check to see if our process is ready
                health = self.client.health()
                if (health['status'] == 'green' and
                   health['cluster_name'] == 'test'):
                    break
            except Exception:
                # wait a bit before re-trying
                time.sleep(0.5)
        else:
            self.client = None
            raise OSError("Couldn't start elasticsearch")

    def reset(self):
        if self.client is None:
            return
        # cleanup all indices after each test run
        self.client.delete_all_indexes()


class ESTestHarness(object):

    def setup_es(self):
        self.es_process = get_global_es()

    def teardown_es(self):
        self.es_process.reset()


class TestExtendedClient(TestCase, ESTestHarness):

    def setUp(self):
        self.setup_es()
        self.added_templates = set()

    def tearDown(self):
        for t in self.added_templates:
            try:
                self.es_process.client.delete_template(t)
            except Exception:
                pass
        self.teardown_es()

    def _make_one(self):
        from aggregator.plugins import es
        return es.ExtendedClient(self.es_process.address)

    def test_create_template(self):
        client = self._make_one()
        client.create_template('template1', {
            'template': 'test_index_*',
            'settings': {
                'number_of_shards': 3,
            },
        })
        self.added_templates.add('template1')
        # make sure an index matching the pattern gets the shard setting
        client.create_index('test_index_1')
        self.assertEqual(client.status('test_index_1')['_shards']['total'], 3)

    def test_delete_template(self):
        client = self._make_one()
        client.create_template('template2', {
            'template': 'test_index',
        })
        self.added_templates.add('template2')
        client.delete_template('template2')
        self.assertFalse(client.get_template('template2'))

    def test_get_template(self):
        client = self._make_one()
        client.create_template('template3', {
            'template': 'test_index',
        })
        self.added_templates.add('template3')
        res = client.get_template('template3')
        self.assertEqual(res['template3']['template'], 'test_index')


class TestESSetup(TestCase, ESTestHarness):

    def setUp(self):
        self.setup_es()

    def tearDown(self):
        self.teardown_es()

    def _make_one(self):
        from aggregator.plugins import es
        client = es.ExtendedClient(self.es_process.address)
        return es.ESSetup(client)

    def test_create_index(self):
        setup = self._make_one()
        client = setup.client
        setup.create_index('foo_2011-11')
        self.assertEqual(
            client.status('foo_2011-11')['_shards']['successful'], 1)

    def test_create_index_no_string_analysis(self):
        setup = self._make_one()
        client = setup.client
        setup.create_index('foo_2011-11')
        client.index('foo_2011-11', 'test', {'a': 'Foo bar', 'b': 1})
        client.index('foo_2011-11', 'test', {'a': 'foo baz', 'b': 2})
        client.refresh()
        # make sure we get facets for the two exact strings we indexed
        res = client.search(
            {'query': {'match_all': {}},
             'facets': {'facet1': {'terms': {'field': 'a'}}}})
        facet1 = res['facets']['facet1']
        self.assertEqual(set([f['term'] for f in facet1['terms']]),
            set(['Foo bar', 'foo baz']))

    def test_optimize_index(self):
        setup = self._make_one()
        client = setup.client
        setup.create_index('foo_2011-11')
        client.index('foo_2011-11', 'test', {'foo': 1})
        res = client.index('foo_2011-11', 'test', {'foo': 2})
        client.delete('foo_2011-11', 'test', res['_id'])
        setup.optimize_index('foo_2011-11')
        res = client.status('foo_2011-11')['indices']['foo_2011-11']
        # the deleted doc was merged away
        self.assertEqual(res['docs']['deleted_docs'], 0)


class TestESWrite(TestCase, ESTestHarness):

    def setUp(self):
        self.setup_es()

    def tearDown(self):
        self.teardown_es()

    def _make_one(self):
        from aggregator.plugins import es
        options = {'url': self.es_process.address}
        return es.ESWrite(**options)

    def test_constructor(self):
        plugin = self._make_one()
        self.assertEqual(len(plugin.client.servers.live), 1)

    def test_call(self):
        plugin = self._make_one()
        es_client = self.es_process.client
        data = {'foo': 'bar'}
        result = plugin(data)
        id_ = result['_id']
        es_client.refresh('monolith_2013-01')
        res = es_client.get('monolith_2013-01', 'downloads', id_)
        self.assertEqual(res['_source'], data)
