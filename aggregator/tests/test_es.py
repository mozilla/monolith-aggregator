import atexit
import collections
import datetime
import os
import os.path
import random
import shutil
import subprocess
import tempfile
import time
import uuid

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
        self._prior_templates = self._get_template_names()

    def teardown_es(self):
        self._delete_extra_templates()
        self.es_process.reset()

    def _delete_extra_templates(self):
        current_templates = self._get_template_names()
        for t in current_templates - self._prior_templates:
            self.es_process.client.delete_template(t)

    def _get_template_names(self):
        return set(self.es_process.client.list_templates().keys())


class TestExtendedClient(TestCase, ESTestHarness):

    def setUp(self):
        self.setup_es()

    def tearDown(self):
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
        # make sure an index matching the pattern gets the shard setting
        client.create_index('test_index_1')
        self.assertEqual(client.status('test_index_1')['_shards']['total'], 3)

    def test_delete_template(self):
        client = self._make_one()
        client.create_template('template2', {
            'template': 'test_index',
        })
        client.delete_template('template2')
        self.assertFalse(client.get_template('template2'))

    def test_get_template(self):
        client = self._make_one()
        client.create_template('template3', {
            'template': 'test_index',
        })
        res = client.get_template('template3')
        self.assertEqual(res['template3']['template'], 'test_index')

    def test_list_templates(self):
        client = self._make_one()
        client.create_template('t1', {'template': 'test1'})
        client.create_template('t2', {'template': 'test2'})
        client.create_template('t3', {'template': 'test3'})
        res = client.list_templates()
        self.assertEqual(len(res), 3)
        self.assertEqual(set(res.keys()), set(['t1', 't2', 't3']))

    def test_cluster_state(self):
        client = self._make_one()
        res = client.cluster_state(filter_routing_table=True)
        self.assertTrue('nodes' in res)
        self.assertFalse('routing_table' in res)


class TestESSetup(TestCase, ESTestHarness):

    def setUp(self):
        self.setup_es()

    def tearDown(self):
        self.teardown_es()

    def _make_one(self):
        from aggregator.plugins import es
        client = es.ExtendedClient(self.es_process.address)
        return es.ESSetup(client)

    def test_configure_templates(self):
        setup = self._make_one()
        client = setup.client
        setup.configure_templates()
        # directly use the client, which should pick up the template settings
        client.create_index('time_2013-01')
        client.create_index('totals')
        self.assertEqual(
            client.status('time_2013-01')['_shards']['total'], 2)
        self.assertEqual(
            client.status('totals')['_shards']['total'], 6)
        for i in range(1, 32):
            client.index('time_2013-01', 'downloads', {
                'category': 'daily',
                'date': datetime.datetime(2013, 01, i),
                'count': i % 5,
            })
        app_uuids = []
        for i in range(9, -1, -1):
            app_uuid = uuid.uuid4().hex
            app_uuids.append(app_uuid)
            if i > 0:
                client.index('totals', 'apps', {
                    'downloads': i * 2,
                    'users': i},
                    id=app_uuid)
            else:
                # index one document with a missing value
                client.index('totals', 'apps', {
                    'users': i},
                    id=app_uuid)
        client.refresh()
        # integers should stay as ints, and not be converted to strings
        res = client.search(
            {'facets': {'facet1': {'terms': {'field': 'count'}}},
             'sort': [{"date": {"order": "asc"}}]})
        for ft in [t['term'] for t in res['facets']['facet1']['terms']]:
            self.assertTrue(isinstance(ft, int))
        # and dates should be in their typical ES format
        first = res['hits']['hits'][0]['_source']['date']
        self.assertEqual(first, '2013-01-01T00:00:00')
        # test totals
        res = client.search({'sort': [{'downloads': {'order': 'desc'}}]},
            index='totals', doc_type='apps')
        # all apps should be in the list, those with missing values
        # are sorted last in descending order
        sorted_ids = [h['_id'] for h in res['hits']['hits']]
        self.assertEqual(sorted_ids, list(app_uuids))

    def test_create_index_no_string_analysis(self):
        setup = self._make_one()
        client = setup.client
        setup.configure_templates()
        client.create_index('time_2011-11')
        client.index('time_2011-11', 'test', {'a': 'Foo bar', 'b': 1})
        client.index('time_2011-11', 'test', {'a': 'foo baz', 'b': 2})
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
        client.create_index('foo_2011-11')
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
        data = {
            'category': 'downloads',
            'date': datetime.datetime(2012, 7, 4),
            'foo': 'bar',
            'baz': 2,
        }
        plugin([data])
        es_client.refresh()
        res = es_client.search({'query': {'match_all': {}}})
        source = res['hits']['hits'][0]['_source']
        for field in ('foo', 'baz'):
            self.assertEqual(source[field], data[field])
        self.assertEqual(source['date'], '2012-07-04T00:00:00')

    def test_sum_up_app(self):
        plugin = self._make_one()
        apps = collections.defaultdict(lambda: dict(downloads=0, users=0))

        plugin.sum_up_app({'foo': 1}, apps)
        self.assertEqual(len(apps), 0)

        plugin.sum_up_app({'app_uuid': 1}, apps)
        self.assertEqual(apps[1], {'downloads': 0, 'users': 0})

        apps[2]['downloads'] = 3
        plugin.sum_up_app({'app_uuid': 2, 'downloads_count': 4}, apps)
        self.assertEqual(apps[2], {'downloads': 7, 'users': 0})

        apps[3]['downloads'] = 5
        apps[3]['users'] = 4
        plugin.sum_up_app(
            {'app_uuid': 3, 'downloads_count': 4, 'users_count': 1}, apps)
        self.assertEqual(apps[3], {'downloads': 9, 'users': 5})

    def test_get_app_totals(self):
        plugin = self._make_one()
        client = plugin.client
        client.index('totals', 'apps', {'downloads': 10, 'users': 20}, id='1')
        client.index('totals', 'apps', {'downloads': 40, 'users': 50}, id='2')
        client.refresh('totals')

        res = plugin.get_app_totals(['1', '2'])
        self.assertEqual(res['1']['_source'], {'downloads': 10, 'users': 20})
        self.assertEqual(res['2']['_source'], {'downloads': 40, 'users': 50})

    def test_update_app_totals(self):
        plugin = self._make_one()
        client = plugin.client
        found = {
            '1': {'_source': {'downloads': 10, 'users': 20}, '_version': 0},
            '2': {'_source': {'downloads': 40, 'users': 50}, '_version': 0}
        }
        plugin.update_app_totals({
            '1': {'downloads': 3, 'users': 6},
            '2': {'downloads': 7, 'users': 14},
            '3': {'downloads': 13, 'users': 17},
        }, found)
        res = client.multi_get('totals', 'apps', {'ids': ['1', '2', '3']})
        docs = dict([(d['_id'], d) for d in res['docs']])

        self.assertEqual(docs['1']['_source'], {'downloads': 13, 'users': 26})
        self.assertEqual(docs['2']['_source'], {'downloads': 47, 'users': 64})
        self.assertEqual(docs['3']['_source'], {'downloads': 13, 'users': 17})

    def test_get_concurrent_app_totals_update(self):
        plugin = self._make_one()
        client = plugin.client
        client.index('totals', 'apps', {'downloads': 1, 'users': 1}, id='1')
        client.index('totals', 'apps', {'downloads': 2, 'users': 2}, id='2')
        client.refresh('totals')
        found = plugin.get_app_totals(['1', '2'])
        # introduce updates in between get/update calls
        client.index('totals', 'apps', {'downloads': 5, 'users': 5}, id='1')
        client.refresh('totals')
        plugin.update_app_totals({
            '1': {'downloads': 3, 'users': 6},
            '2': {'downloads': 7, 'users': 14},
        }, found)
        res = client.multi_get('totals', 'apps', {'ids': ['1', '2']})
        docs = dict([(d['_id'], d) for d in res['docs']])
        self.assertEqual(docs['1']['_source'], {'downloads': 8, 'users': 11})
        self.assertEqual(docs['2']['_source'], {'downloads': 9, 'users': 16})

    def test_call_and_update_app_totals(self):
        plugin = self._make_one()
        client = plugin.client
        client.index('totals', 'apps', {'downloads': 1, 'users': 2}, id='1')
        client.index('totals', 'apps', {'downloads': 2, 'users': 3}, id='2')
        data = [
            {'app_uuid': '1', 'downloads_count': 1, 'users_count': 2},
            {'app_uuid': '2', 'downloads_count': 2, 'users_count': 3},
            {'app_uuid': '3', 'downloads_count': 3, 'users_count': 6},
            {'app_uuid': '1', 'downloads_count': 10, 'users_count': 20},
            {'app_uuid': '3', 'downloads_count': 3, 'users_count': 6},
            {'app_uuid': '1', 'downloads_count': 5, 'users_count': 10},
        ]
        plugin(data)

        res = client.multi_get('totals', 'apps', {'ids': ['1', '2', '3']})
        docs = dict([(d['_id'], d) for d in res['docs']])

        self.assertEqual(docs['1']['_source'], {'downloads': 17, 'users': 34})
        self.assertEqual(docs['2']['_source'], {'downloads': 4, 'users': 6})
        self.assertEqual(docs['3']['_source'], {'downloads': 6, 'users': 12})
