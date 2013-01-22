import atexit
import subprocess
import time

from pyes import ES
from unittest2 import TestCase

ES_PROCESS = None


def get_global_es():
    global ES_PROCESS
    if ES_PROCESS is None:
        ES_PROCESS = ESProcess()
        ES_PROCESS.start()
        atexit.register(lambda proc: proc.stop(), ES_PROCESS)
    return ES_PROCESS


class ESProcess(object):

    def __init__(self):
        self.process = None
        self.running = None
        self.client = None

    def start(self):
        self.process = subprocess.Popen(
            args=["elasticsearch/bin/elasticsearch", "-f"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        self.running = True
        self.client = ES('localhost:9210')
        self.wait_until_ready()

    def stop(self):
        self.process.terminate()
        self.running = False
        self.process.wait()

    def wait_until_ready(self):
        now = time.time()
        while time.time() - now < 60:
            try:
                health = self.client.cluster_health()
                if (health['status'] == 'green' and
                   health['cluster_name'] == 'monolith'):
                    break
            except Exception:
                pass
        else:
            self.client = None
            raise OSError("Couldn't start elasticsearch")

    def reset(self):
        if self.client is None:
            return
        for index in self.client.get_indices():
            self.client.delete_index(index)


class ESTestHarness(object):

    def setup_es(self):
        self.es_process = get_global_es()

    def teardown_es(self):
        self.es_process.reset()


class TestESWrite(TestCase, ESTestHarness):

    def setUp(self):
        self.setup_es()

    def tearDown(self):
        self.teardown_es()

    def _make_one(self):
        from aggregator.plugins import es
        options = {'url': 'http://localhost:9210'}
        return es.ESWrite(**options)

    def test_constructor(self):
        plugin = self._make_one()
        self.assertEqual(len(plugin.client.servers), 1)

    def test_call(self):
        plugin = self._make_one()
        data = {'foo': 'bar'}
        result = plugin(data)
        id_ = result['_id']
        res = self.es_process.client.get('monolith_2013-01', 'downloads', id_)
        self.assertEqual(res, data)
