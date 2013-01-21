import atexit
import subprocess
import time

from pyes import ES
from unittest2 import TestCase


class ESTestHarness(object):

    def setup_es(self):
        self.process = subprocess.Popen(
            args=["elasticsearch/bin/elasticsearch", "-f"],
        )
        self._running = True
        atexit.register(lambda harness: harness.teardown_es(), self)
        self.es_client = ES('localhost:9210')
        self.wait_until_ready()

    def wait_until_ready(self):
        now = time.time()
        while time.time() - now < 60:
            try:
                health = self.es_client.cluster_health()
                if (health['status'] == 'green' and
                   health['cluster_name'] == 'monolith'):
                    break
            except Exception:
                pass
        else:
            del self.es_client
            raise OSError("Couldn't start elasticsearch")

    def teardown_es(self):
        if self._running:
            self.process.terminate()
            self._running = False
            self.process.wait()


class TestES(TestCase, ESTestHarness):

    def setUp(self):
        self.setup_es()

    def tearDown(self):
        self.teardown_es()

    def test_nothing(self):
        self.es_client.get_indices()
        self.assertTrue(True)
