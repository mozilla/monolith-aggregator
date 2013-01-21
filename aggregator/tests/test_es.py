import atexit
import subprocess
import time

from pyes import ES
from unittest2 import TestCase


class ESTestHarness(object):

    @classmethod
    def setup_es(cls):
        cls.process = subprocess.Popen(
            args=["elasticsearch/bin/elasticsearch", "-f"],
        )
        cls._running = True
        atexit.register(lambda harness: harness.teardown_es(), cls)
        cls.es_client = ES('localhost:9210')
        cls.wait_until_ready()

    @classmethod
    def wait_until_ready(cls):
        now = time.time()
        while time.time() - now < 60:
            try:
                health = cls.es_client.cluster_health()
                if (health['status'] == 'green' and
                   health['cluster_name'] == 'monolith'):
                    break
            except Exception:
                pass
        else:
            del cls.es_client
            raise OSError("Couldn't start elasticsearch")

    @classmethod
    def teardown_es(cls):
        if cls._running:
            cls.process.terminate()
            cls._running = False
            cls.process.wait()


class TestES(TestCase, ESTestHarness):

    @classmethod
    def setUpClass(cls):
        cls.setup_es()

    @classmethod
    def tearDownClass(cls):
        cls.teardown_es()

    def test_nothing(self):
        self.es_client.get_indices()
        self.assertTrue(True)
