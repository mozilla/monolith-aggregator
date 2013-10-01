import unittest
import os
import time

from monolith.aggregator.plugins.ganalytics import BaseGoogleAnalytics


class FakeClient(object):
    calls = 0

    def data(self, **options):
        return self
    get = ga = data

    def execute(self):
        self.calls += 1


class TestGoogleAnalytics(unittest.TestCase):

    def test_rate_limiter(self):
        options = {
            'oauth_token': os.path.join(os.path.dirname(__file__),
                                        'auth.json'),
            'profile_id': 'XXX',
            'metrics': 'XXX',
            'rate_limit': 6,
            'rate_span': 0.2,
        }
        ga = BaseGoogleAnalytics(**options)
        ga.client = FakeClient()

        now = time.time()
        while time.time() - now < 0.5:
            ga._rate_limited_get()

        # let's see how many call we got through
        self.assertTrue(ga.client.calls < 30, ga.client.calls)
