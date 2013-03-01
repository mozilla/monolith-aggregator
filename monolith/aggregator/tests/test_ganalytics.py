import unittest
import os
import time

from monolith.aggregator.plugins.ganalytics import GoogleAnalytics


class FakeClient(object):
    calls = 0

    def data(self, **options):
        return self
    get = ga = data

    def execute(self):
        self.calls += 1


class TestGoogleAnalytics(unittest.TestCase):

    def test_rate_limiter(self):

        options = {'oauth_token': os.path.join(os.path.dirname(__file__),
                                               'auth.json'),
                   'profile_id': 'XXX',
                   'metrics': 'XXX',
                   }

        ga = GoogleAnalytics(**options)
        ga.client = FakeClient()

        # calling continuously for 5 seconds.
        now = time.time()

        while time.time() - now < 3.:
            ga._rate_limited_get()

        # let's see how many call we got through
        # 3 seconds => 30 call max !
        self.assertTrue(ga.client.calls < 30, ga.client.calls)
