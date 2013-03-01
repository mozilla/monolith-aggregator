from unittest2 import TestCase
from datetime import date, timedelta

from monolith.aggregator.util import all_, word2daterange


class TestUtils(TestCase):

    def test_all(self):
        self.assertFalse(all_([1, 2, 3], 1))
        self.assertTrue(all_((1, 1, 1), 1))

    def test_word2daterange(self):

        def _d(days):
            return timedelta(days=days)

        def _diff(d1, d2):
            diff = d1 - d2
            return diff.days

        # don't run those tests a millisecond before midnight!
        #
        self.assertRaises(NotImplementedError, word2daterange, 'bleh')

        now = date.today()
        today, __ = word2daterange('today')
        self.assertTrue(today, now)

        yesterday, __ = word2daterange('yesterday')
        self.assertTrue(today - yesterday, _d(1))

        first, last = word2daterange('last-week')
        self.assertTrue(_diff(now, last) >= 0)
        self.assertTrue(_diff(now, first) >= 7)
        self.assertTrue(_diff(last, first) == 7)
