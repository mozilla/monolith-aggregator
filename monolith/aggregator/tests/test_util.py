from unittest2 import TestCase
from datetime import date, datetime, timedelta

from monolith.aggregator.util import word2daterange, date_range
from monolith.aggregator.util import json_loads, json_dumps


class TestUtils(TestCase):

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

    def test_date_range(self):
        now = date.today()
        yesterday = now - timedelta(days=1)
        last_week = now - timedelta(days=7)

        res = list(date_range(last_week, now))
        self.assertEqual(res[0], last_week)
        self.assertEqual(res[-1], now)
        self.assertEqual(len(res), 8)

        self.assertEqual(list(date_range(yesterday, now)), [yesterday, now])
        self.assertEqual(list(date_range(yesterday, yesterday)), [yesterday])


class TestJSON(TestCase):

    def test_dumps_date(self):
        data = json_dumps({'date': date(2012, 3, 15)})
        self.assertEqual(data, '{"date": "2012-03-15"}')

    def test_dumps_datetime(self):
        data = json_dumps({'date': datetime(2012, 3, 15, 13, 55, 10)})
        self.assertEqual(data, '{"date": "2012-03-15T13:55:10.000000"}')

    def test_dumps_error(self):
        self.assertRaises(TypeError, json_dumps, {'foo': object()})

    def test_loads_date(self):
        data = json_loads('{"date": "2012-03-15"}')
        self.assertEqual(data, {'date': '2012-03-15'})
