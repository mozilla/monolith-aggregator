import os
import datetime
import tempfile

from unittest2 import TestCase

from monolith.aggregator.db import Database, Record


class TestDatabase(TestCase):

    def setUp(self):
        fd, self.filename = tempfile.mkstemp()
        os.close(fd)
        self.sqluri = 'sqlite:///%s' % self.filename
        self.db = Database(database=self.sqluri)
        self._today = datetime.date.today()
        self._last_week = self._today - datetime.timedelta(weeks=1)
        self._yesterday = self._today - datetime.timedelta(days=1)

    def tearDown(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)

    def test_record_creation_no_defaults(self):
        self.assertRaises(KeyError, self.db.inject,
                          [('test', dict(_type='foo', key='value'))])
        self.assertRaises(KeyError, self.db.inject,
                          [('test', dict(key='value', _date=self._today))])

    def test_record_creation_use_specified_date(self):
        self.db.inject([
            ('test', dict(_type='foo', key='value', _date=self._last_week)),
        ])
        query = self.db.session.query(Record)
        results = query.all()
        self.assertEquals(results[0].date, self._last_week)

    def test_put_item_with_date(self):
        self.db.inject([
            ('test', dict(_type='foo', key='value', _date=self._yesterday)),
        ])
        results = self.db.session.query(Record).all()
        self.assertEquals(len(results), 1)

    def test_clear(self):
        self.db.inject([
            ('s1', dict(_type='foo', key='1', _date=self._yesterday)),
            ('s1', dict(_type='bar', key='2', _date=self._today)),
            ('s1', dict(_type='bar', key='3', _date=self._today)),
            ('s2', dict(_type='baz', key='4', _date=self._today)),
            ('s2', dict(_type='baz', key='5', _date=self._today)),
        ])
        removed = self.db.clear(self._yesterday, self._yesterday, ['s2'])
        self.assertEqual(removed, 0)
        removed = self.db.clear(self._today, self._today, ['s1'])
        self.assertEqual(removed, 2)
        removed = self.db.clear(self._yesterday, self._today, ['s1', 's2'])
        self.assertEqual(removed, 3)
