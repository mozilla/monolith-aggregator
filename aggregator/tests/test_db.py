import os
import datetime
import tempfile

from sqlalchemy import create_engine
from unittest2 import TestCase

from aggregator.db import Database, Record


class TestDatabase(TestCase):

    def setUp(self):
        fd, self.filename = tempfile.mkstemp()
        os.close(fd)
        self.sqluri = 'sqlite:///%s' % self.filename
        self.engine = create_engine(self.sqluri)
        self.db = Database(self.engine)
        self._today = datetime.date.today()
        self._last_week = self._today - datetime.timedelta(weeks=1)
        self._yesterday = self._today - datetime.timedelta(days=1)

    def tearDown(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)

    def test_record_creation_no_defaults(self):
        self.assertRaises(KeyError, self.db.put,
                          [('test', dict(_type='foo', key='value'))])
        self.assertRaises(KeyError, self.db.put,
                          [('test', dict(key='value', _date=self._today))])

    def test_record_creation_use_specified_date(self):
        self.db.put([
            ('test', dict(_type='foo', key='value', _date=self._last_week)),
        ])
        query = self.db.session.query(Record)
        results = query.all()
        self.assertEquals(results[0].date, self._last_week)

    def test_record_retrieval_need_a_filter(self):
        self.assertRaises(ValueError, self.db.get)

    def test_filter_end_date(self):
        self.db.put([
            ('test', dict(_type='foo', key='value', _date=self._last_week)),
            ('test', dict(_type='foo', key='value', _date=self._today)),
        ])
        results = self.db.get(end_date=self._yesterday).all()
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0].date, self._last_week)

    def test_filter_start_date(self):
        self.db.put([
            ('test', dict(_type='foo', key='value', _date=self._last_week)),
            ('test', dict(_type='foo', key='value', _date=self._yesterday)),
        ])
        results = self.db.get(start_date=self._yesterday).all()
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0].date, self._yesterday)

    def test_filter_start_and_end_date(self):
        self.db.put([
            ('test', dict(_type='foo', key='value', _date=self._last_week)),
            ('test', dict(_type='foo', key='value', _date=self._yesterday)),
            ('test', dict(_type='foo', key='value', _date=self._today)),
        ])
        results = self.db.get(start_date=self._last_week,
                              end_date=self._yesterday).all()
        self.assertEquals(len(results), 2)

    def test_put_item_with_date(self):
        self.db.put([
            ('test', dict(_type='foo', key='value', _date=self._yesterday)),
        ])
        results = self.db.get(start_date=self._last_week).all()
        self.assertEquals(len(results), 1)

    def test_filter_type(self):
        self.db.put([
            ('test', dict(_type='foo', key='value', _date=self._yesterday)),
            ('test', dict(_type='foobar', key='value', _date=self._today)),
        ])
        results = self.db.get(type='foo').all()
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0].type, 'foo')

    def test_filter_type_and_date(self):
        self.db.put([
            ('test', dict(_type='foo', key='value', _date=self._last_week)),
            ('test', dict(_type='foo', key='value', _date=self._yesterday)),
            ('test', dict(_type='bar', key='value', _date=self._yesterday)),
            ('test', dict(_type='foo', key='value', _date=self._today)),
            ('test', dict(_type='bar', key='value', _date=self._today)),
        ])
        results = self.db.get(start_date=self._last_week,
                              end_date=self._yesterday,
                              type='foo').all()

        types = [r.type for r in results]

        self.assertEquals(len(results), 2)
        self.assertTrue('bar' not in types)
