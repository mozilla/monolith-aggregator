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

    def test_record_creation_defaults_to_today(self):
        before = self._today
        self.db.put([
            ('test', dict(category='foo', key='value', another_key='value2')),
        ])
        query = self.db.session.query(Record)
        results = query.all()
        self.assertTrue(before <= results[0].date <= self._today)

    def test_record_creation_use_specified_date(self):
        self.db.put([
            ('test', dict(category='foo', key='value', date=self._last_week)),
        ])
        query = self.db.session.query(Record)
        results = query.all()
        self.assertEquals(results[0].date, self._last_week)

    def test_record_retrieval_need_a_filter(self):
        self.assertRaises(ValueError, self.db.get)

    def test_filter_end_date(self):
        self.db.put([
            ('test', dict(category='foo', key='value', date=self._last_week)),
            ('test', dict(category='foo', key='value', date=self._today)),
        ])
        results = self.db.get(end_date=self._yesterday).all()
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0].date, self._last_week)

    def test_filter_start_date(self):
        self.db.put([
            ('test', dict(category='foo', key='value', date=self._last_week)),
            ('test', dict(category='foo', key='value', date=self._yesterday)),
        ])
        results = self.db.get(start_date=self._yesterday).all()
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0].date, self._yesterday)

    def test_filter_start_and_end_date(self):
        self.db.put([
            ('test', dict(category='foo', key='value', date=self._last_week)),
            ('test', dict(category='foo', key='value', date=self._yesterday)),
            ('test', dict(category='foo', key='value', date=self._today)),
        ])
        results = self.db.get(start_date=self._last_week,
                              end_date=self._yesterday).all()
        self.assertEquals(len(results), 2)

    def test_put_item_with_date(self):
        self.db.put([
            ('test', dict(category='foo', key='value', date=self._yesterday)),
        ])
        results = self.db.get(start_date=self._last_week).all()
        self.assertEquals(len(results), 1)

    def test_filter_category(self):
        self.db.put([
            ('test', dict(category='foo', key='value')),
            ('test', dict(category='foobar', key='value')),
        ])
        results = self.db.get(category='foo').all()
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0].category, 'foo')

    def test_filter_category_and_date(self):
        self.db.put([
            ('test', dict(category='foo', key='value', date=self._last_week)),
            ('test', dict(category='foo', key='value', date=self._yesterday)),
            ('test', dict(category='bar', key='value', date=self._yesterday)),
            ('test', dict(category='foo', key='value', date=self._today)),
            ('test', dict(category='bar', key='value', date=self._today)),
        ])
        results = self.db.get(start_date=self._last_week,
                              end_date=self._yesterday,
                              category='foo').all()

        categories = [r.category for r in results]

        self.assertEquals(len(results), 2)
        self.assertTrue('bar' not in categories)
