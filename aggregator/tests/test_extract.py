import sys
import os
import random
import copy
import datetime
import json

from unittest2 import TestCase
from sqlalchemy import create_engine

from aggregator.extract import extract, main
from aggregator.plugins import plugin
from aggregator.util import word2daterange
from aggregator.engine import AlreadyDoneError
from sqlalchemy.sql import text


_res = []
_FEED = os.path.join(os.path.dirname(__file__), 'feed.xml')


@plugin
def put_es(data, **options):
    """ElasticSearch
    """
    for d in data:
        _res.append(d)


@plugin
def get_ga(start_date, end_date, **options):
    """Google Analytics
    """
    for i in range(10):
        yield {'from': 'Google Analytics'}


@plugin
def get_rest(start_date, end_date, **options):
    """Solitude
    """
    for i in range(100):
        yield {'from': 'Solitude'}


@plugin
def get_market_place(start_date, end_date, **options):
    """MarketPlace
    """
    for i in range(2):
        yield {'from': 'Marketplace'}


DB_FILES = (os.path.join(os.path.dirname(__file__), 'source.db'),
            os.path.join(os.path.dirname(__file__), 'target.db'),
            os.path.join(os.path.dirname(__file__), 'history.db'))

DB = 'sqlite:///' + DB_FILES[0]

CREATE = """\
create table downloads
    (count INTEGER, date DATE)
"""

INSERT = text("""\
insert into downloads (count, date)
values (:count, :date)
""")


class TestExtract(TestCase):

    def setUp(self):
        self.config = os.path.join(os.path.dirname(__file__), 'config.ini')
        _res[:] = []

        # let's create a DB for the tests
        engine = create_engine(DB)
        today = datetime.date.today()

        try:
            engine.execute(CREATE)
            for i in range(30):
                date = today - datetime.timedelta(days=i)
                for i in range(10):
                    v = random.randint(0, 1000)
                    engine.execute(INSERT, count=v, date=date)
        except Exception:
            self.tearDown()
            raise

        from apiclient.http import HttpRequest

        def _execute(self, *args, **options):
            call = self.uri.split('/')[-1].split('?')[0]
            name = os.path.join(os.path.dirname(__file__), '%s.json' % call)

            with open(name) as f:
                data = f.read()

            return json.loads(data)

        HttpRequest.execute = _execute

    def tearDown(self):
        for file_ in DB_FILES:
            if os.path.exists(file_):
                os.remove(file_)

    def test_extract(self):
        start, end = word2daterange('last-month')
        extract(self.config, start, end)
        self.assertEqual(len(_res), 6080)

        # a second attempt should fail
        # because we did not use the force flag
        self.assertRaises(AlreadyDoneError, extract, self.config, start, end)

        # unless we force it
        extract(self.config, start, end, force=True)
        self.assertEqual(len(_res), 6080 * 3)

    def test_main(self):
        # XXX this still depends on google.com, on this call:
        # aggregator/plugins/ganalytics.py:24
        #    return build('analytics', 'v3', http=h)
        old = copy.copy(sys.argv)
        sys.argv[:] = ['python', '--date', 'last-month', self.config]
        try:
            main()
        finally:
            sys.argv[:] = old

        self.assertEqual(len(_res), 6080)

        # a second attempt should fail
        # because we did not use the force flag
        old = copy.copy(sys.argv)
        sys.argv[:] = ['python', '--date', 'last-month', self.config]
        try:
            self.assertRaises(AlreadyDoneError, main)
        finally:
            sys.argv[:] = old

        # unless we force it
        old = copy.copy(sys.argv)
        sys.argv[:] = ['python', '--force', '--date', 'last-month',
                       self.config]
        try:
            main()
        finally:
            sys.argv[:] = old

        self.assertEqual(len(_res), 6080 * 3)
