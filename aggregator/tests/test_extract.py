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


DB_FILE = os.path.join(os.path.dirname(__file__), 'source.db')
DB = 'sqlite:///' + DB_FILE

CREATE = """\
create table downloads
    (count INTEGER, start DATE, end DATE)
"""

INSERT = """\
insert into downloads (count, start, end)
values (:count, :start, :end)
"""


class TestExtract(TestCase):

    def setUp(self):
        self.config = os.path.join(os.path.dirname(__file__), 'config.ini')
        _res[:] = []

        # let's create a DB for the tests
        engine = create_engine(DB)
        today = datetime.date.today().isoformat()
        start, end = word2daterange('last-month')

        try:
            engine.execute(CREATE)
            for i in range(100):
                v = random.randint(0, 1000)
                engine.execute(INSERT, count=v, start=start, end=end)
        except Exception:
            self.tearDown()
            raise

        from aggregator.plugins import ganalytics
        from apiclient.http import HttpRequest

        def _execute(self, *args, **options):
            call = self.uri.split('/')[-1].split('?')[0]
            name = os.path.join(os.path.dirname(__file__), '%s.json' % call)

            with open(name) as f:
                data = f.read()

            return json.loads(data)


        HttpRequest.execute = _execute


    def tearDown(self):
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)

    def test_extract(self):
        start, end = word2daterange('last-month')
        extract(self.config, start, end)
        self.assertEqual(len(_res), 6202)

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

        self.assertEqual(len(_res), 6202)
