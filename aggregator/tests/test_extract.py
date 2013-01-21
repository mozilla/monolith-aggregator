import os
import random

from unittest2 import TestCase
from sqlalchemy import create_engine

from aggregator.extract import extract
from aggregator.plugins import plugin
from aggregator.db import Database


class SQLInjecter(object):
    """SQL"""

    def __init__(self, database, **options):
        self.db = Database(sqluri=database)

    def __call__(self, data, **options):
        self.db.put(category='', **data)


_res = []


@plugin
def put_es(data, **options):
    """ElasticSearch
    """
    _res.append(data)


@plugin
def get_ga(**options):
    """Google Analytics
    """
    for i in range(10):
        yield {'from': 'Google Analytics'}


@plugin
def get_rest(**options):
    """Solitude
    """
    for i in range(100):
        yield {'from': 'Solitude'}


@plugin
def get_market_place(**options):
    """MarketPlace
    """
    for i in range(2):
        yield {'from': 'Marketplace'}


DB_FILE = os.path.join(os.path.dirname(__file__), 'source.db')
DB = 'sqlite:///' + DB_FILE


class TestExtract(TestCase):

    def setUp(self):
        self.config = os.path.join(os.path.dirname(__file__), 'config.ini')
        _res[:] = []

        # let's create a DB for the tests
        engine = create_engine(DB)
        try:
            engine.execute('create table downloads (count INTEGER)')
            for i in range(100):
                v = random.randint(0, 1000)
                engine.execute('insert into downloads (count) values (%d)' % v)
        except Exception:
            self.tearDown()
            raise

    def tearDown(self):
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)

    def test_extract(self):

        extract(self.config)
        self.assertEqual(len(_res), 212)
