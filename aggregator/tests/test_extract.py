import os
from unittest2 import TestCase


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


class TestExtract(TestCase):

    def setUp(self):
        self.config = os.path.join(os.path.dirname(__file__), 'config.ini')
        _res[:] = []


    def test_extract(self):
        extract(self.config)
        self.assertEqual(len(_res), 112)
