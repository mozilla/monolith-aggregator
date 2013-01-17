import os
from unittest2 import TestCase
from aggregator.extract import extract


_res = []
_databases = {}


def put_sql(data, **options):
    """ElasticSearch
    """
    dbname = options['database']
    if dbname not in _databases:
        db = Database(dbname)
        _databases[dbname] = db

    db = _databases[dbname]
    db.put(category='', **data)


def put_elasticsearch(data, **options):
    """ElasticSearch
    """
    _res.append(data)


def get_ganalytics(**options):
    """Google Analytics
    """
    for i in range(10):
        yield {'from': 'Google Analytics'}


def get_generic_rest(**options):
    """Solitude
    """
    for i in range(100):
        yield {'from': 'Solitude'}


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
