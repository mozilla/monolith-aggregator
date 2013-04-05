import copy
import datetime
import os
import random
import re
import sys

from httpretty import HTTPretty
from httpretty import httprettified
from unittest2 import TestCase
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from monolith.aggregator.extract import extract, main
from monolith.aggregator.plugins import inject as inject_plugin
from monolith.aggregator.plugins import extract as extract_plugin
from monolith.aggregator.util import json_loads
from monolith.aggregator.util import word2daterange
from monolith.aggregator.engine import AlreadyDoneError, RunError
from monolith.aggregator.tests.test_zamboni import _mock_fetch_uris

_res = {}
_FEED = os.path.join(os.path.dirname(__file__), 'feed.xml')
TODAY = datetime.date.today()


@inject_plugin
def put_es(data, overwrite, **options):
    """ElasticSearch
    """
    for source, line in data:
        _res[str(line['_id'])] = line


_FAILS = 0


@inject_plugin
def put_es_failing(data, overwrite, **options):
    global _FAILS
    if _FAILS == 2:
        # things will work fine after the 2nd call
        _FAILS = 3
        raise ValueError('boom')
    elif _FAILS < 2:
        _FAILS += 1

    for source, line in data:
        _res[str(line['_id'])] = line


@extract_plugin
def get_ga_fails(start_date, end_date, **options):
    raise ValueError('boom')


@extract_plugin
def get_ga(start_date, end_date, **options):
    """Google Analytics
    """
    for i in range(10):
        yield {'_type': 'google_analytics', '_date': TODAY}


@extract_plugin
def get_solitude(start_date, end_date, **options):
    """Solitude
    """
    for i in range(100):
        yield {'_type': 'solitude', '_date': TODAY}


@extract_plugin
def get_market_place(start_date, end_date, **options):
    """MarketPlace
    """
    for i in range(2):
        yield {'_type': 'marketplace', '_date': TODAY}


DB_FILES = (os.path.join(os.path.dirname(__file__), 'source.db'),
            os.path.join(os.path.dirname(__file__), 'target.db'),
            os.path.join(os.path.dirname(__file__), 'history.db'))

CREATE = """\
create table downloads
    (_date DATE, _type VARCHAR(32), count INTEGER)
"""

INSERT = text("""\
insert into downloads (_date, _type, count)
values (:_date, :_type, :count)
""")


class TestExtract(TestCase):

    def setUp(self):
        self._reset()
        global _res
        _res = {}

        from apiclient.http import HttpRequest

        def _execute(self, *args, **options):
            call = self.uri.split('/')[-1].split('?')[0]
            name = os.path.join(os.path.dirname(__file__), '%s.json' % call)

            with open(name) as f:
                data = f.read()

            return json_loads(data)

        HttpRequest.execute = _execute

    def tearDown(self):
        self._reset()

    def _reset(self):
        for file_ in DB_FILES:
            if os.path.exists(file_):
                os.remove(file_)

    def _make_sql_plugin_db(self):
        # let's create a DB for the tests
        self.engine = engine = create_engine('sqlite:///' + DB_FILES[0])
        today = datetime.date.today()
        engine.execute(CREATE)
        for i in range(8):
            date = today - datetime.timedelta(days=i)
            for i in range(2):
                v = random.randint(0, 1000)
                engine.execute(INSERT, _date=date, _type='sql', count=v)

    def test_extract(self):
        config = os.path.join(os.path.dirname(__file__), 'config_extract.ini')
        start, end = word2daterange('today')
        extract(config, start, end)
        count = len(_res)
        self.assertEqual(count, 102)

        # a second attempt should fail
        # because we did not use the force flag
        self.assertRaises(AlreadyDoneError, extract, config, start, end)

        # unless we force it
        extract(config, start, end, force=True)
        # overwrite has generated the same entries with new ids, so
        # we end up with double the entries
        self.assertEqual(count * 2, len(_res))

        # forcing only the load phase
        extract(config, start, end, sequence='load', force=True)
        # loading the same data (ids) won't generate any more entries
        self.assertEqual(count * 2, len(_res))

    @httprettified
    def test_main(self):
        config = os.path.join(os.path.dirname(__file__), 'config_main.ini')
        ga_rest = os.path.join(os.path.dirname(__file__), 'ga_rest.json')
        self._make_sql_plugin_db()
        _mock_fetch_uris('https://addons.mozilla.dev/api/monolith/data/',
                         '/api/monolith/data/')
        with open(ga_rest, 'r') as fd:
            HTTPretty.register_uri(
                HTTPretty.GET,
                re.compile('.*/discovery/v1/apis/analytics/.*'),
                body=fd.read(),
            )
        arguments = ['python', '--date', 'last-month', '--log-level=WARNING']

        def _run(args):
            old = copy.copy(sys.argv)
            sys.argv[:] = arguments + args
            exit = -1
            try:
                main()
            except AlreadyDoneError:
                exit = -42
            except SystemExit as exc:
                exit = exc.code
            finally:
                sys.argv[:] = old
            return exit

        self.assertEqual(_run([config]), 0)
        count = len(_res)
        self.assertTrue(count > 1000, count)

        # a second attempt should fail
        # because we did not use the force flag
        self.assertEqual(_run([config]), -42)

        # unless we force it
        self.assertEqual(_run(['--force', config]), 0)
        # overwrite has generated the same entries with new ids, so
        # we end up with double the entries
        self.assertEqual(count * 2, len(_res))

        # purge only
        self.assertEqual(_run(['--force', '--purge-only', config]), 0)
        # purging doesn't add new entries
        self.assertEqual(count * 2, len(_res))

    def test_retry(self):
        config = os.path.join(os.path.dirname(__file__), 'config_retry.ini')
        # retrying 3 times before failing in the load phase.
        start, end = word2daterange('today')
        extract(config, start, end)
        self.assertEqual(len(_res), 102)

    def test_fails(self):
        config = os.path.join(os.path.dirname(__file__), 'config_fails.ini')
        # retrying 3 times before failing in the extract phase
        start, end = word2daterange('last-month')
        self.assertRaises(RunError, extract, config, start, end)
