import copy
import datetime
import os
import re
import sys
import tempfile

from httpretty import HTTPretty
from httpretty import httprettified
from pyelastictest import IsolatedTestCase

from monolith.aggregator.extract import extract, main
from monolith.aggregator.plugins import extract as extract_plugin
from monolith.aggregator.plugins import Plugin
from monolith.aggregator.util import word2daterange
from monolith.aggregator.engine import AlreadyDoneError, RunError
from monolith.aggregator.tests.test_zamboni import _mock_fetch_uris

_res = {}
TODAY = datetime.date.today()
_FAILS = 0


class ESDummy(Plugin):

    def fill_es(self, data):
        global _res
        for source, line in data:
            _res[str(line['_id'])] = line

    def clear(self, start_date, end_date, source_ids):
        # dummy version ignores source_ids filtering
        global _res
        remove = []
        for key, value in _res.items():
            date = value['date'].date()
            if date >= start_date and date <= end_date:
                remove.append(key)
        for key in remove:
            del _res[key]


class PutESPlugin(ESDummy):

    def inject(self, data, **options):
        self.fill_es(data)


class PutESFailPlugin(ESDummy):

    def inject(self, data, **options):
        global _FAILS
        _FAILS += 1
        if _FAILS < 2:
            raise ValueError('boom')
        # things will work fine after the 2nd call
        self.fill_es(data)


@extract_plugin
def get_ga_fails(start_date, end_date, **options):
    raise ValueError('boom')


@extract_plugin
def get_ga(start_date, end_date, **options):
    for i in range(10):
        yield {'_type': 'google_analytics', '_date': TODAY}


@extract_plugin
def get_solitude(start_date, end_date, **options):
    for i in range(100):
        yield {'_type': 'solitude', '_date': TODAY}


@extract_plugin
def get_market_place(start_date, end_date, **options):
    for i in range(2):
        yield {'_type': 'marketplace', '_date': TODAY}


class TestExtract(IsolatedTestCase):

    def setUp(self):
        super(TestExtract, self).setUp()
        global _res, _FAILS
        _res = {}
        _FAILS = 0

    def _make_config(self, base_name, **kw):
        here = os.path.dirname(__file__)
        kw['es_location'] = self.es_cluster.urls[0]
        kw['tests_path'] = here
        base_config = os.path.join(here, base_name)
        temp_dir = tempfile.mkdtemp()
        fd, config = tempfile.mkstemp(dir=temp_dir)
        with open(base_config) as base:
            config_text = base.read()
            config_text = config_text.format(**kw)
            os.write(fd, config_text)
        return config, temp_dir

    def test_fails(self):
        config, _ = self._make_config('config_fails.ini')
        # retrying 3 times before failing in the extract phase
        start, end = word2daterange('last-month')
        self.assertRaises(RunError, extract, config, start, end)

    def test_retry(self):
        config, _ = self._make_config('config_retry.ini')
        # retrying 3 times before failing in the load phase.
        start, end = word2daterange('today')
        extract(config, start, end)
        self.assertEqual(len(_res), 102)

    def test_extract(self):
        config, _ = self._make_config('config_extract.ini')

        def _count():
            self.es_client.refresh()
            return self.es_client.count({'match_all': {}})['count']

        start, end = word2daterange('today')
        extract(config, start, end)
        count = _count()
        self.assertEqual(count, 102)

        # a second attempt should fail
        # because we did not use the force flag
        self.assertRaises(AlreadyDoneError, extract, config, start, end)

        # unless we force it
        extract(config, start, end, force=True)
        # overwrite has generated the same entries with new ids, so
        # we end up with double the entries
        self.assertEqual(count, _count())

        # forcing only the load phase
        extract(config, start, end, sequence='load', force=True)
        # loading the same data (ids) won't generate any more entries
        self.assertEqual(count, _count())

    @httprettified
    def test_main(self):
        config, temp_dir = self._make_config('config_main.ini')
        _mock_fetch_uris('https://addons.mozilla.dev/api/monolith/data/',
                         '/api/monolith/data/')
        here = os.path.dirname(__file__)
        with open(os.path.join(here, 'ga_rest.json'), 'r') as fd:
            HTTPretty.register_uri(
                HTTPretty.GET,
                re.compile('.*/discovery/v1/apis/analytics/.*'),
                body=fd.read(),
            )

        with open(os.path.join(here, 'ga.json'), 'r') as fd:
            HTTPretty.register_uri(
                HTTPretty.GET,
                re.compile('.*/analytics/v3/data/ga.*'),
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
        # overwrite has removed all data and added new entries
        self.assertEqual(count, len(_res))

        # purge only
        self.assertEqual(_run(['--force', '--purge-only', config]), 0)
        # purging doesn't add or remove entries
        self.assertEqual(count, len(_res))
