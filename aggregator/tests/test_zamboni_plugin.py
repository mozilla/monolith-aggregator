import json
import random
import re
from datetime import datetime, timedelta
from itertools import islice
from unittest2 import TestCase

from aggregator.plugins.zamboni import InstallsAggregator, APIReader

from httpretty import HTTPretty
from httpretty import httprettified
from requests import HTTPError

DATA_ID = 0


def get_data(count, key, user, addon_id, date=None, **data):
    """Returns :param count: elements for the given user and addon_id.

    You can pass additional data into :param data:
    """
    def __get_data(user=None, date=None, anonymous=None, **data):
        global DATA_ID
        DATA_ID += 1
        return {'id': DATA_ID,
                'key': key,
                'user': user,
                'data': data,
                'date': date,
                'anonymous': anonymous}

    if date is None:
        date = datetime(2013, 02, 12, 17, 34)

    returned_data = []
    for i in range(1, count + 1):
        data_ = __get_data(user=user,
                           addon_id=addon_id,
                           anonymous=(user == 'anonymous'),
                           date=(date + timedelta(days=i)).isoformat(),
                           **data)
        returned_data.append(data_)
    return returned_data


class TestInstallsAggregator(TestCase):
    def setUp(self, *args, **kwargs):
        global DATA_ID
        DATA_ID = 0
        super(TestInstallsAggregator, self).setUp(*args, **kwargs)

    def test_aggregation(self):
        data = []
        key = 'app.installs'
        data.extend(get_data(20, key, 'anonymous', 1234, installs=1))
        data.extend(get_data(20, key, 'alexis', 1234, installs=1))
        data.extend(get_data(20, key, 'tarek', 1234, installs=1))
        data.extend(get_data(20, key, 'alexis', 4321, installs=1))
        random.shuffle(data)

        aggregator = InstallsAggregator()
        yielded = aggregator.aggregate(data, type='foobar')
        records = [i for i in yielded if i['installs_count'] > 1]
        self.assertEquals(len(records), 20)


class TestAPIReader(TestCase):

    def setUp(self, *args, **kwargs):
        global DATA_ID
        DATA_ID = 0
        self.server = 'marketplace.firefox.com'
        self.resource_uri = '/api/monolith/data'
        self.endpoint = self.server + self.resource_uri
        self.now = datetime(2013, 02, 12, 17, 34)
        self.yesterday = self.now - timedelta(days=1)
        self.last_week = self.now - timedelta(days=7)
        self.calls = 0
        super(TestAPIReader, self).setUp(*args, **kwargs)

    def _get_raw_values(self, key):
        data = []
        data.extend(get_data(20, key, 'anonymous', 1234, installs=1))
        data.extend(get_data(20, key, 'alexis', 1234, installs=1))
        data.extend(get_data(20, key, 'tarek', 1234, installs=1))
        data.extend(get_data(20, key, 'alexis', 4321, installs=1))
        random.shuffle(data)
        return data

    def mock_fetch_uris(self):
        raw_values = self._get_raw_values('app.installs')
        values = iter(raw_values)

        rest = islice(values, 20)
        rest_data = list(rest)
        offset = 0
        while rest_data:
            # match every request without "offset" in it
            if offset == 0:
                regexp = re.compile(self.endpoint + "(?!.*offset)")
            else:
                regexp = re.compile(self.endpoint + ".*&offset=%d.*" % offset)

            offset += 20

            if offset == 80:
                next_uri = None
            else:
                query = '/?limit=20&key=app.installs&offset=%d'
                next_uri = self.resource_uri + query % offset

            HTTPretty.register_uri(
                HTTPretty.GET,
                regexp,
                body=json.dumps({'meta': {"limit": 20,
                                          "next": next_uri,
                                          "offset": 0,
                                          "previous": None,
                                          "total_count": len(raw_values)},
                                 'objects': rest_data}))

            rest = islice(values, 20)
            rest_data = list(rest)

    @httprettified
    def test_rest_endpoint_is_called(self):
        self.mock_fetch_uris()

        reader = APIReader(None, keys='app.installs',
                           endpoint='http://' + self.endpoint)
        values = list(reader.extract(self.last_week, self.yesterday))

        # If we get back 60 values, it means that all the data had been used
        # and aggregated, so we're good.
        self.assertEquals(len(values), 60)

    @httprettified
    def test_purge_calls_delete(self):
        self.calls = 0

        def _callback(method, uri, headers):
            self.calls += 1
            return ""

        HTTPretty.register_uri(
            HTTPretty.DELETE,
            re.compile(self.endpoint + ".*app.*"),
            body=_callback,
            status=204)

        HTTPretty.register_uri(
            HTTPretty.DELETE,
            re.compile(self.endpoint + ".*foo.*"),
            body=_callback,
            status=204)

        reader = APIReader(None, keys='app.installs,foo.bar',
                           endpoint='http://' + self.endpoint)
        reader.purge(self.last_week, self.now)
        self.assertEquals(self.calls, 2)

    @httprettified
    def test_purge_raise_on_errors(self):
        HTTPretty.register_uri(
            HTTPretty.DELETE,
            re.compile(self.endpoint + ".*"),
            body="",
            status=400)
        reader = APIReader(None, keys='app.installs,foo.bar',
                           endpoint='http://' + self.endpoint)

        with self.assertRaises(HTTPError):
            reader.purge(self.last_week, self.now)
