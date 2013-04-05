import random
import re
from datetime import datetime, timedelta
from itertools import islice
from unittest2 import TestCase

from monolith.aggregator.util import json_dumps
from monolith.aggregator.plugins.zamboni import APIReader

from httpretty import HTTPretty
from httpretty import httprettified
from requests import HTTPError

DATA_ID = 0


def _get_data(count, key, user, app_id, date=None, **data):
    """Returns :param count: elements for the given user and app_id.

    You can pass additional data into :param data:
    """
    def __get_data(user=None, date=None, **data):
        global DATA_ID
        DATA_ID += 1
        return {'id': DATA_ID,
                'key': key,
                'user_hash': user,
                'value': data,
                'recorded': date}

    if date is None:
        date = datetime(2013, 02, 12, 17, 34)

    returned_data = []
    data['app-id'] = app_id
    for i in range(1, count + 1):
        data_ = __get_data(user=user,
                           anonymous=(user == 'anonymous'),
                           date=(date + timedelta(days=i)).isoformat(),
                           **data)
        returned_data.append(data_)
    return returned_data


def _get_raw_values():
    data = []
    data.extend(_get_data(20, 'install', 'anonymous', 1234, installs=1))
    data.extend(_get_data(20, 'install', 'alexis', 1234, installs=1))
    data.extend(_get_data(20, 'install', 'tarek', 1234, installs=1))
    data.extend(_get_data(20, 'install', 'alexis', 4321, installs=1))
    random.shuffle(data)
    return data


def _mock_fetch_uris(endpoint, resource_uri):
    raw_values = _get_raw_values()
    values = iter(raw_values)

    rest = islice(values, 20)
    rest_data = list(rest)
    offset = 0
    while rest_data:
        # match every request without "offset" in it
        if offset == 0:
            regexp = re.compile(endpoint + "(?!.*offset)")
        else:
            regexp = re.compile(endpoint + ".*&offset=%d.*" % offset)

        offset += 20

        if offset == 80:
            next_uri = None
        else:
            query = '/?limit=20&key=install&offset=%d'
            next_uri = resource_uri + query % offset

        HTTPretty.register_uri(
            HTTPretty.GET,
            regexp,
            body=json_dumps({'meta': {"limit": 20,
                                      "next": next_uri,
                                      "offset": 0,
                                      "previous": None,
                                      "total_count": len(raw_values)},
                             'objects': rest_data}))

        rest = islice(values, 20)
        rest_data = list(rest)


class TestAPIReader(TestCase):

    def setUp(self, *args, **kwargs):
        super(TestAPIReader, self).setUp(*args, **kwargs)
        self._data_id = 0
        self.server = 'marketplace.firefox.com'
        self.resource_uri = '/api/monolith/data'
        self.endpoint = self.server + self.resource_uri
        self.now = datetime(2013, 02, 12, 17, 34)
        self.yesterday = self.now - timedelta(days=1)
        self.last_week = self.now - timedelta(days=7)

    def test_get_id(self):
        reader = APIReader(id='mkt-install-foo',
                           endpoint='http://' + self.endpoint,
                           type='install', field='foo')
        self.assertEqual(reader.get_id(), 'mkt-install-foo')

    @httprettified
    def test_rest_endpoint_is_called(self):
        _mock_fetch_uris(self.endpoint, self.resource_uri)

        reader = APIReader(endpoint='http://' + self.endpoint, type='install',
                           field='foo')
        values = list(reader.extract(self.last_week, self.yesterday))

        # If we get back 80 values, it means that all the data had been read
        # from the API
        self.assertEquals(len(values), 80)

    @httprettified
    def test_purge_calls_delete(self):
        HTTPretty.register_uri(
            HTTPretty.DELETE,
            re.compile(self.endpoint + ".*install.*"),
            body="",
            status=204)

        reader = APIReader(endpoint='http://' + self.endpoint, type='install',
                           purge_data=True, field='foo')
        reader.purge(self.last_week, self.now)
        self.assertEquals(HTTPretty.last_request.method, 'DELETE')

    @httprettified
    def test_purge_raise_on_errors(self):
        HTTPretty.register_uri(
            HTTPretty.DELETE,
            re.compile(self.endpoint + ".*"),
            body="",
            status=400)
        reader = APIReader(endpoint='http://' + self.endpoint, type='install',
                           purge_data=True, field='foo')

        with self.assertRaises(HTTPError):
            reader.purge(self.last_week, self.now)
