import os
import hashlib

from ConfigParser import ConfigParser
from datetime import datetime, timedelta
from urlparse import urljoin

from requests import Request, Session
from oauth_hook import OAuthHook

from monolith.aggregator.plugins import Plugin

_ISO = '%Y-%m-%dT%H:%M:%S'


def iso2datetime(data):
    return datetime.strptime(data, _ISO)


class APIReader(Plugin):
    """This plugins calls the zamboni API and returns it."""

    def __init__(self, parser=None, **kwargs):
        self.endpoint = kwargs['endpoint']
        self.type = kwargs['type']
        self.options = kwargs

        self.client = Session()
        self.oauth_hook = None

        if 'password-file' in kwargs:
            passwd = kwargs['password-file']
            if not os.path.exists(passwd):
                raise ValueError('%r not found.' % passwd)

            parser = ConfigParser()
            parser.read(passwd)
            username = parser.get('auth', 'username', None)
            password = parser.get('auth', 'password', None)

            if username and password:
                key, secret = self._get_oauth_credentials(username, password)
                self.oauth_hook = OAuthHook(consumer_key=key,
                                            consumer_secret=secret,
                                            header_auth=True)

    def _get_oauth_credentials(self, username, password):
        key = hashlib.sha512(password + username + 'key').hexdigest()
        secret = hashlib.sha512(password + username + 'secret').hexdigest()
        return key, secret

    def purge(self, start_date, end_date):
        if self.options.get('purge_data', False):
            end_date = end_date + timedelta(days=1)
            params = {'key': self.type,
                      'recorded__gte': start_date.isoformat(),
                      'recorded__lte': end_date.isoformat()}

            req = Request('DELETE', self.endpoint, params=params)
            if self.oauth_hook:
                self.oauth_hook(req)

            res = self.client.send(req.prepare())
            res.raise_for_status()

    def extract(self, start_date, end_date):
        end_date = end_date + timedelta(days=1)

        data = []

        def _do_query(url, params=None):
            if not params:
                params = {}

            req = Request('GET', url, params=params)
            if self.oauth_hook:
                self.oauth_hook(req)

            res = self.client.send(req.prepare())

            res = res.json()
            data.extend(res['objects'])

            # we can have paginated elements, so we need to get them all
            if 'meta' in res and res['meta']['next']:
                _do_query(urljoin(url, res['meta']['next']))

        _do_query(self.endpoint, {
            'key': self.type,
            'recorded__gte': start_date.isoformat(),
            'recorded__lte': end_date.isoformat()})

        general_sort_key = lambda x: (x['recorded'],
                                      x['value']['app-id'],
                                      x['value']['anonymous'])
        data = sorted(data, key=general_sort_key)

        for item in data:
            values = item.pop('value')
            values['add_on'] = values.pop('app-id')
            values['apps_installed'] = values.pop('count', 1)

            values.update({'_date': iso2datetime(item['recorded']),
                           '_type': self.type})
            yield values
