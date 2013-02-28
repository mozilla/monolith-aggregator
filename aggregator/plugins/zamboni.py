import hashlib

from datetime import datetime
from operator import itemgetter
from itertools import groupby
from urlparse import urljoin

from requests import Request, Session
from oauth_hook import OAuthHook

from aggregator.plugins import Plugin

_ISO = '%Y-%m-%dT%H:%M:%S'


def iso2datetime(data):
    return datetime.strptime(data, _ISO)


class APIReader(Plugin):
    """This plugins calls the zamboni API and aggregate the data before
    returning it.

    It needs to be subclassed, and shouldn't be used like that.
    Check GetAppInstalls for an example.
    """

    def __init__(self, parser=None, **kwargs):
        self.endpoint = kwargs['endpoint']
        self.options = kwargs

        username = kwargs.get('username', None)
        password = kwargs.get('password', None)
        self.client = Session()
        self.oauth_hook = None
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
        params = {'key': self.type,
                  'recorded__gte': start_date.isoformat(),
                  'recorded__lte': end_date.isoformat()}

        req = Request('DELETE', self.endpoint, params=params)
        if self.oauth_hook:
            self.oauth_hook(req)

        res = self.client.send(req.prepare())
        res.raise_for_status()

    def extract(self, start_date, end_date):

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
            'recorded_lte': end_date.isoformat()})

        return self.aggregate(data)


class GetAppInstalls(APIReader):

    type = 'install'

    def aggregate(self, items):
        # sort by date, addon and then by user.
        general_sort_key = lambda x: (x['recorded'],
                                      x['value']['app-id'],
                                      x['value']['anonymous'])
        items = sorted(items, key=general_sort_key)

        # group by addon
        dates = groupby(items, key=itemgetter('recorded'))

        for date, date_group in dates:
            addons = groupby(date_group, key=lambda x: x['value']['app-id'])
            for app_id, addon_group in addons:
                # for each addon, group by user.

                groupby_anon = groupby(addon_group,
                                       key=lambda x: x['value']['anonymous'])
                for anonymous, group in groupby_anon:
                    count = sum([i['value'].get('installs', 1) for i in group])
                    yield {'_date': iso2datetime(date),
                           '_type': self.type,
                           'add_on': app_id,
                           'installs_count': count,
                           'anonymous': anonymous,
                           }
