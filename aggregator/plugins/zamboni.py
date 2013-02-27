from operator import itemgetter
from itertools import groupby
from urlparse import urljoin

import requests
from oauth_hook import OAuthHook

from aggregator.plugins import Plugin


class APIReader(Plugin):
    """This plugins calls the zamboni API and aggregate the data before
    returning it.

    It needs to be subclassed, and shouldn't be used like that.
    Check GetAppInstalls for an example.
    """

    def __init__(self, parser=None, **kwargs):
        self.endpoint = kwargs['endpoint']
        key = kwargs.get('oauth_key', None)
        secret = kwargs.get('oauth_secret', None)
        if key and secret:
            oauth_hook = OAuthHook(consumer_key=key, consumer_secret=secret,
                                   header_auth=True)
            self.client = requests.session(hooks={'pre_request': oauth_hook})
        else:
            self.client = requests.session()

    def purge(self, start_date, end_date):
        params = {'key': self.type,
                  'recorded__gte': start_date.isoformat(),
                  'recorded__lte': end_date.isoformat()}
        res = self.client.delete(self.endpoint, params=params)
        res.raise_for_status()

    def extract(self, start_date, end_date):

        data = []

        def _do_query(url, params=None):
            if not params:
                params = {}
            res = self.client.get(url, params=params).json()
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

    type = 'app.installs'

    def aggregate(self, items):
        # sort by date, addon and then by user.
        general_sort_key = lambda x: (x['date'],
                                      x['data']['addon_id'],
                                      x['data']['anonymous'])
        items = sorted(items, key=general_sort_key)

        # group by addon
        dates = groupby(items, key=itemgetter('date'))

        for date, date_group in dates:
            addons = groupby(date_group, key=lambda x: x['data']['addon_id'])
            for addon_id, addon_group in addons:
                # for each addon, group by user.

                groupby_anon = groupby(addon_group,
                                       key=lambda x: x['data']['anonymous'])
                for anonymous, group in groupby_anon:
                    count = sum([i['data']['installs'] for i in group])
                    yield {'_date': date,
                           '_type': type,
                           'add_on': addon_id,
                           'installs_count': count,
                           'anonymous': anonymous,
                           'type': self.type}
