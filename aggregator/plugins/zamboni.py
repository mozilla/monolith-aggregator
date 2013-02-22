from collections import defaultdict
from operator import itemgetter
from itertools import groupby
from uuid import uuid1
from urlparse import urljoin

import requests
from requests_oauthlib import OAuth1

from aggregator.plugins import Plugin


class Registry(object):
    def __init__(self):
        self.aggregators = {}

    def add(self):
        def wrapper(cls):
            self.aggregators[cls.key] = cls
            return cls
        return wrapper

    def get(self, key):
        return self.aggregators[key]

AGGREGATORS = Registry()


@AGGREGATORS.add()
class InstallsAggregator(object):

    key = 'app.installs'

    def aggregate(self, items, category):
        # sort by date, addon and then by user.
        general_sort_key = lambda x: (x['date'],
                                      x['data']['addon_id'],
                                      x['anonymous'])
        items = sorted(items, key=general_sort_key)

        # group by addon
        dates = groupby(items, key=itemgetter('date'))

        for date, date_group in dates:
            addons = groupby(date_group, key=lambda x: x['data']['addon_id'])
            for addon_id, addon_group in addons:
                # for each addon, group by user.
                for anonymous, group in groupby(addon_group,
                                                key=lambda x: x['anonymous']):
                    count = sum([i['data']['installs'] for i in group])
                    yield {'uuid': uuid1().hex,
                           'date': date,
                           'add_on': addon_id,
                           'installs_count': count,
                           'anonymous': anonymous,
                           'category': category}


class APIReader(Plugin):
    """This plugins calls the zamboni API and aggregate the data before
    returning it.
    """

    def __init__(self, parser, **kwargs):
        self.keys = kwargs['keys'].split(',')
        self.endpoint = kwargs['endpoint']
        self.oauth_key = kwargs.get('oauth_key', None)
        self.oauth_secret = kwargs.get('oauth_secret', None)
        if self.oauth_key and self.oauth_secret:
            self.oauth_header = OAuth1(self.oauth_key, self.oauth_secret)
        else:
            self.oauth_header = None

        # Store the data for each key.
        self._retrieved_data = defaultdict(list)

    def _get_data(self, key, start_date, end_date):
        """Gets the data from the API, takes care of the pagination if any.
        """
        def _do_query(url, params=None):
            if not params:
                params = {}
            res = requests.get(url, params=params,
                               auth=self.oauth_header).json()
            self._retrieved_data[key].extend(res['objects'])
            # we can have paginated elements, so we need to get them all
            if 'meta' in res and res['meta']['next']:
                _do_query(urljoin(url, res['meta']['next']))

        params = {'key': key,
                  'recorded__gte': start_date.isoformat(),
                  'recorded_lte': end_date.isoformat()}

        _do_query(self.endpoint, params)
        return self._retrieved_data[key]

    def _delete_data(self, key):
        min_id = min([int(i['id']) for i in self._retrieved_data[key]])
        max_id = max([int(i['id']) for i in self._retrieved_data[key]])
        params = {'key': key, 'id__gte': min_id, 'id__lte': max_id}
        requests.delete(self.endpoint, params=params)

    def __call__(self, start_date, end_date):
        # we want to do a call for each key we have.
        for key in self.keys:
            data = self._get_data(key, start_date, end_date)
            for item in AGGREGATORS.get(key)().aggregate(data, key):
                yield item
