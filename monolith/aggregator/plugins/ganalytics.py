from collections import deque
import time

from apiclient.discovery import build
from oauth2client.client import OAuth2Credentials
import httplib2
import gevent

from monolith.aggregator import __version__
from monolith.aggregator.plugins import Plugin
from monolith.aggregator.util import json_loads, date_range


SOURCE_APP_NAME = 'monolith-aggregator-v%s' % __version__


def get_service(**options):
    creds = OAuth2Credentials(
        *[options[k] for k in
          ('access_token', 'client_id', 'client_secret',
           'refresh_token', 'token_expiry', 'token_uri',
           'user_agent')])
    h = httplib2.Http()
    creds.authorize(h)
    return build('analytics', 'v3', http=h)


def _ga(name):
    name = name.strip()
    if name.startswith('ga:'):
        return name
    return 'ga:' + name


def _gatable(option):
    return [_ga(item) for item in option.split(',')]


class BaseGoogleAnalytics(Plugin):
    def __init__(self, **options):
        super(BaseGoogleAnalytics, self).__init__(**options)

        with open(options['oauth_token']) as f:
            token = json_loads(f.read())

        self.client = get_service(**token)
        self.profile_id = _ga(options['profile_id'])
        self.metrics = _gatable(options['metrics'])
        self.qmetrics = ','.join(self.metrics)
        if 'dimensions' in options:
            self.dimensions = _gatable(options['dimensions'])
            self.qdimensions = ','.join(self.dimensions)
        else:
            self.dimensions = ['ga:date']
            self.qdimensions = 'ga:date'
        if 'filters' in options:
            self.filters = _gatable(options['filters'])
            self.qfilters = ','.join(self.filters)
        else:
            self.qfilters = None
        if 'rate_limit' in options:
            self.rate_limit = int(options['rate_limit'])
        else:
            self.rate_limit = 3
        if 'rate_span' in options:
            self.rate_span = float(options['rate_span'])
        else:
            self.rate_span = 1.0
        self.frequency = deque(maxlen=self.rate_limit)

    def _fix_name(self, name):
        if name.startswith('ga:'):
            name = name[len('ga:'):]
        return name

    def _rate_limited_get(self, **options):
        if len(self.frequency) < self.rate_limit:
            self.frequency.append(time.time())
        else:
            # making sure we rate-limit our calls
            now = time.time()
            ten_calls_ago = self.frequency.popleft()
            freq = now - ten_calls_ago

            if freq < self.rate_span:
                gevent.sleep((self.rate_span - freq) + 0.1)
                now = time.time()

            self.frequency.append(now)

        return self.client.data().ga().get(**options).execute()

    def processor(self, rows, current_date, col_headers):
        for entry in rows:
            data = {'_date': current_date, '_type': 'visitors'}

            for index, value in enumerate(entry):
                field = self._fix_name(col_headers[index])
                data[field] = value

            yield data

    def extract(self, start_date, end_date):
        # we won't use GA aggregation feature here,
        # but extract day-by-day
        # can this query be batched
        for current in date_range(start_date, end_date):
            iso = current.isoformat()

            options = {'ids': self.profile_id,
                       'start_date': iso,
                       'end_date': iso,
                       'dimensions': self.qdimensions,
                       'filters': self.qfilters,
                       'metrics': self.qmetrics,
                       'start_index': 1,
                       'max_results': 1000}

            rows = []

            results = self._rate_limited_get(**options)

            while results.get('totalResults', 0) > 0:
                rows.extend(results['rows'])
                if results.get('nextLink'):
                    options['start_index'] += options['max_results']
                    results = self._rate_limited_get(**options)
                else:
                    break

            cols = [col['name'] for col in results['columnHeaders']]
            for data in self.processor(rows, current, cols):
                yield data


class GAPageViews(BaseGoogleAnalytics):

    def processor(self, rows, current_date, col_headers):
        for entry in rows:
            data = {'_date': current_date, '_type': 'visitors'}

            for index, value in enumerate(entry):
                field = self._fix_name(col_headers[index])
                if field == 'pageviews':
                    value = int(value)
                data[field] = value

            yield data


class GAVisits(BaseGoogleAnalytics):

    def processor(self, rows, current_date, col_headers):
        for entry in rows:
            data = {'_date': current_date, '_type': 'visitors'}

            for index, value in enumerate(entry):
                field = self._fix_name(col_headers[index])
                if field == 'visits':
                    value = int(value)
                data[field] = value

            yield data


class GAPerAppVisits(BaseGoogleAnalytics):

    def processor(self, rows, current_date, col_headers):
        for entry in rows:
            data = {'_date': current_date, '_type': 'per-app-visitors'}

            for index, value in enumerate(entry):
                field = self._fix_name(col_headers[index])

                if field == 'customVarValue7':
                    data['app-id'] = int(value)
                elif field == 'visits':
                    data['app_visits'] = int(value)

            # Only log if visits count is non-zero.
            if data.get('app_visits', 0) > 0:
                yield data


class GAAppInstalls(BaseGoogleAnalytics):
    """
    Handles pulling in the "Successful app install" custom event.

    Configure the source to include the following::

        metrics = ga:totalEvents
        dimensions = ga:eventLabel
        filters = ga:eventCategory=~Successful App Install

    This also aggregates by "eventLabel" whose value is <app name>:<app id>,
    which allows us to get per-app install counts by filtering by app-id via
    Monolith, or global install counts by excluding the filter.

    """
    def processor(self, rows, current_date, col_headers):
        for entry in rows:
            data = {'_date': current_date, '_type': 'installs'}

            for index, value in enumerate(entry):
                field = self._fix_name(col_headers[index])

                if field == 'eventLabel':
                    data['app-id'] = int(value.split(':')[-1])
                elif field == 'totalEvents':
                    data['app_installs'] = int(value)

            # Only log if install counts is non-zero (and we have data).
            if data.get('app_installs', 0) > 0:
                yield data
