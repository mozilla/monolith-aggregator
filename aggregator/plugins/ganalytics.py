import json
import datetime

from aggregator.plugins import Plugin
from aggregator import __version__

from apiclient.discovery import build
from oauth2client.client import OAuth2Credentials
import httplib2


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


_ISO = '%Y-%m-%dT%H:%M:%S.%f'


def iso2datetime(data):
    return datetime.datetime.strptime(data, _ISO)


class GoogleAnalytics(Plugin):
    def __init__(self, **options):
        super(GoogleAnalytics, self).__init__(**options)

        with open(options['oauth_token']) as f:
            token = json.loads(f.read())

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

    def _fix_name(self, name):
        if name.startswith('ga:'):
            name = name[len('ga:'):]
        return name

    def extract(self, start_date, end_date):
        # we won't use GA aggregation feature here,
        # but extract day-by-day

        # can this query be batched
        delta = (end_date - start_date).days
        drange = (start_date + datetime.timedelta(n) for n in range(delta))

        for current in drange:
            iso = current.isoformat()

            options = {'ids': self.profile_id,
                       'start_date': iso,
                       'end_date': iso,
                       'dimensions': self.qdimensions,
                       'metrics': self.qmetrics}

            results = self.client.data().ga().get(**options).execute()
            if results['totalResults'] == 0:
                continue

            cols = [col['name'] for col in results['columnHeaders']]
            for entry in results['rows']:
                data = {'_date': current, '_type': 'visitors'}

                for index, value in enumerate(entry):
                    field = self._fix_name(cols[index])
                    # XXX see how to convert genericaly
                    if field in ('pageviews', 'visitors'):
                        value = int(value)

                    data[field] = value

                yield data
