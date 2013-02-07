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


def get_profile_id(service, domain):
    accounts = service.management().accounts().list().execute()
    account_ids = [a['id'] for a in accounts.get('items', ())]
    for account_id in account_ids:
        webproperties = service.management().webproperties().list(
            accountId=account_id).execute()
        webproperty_ids = [p['id'] for p in webproperties.get('items', ())]
        for webproperty_id in webproperty_ids:
            profiles = service.management().profiles().list(
                accountId=account_id,
                webPropertyId=webproperty_id).execute()
            for p in profiles.get('items', ()):
                # sometimes GA includes "http://", sometimes it doesn't.
                if '://' in p['websiteUrl']:
                    name = p['websiteUrl'].partition('://')[-1]
                else:
                    name = p['websiteUrl']

                if name == domain:
                    return p['id']


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
        self.options = options

        with open(options['oauth_token']) as f:
            token = json.loads(f.read())

        self.client = get_service(**token)
        self.profile_id = _ga(get_profile_id(self.client, options['domain']))
        self.metrics = _gatable(options['metrics'])
        self.qmetrics = ','.join(self.metrics)
        if 'dimensions' in options:
            self.dimensions = _gatable(options['dimensions'])
            self.qdimensions = ','.join(self.dimensions)
        else:
            self.dimensions = ['ga:date']
            self.qdimensions = 'ga:date'

    def __call__(self, start_date, end_date):
        options = {'ids': self.profile_id,
                   'start_date': start_date.isoformat(),
                   'end_date': end_date.isoformat(),
                   'dimensions': self.qdimensions,
                   'metrics': self.qmetrics}

        results = self.client.data().ga().get(**options).execute()

        cols = [col['name'] for col in results['columnHeaders']]

        for entry in results['rows']:
            data = {}
            for index, value in enumerate(entry):
                data[cols[index]] = value

            #for dimension in self.dimensions:
            #    data[dimension] = entry.get_dimension(dimension).value

            #for metric in self.metrics:
            #    data[metric] = float(entry.get_metric(metric).value)

            # XXX more stuff in that mapping ?
            yield data
