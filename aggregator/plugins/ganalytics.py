import json
import datetime

from aggregator.plugins import Plugin
from aggregator import __version__

from gdata.analytics.client import AnalyticsClient, DataFeedQuery
from gdata.gauth import OAuth2Token
from gdata.analytics.client import ProfileQuery, WebPropertyQuery


SOURCE_APP_NAME = 'monolith-aggregator-v%s' % __version__


def get_profile_id(domain, client):
    # collecting profiles
    wbp = client.get_management_feed(WebPropertyQuery())
    for entry in wbp.entry:
        account_id = entry.get_property('ga:accountId').value
        web_property_id = entry.get_property('ga:webPropertyId').value
        profile_query = ProfileQuery(acct_id=account_id,
                                     web_prop_id=web_property_id)
        feed = client.get_management_feed(profile_query)
        for entry in feed.entry:
            # we got 'marketplace.firefox.com (unfiltered)' ... so not sure
            # about that
            # XXX domain?
            profile = entry.get_property('ga:profileName').value
            if profile == domain:
                return entry.get_property('ga:profileId').value

    raise NotImplementedError()


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

        if options['oauth'] == 'true':   # XXX we need a converter
            with open(options['oauth_token']) as f:
                token = json.loads(f.read())

            #token['token_expiry'] = iso2datetime(token['token_expiry'])

            fields = ('access_token', 'refresh_token', 'auth_uri',
                      'token_uri', 'revoke_uri')

            args = {}
            for field in token.keys():
                if field in fields:
                    args[field] = token[field]

            self.token = OAuth2Token(token['client_id'], token['client_secret'],
                                     token.get('scope', ''), token['user_agent'],
                                     **args)
            self.client = AnalyticsClient(source=SOURCE_APP_NAME,
                                          auth_token=self.token)
            self.token.authorize(self.client)
        else:
            self.client = AnalyticsClient(source=SOURCE_APP_NAME)
            login = options['login']
            password = options['password']
            self.client.client_login(login, password, source=SOURCE_APP_NAME,
                                     service=self.client.auth_service)

        self.profile_id = _ga(get_profile_id(options['domain'], self.client))
        self.table_id = _ga(options['table_id'])
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
                   'start-date': start_date.isoformat(),
                   'end-date': end_date.isoformat(),
                   'dimensions': self.qdimensions,
                   'metrics': self.qmetrics}

        query = DataFeedQuery(options)
        feed = self.client.GetDataFeed(query)

        for entry in feed.entry:
            data = {}
            for dimension in self.dimensions:
                data[dimension] = entry.get_dimension(dimension).value

            for metric in self.metrics:
                data[metric] = float(entry.get_metric(metric).value)

            # XXX more stuff in that mapping ?
            yield data
