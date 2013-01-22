from aggregator.plugins import Plugin
from aggregator import __version__
from gdata.analytics.client import AnalyticsClient, DataFeedQuery


SOURCE_APP_NAME = 'monolith-aggregator-v%s' % __version__


def _ga(name):
    name = name.strip()
    if name.startswith('ga:'):
        return name
    return 'ga:' + name


class GoogleAnalytics(Plugin):
    def __init__(self, **options):
        self.options = options
        self.client = AnalyticsClient(source=SOURCE_APP_NAME)
        login = options['login']
        password = options['password']
        self.client.client_login(login, password, source=SOURCE_APP_NAME,
                                 service=self.client.auth_service)
        self.table_id = _ga(options['table_id'])
        self.metrics = [_ga(metric) for metric in
                        options['metrics'].split(',')]
        self.qmetrics = ','.join(self.metrics)
        self.dimensions = ','.join([_ga(dimension) for dimension
                                    in options['dimensions'].split(',')])

    def __call__(self, start_date, end_date, **options):
        query = DataFeedQuery({'ids': self.table_id,
                               'start-date': start_date.isoformat(),
                               'end-date': end_date.isoformat(),
                               'dimensions': 'ga:date',
                               'metrics': self.qmetrics,
                               'dimensions': self.dimensions})

        feed = self.client.GetDataFeed(query)

        for entry in feed.entry:
            data = {}
            for met in self.metrics:
                data[met] = float(entry.get_metric(met).value)
            yield data
