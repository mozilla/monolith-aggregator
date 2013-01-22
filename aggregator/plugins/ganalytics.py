from aggregator.plugins import Plugin
from googleanalytics import Connection


class GoogleAnalytics(Plugin):
    def __init__(self, **options):
        self.options = options
        self.connection = Connection(options['login'], options['password'])
        self.account = self.connection.get_account(options['account'])
        self.metrics = [metric.strip()
                        for metric in options['metrics'].split(',')]
        self.dimensions = [dimension.strip()
                           for dimension in options['dimensions'].split(',')]

    def __call__(self, start_date, end_date, **options):
        data = self.account.get_data(start_date, end_date,
                                     metrics=self.metrics,
                                     dimensions=self.dimensions)
        for elmt in data:
            yield dict(elmt)
