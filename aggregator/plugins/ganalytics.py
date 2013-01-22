from aggregator.plugins import Plugin
from googleanalytics import Connection


class GoogleAnalytics(Plugin):
    def __init__(self, **options):
        self.options = options
        self.connection = Connection(option['login'], option['password'])
        self.account = self.connection.get_account(option['account'])
        self.metrics = [metric.strip()
                        for metric in option['metrics'].split(',')]
        self.dimensions = [dimension.strip()
                           for dimension in option['dimensions'].split(',')]

    def __call__(self, start_date, end_date, **options):
        data = self.account.get_data(start_date, end_date, metrics=self.metrics,
                                     dimensions=self.dimensions)
        for elmt in data:
            yield dict(elmt)
