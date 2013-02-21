from copy import copy
from sqlalchemy import create_engine
from aggregator.plugins import Plugin


class SQLRead(Plugin):
    def __init__(self, **options):
        self.options = options
        self.sqluri = options['database']
        extras = {}

        if not self.sqluri.startswith('sqlite'):
            extras['pool_size'] = int(options.get('pool_size', 10))
            extras['pool_timeout'] = int(options.get('pool_timeout', 30))
            extras['pool_recycle'] = int(options.get('pool_recycle', 60))

        self.engine = create_engine(self.sqluri, **extras)
        self.query = options['query']

    def __call__(self, start_date, end_date):
        query_params = {}
        unwanted = ('database', 'parser', 'here', 'query')

        for key, val in self.options.items():
            if key in unwanted:
                continue
            query_params[key] = val

        query_params['start_date'] = start_date
        query_params['end_date'] = end_date
        return self.engine.execute(self.query, **query_params)
