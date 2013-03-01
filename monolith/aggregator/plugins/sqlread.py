import datetime
import json
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from monolith.aggregator.plugins import Plugin


class SQLRead(Plugin):
    def __init__(self, **options):
        super(SQLRead, self).__init__(**options)
        self.sqluri = options['database']
        extras = {}

        if not self.sqluri.startswith('sqlite'):
            extras['pool_size'] = int(options.get('pool_size', 10))
            extras['pool_timeout'] = int(options.get('pool_timeout', 30))
            extras['pool_recycle'] = int(options.get('pool_recycle', 60))

        self.engine = create_engine(self.sqluri, **extras)
        self.query = text(options['query'])
        self.mysql = 'mysql' in self.engine.driver
        self.json_fields = [field.strip() for field in
                            options.get('json_fields', '').split(',')]

    def _check(self, data):
        data = dict(data)

        for field in self.json_fields:
            if field not in data:
                continue
            value = data[field]
            data.update(json.loads(value))
            del data[field]

        if self.mysql:
            return data

        if '_date' in data:
            date = data['_date']
            if isinstance(date, basestring):
                data['_date'] = datetime.datetime.strptime(date, '%Y-%m-%d')

        return data

    def extract(self, start_date, end_date):

        query_params = {}
        unwanted = ('database', 'parser', 'here', 'query')

        for key, val in self.options.items():
            if key in unwanted:
                continue
            query_params[key] = val

        query_params['start_date'] = start_date
        query_params['end_date'] = end_date
        data = self.engine.execute(self.query, **query_params)
        return (self._check(line) for line in data)