from datetime import timedelta, date
from collections import defaultdict

from monolith.aggregator.plugins.utils import iso2datetime, TastypieReader


class APIReader(TastypieReader):
    """This plugins calls the zamboni API and returns it."""

    def __init__(self, **options):
        super(APIReader, self).__init__(**options)
        self.endpoint = options['endpoint']
        self.type = options['type']
        self.field = options['field']
        self.options = options
        self.dimensions = [dimension.strip() for dimension in
                           options.get('dimensions', 'user-agent').split(',')]

    def purge(self, start_date, end_date):
        if self.options.get('purge_data', False):
            end_date = end_date + timedelta(days=1)
            params = {'key': self.type,
                      'recorded__gte': start_date.isoformat(),
                      'recorded__lte': end_date.isoformat()}

            res = self.delete(self.endpoint, params=params)
            res.raise_for_status()

    def extract(self, start_date, end_date):
        end_date = end_date + timedelta(days=1)

        data = self.read_api(self.endpoint, {
            'key': self.type,
            'recorded__gte': start_date.isoformat(),
            'recorded__lte': end_date.isoformat()})

        # building counts grouped by date & dimensions
        results = defaultdict(int)

        for item in data:
            timestamp = iso2datetime(item['recorded'])
            day = date(timestamp.year, timestamp.month, timestamp.day)
            values = item.pop('value')
            key = [('_date', day)]
            for dimension in self.dimensions:
                if dimension in values:
                    key.append((dimension, values[dimension]))
            key.sort()
            key = tuple(key)

            results[key] += 1

        # rendering the result
        for key, count in results.items():
            line = {'_type': self.type, self.field: count}
            for field, value in key:
                line[field] = value
            yield line
