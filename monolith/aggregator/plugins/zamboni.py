from datetime import timedelta

from monolith.aggregator.plugins.utils import iso2datetime, TastypieReader


class APIReader(TastypieReader):
    """This plugins calls the zamboni API and returns it."""

    def __init__(self, **options):
        super(APIReader, self).__init__(**options)
        self.endpoint = options['endpoint']
        self.type = options['type']
        self.field = options['field']
        self.options = options

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

        for item in data:
            values = item.pop('value')
            if 'app-id' in values:
                values['add_on'] = values.pop('app-id')
            values[self.field] = values.pop('count', 2)

            values.update({'_date': iso2datetime(item['recorded']),
                           '_type': self.type})
            yield values
