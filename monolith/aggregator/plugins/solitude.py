from datetime import timedelta
from monolith.aggregator.plugins.utils import iso2datetime, TastypieReader


class SolitudeReader(TastypieReader):

    def __init__(self, parser, **options):
        self.options = options
        self.endpoint = options['endpoint']
        self.type = options['type']

    def extract(self, start_date, end_date):
        end_date = end_date + timedelta(days=1)
        items = self.read_api(self.endpoint, {
            'key': self.type,
            'recorded__gte': start_date.isoformat(),
            'recorded__lte': end_date.isoformat()})

        for item in items:
            values = {
                '_date': iso2datetime(item['created']),
                '_type': self.type,
                'payment_type': item['type'],
            }
            for key in ('amount', 'buyer', 'currency', 'provider', 'status'):
                values[key] = item[key]

            yield values
