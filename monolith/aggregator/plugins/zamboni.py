from datetime import timedelta, date
from monolith.aggregator import logger
from monolith.aggregator.plugins.utils import iso2datetime, TastypieReader

class APIReader(TastypieReader):
    """This plugins calls the zamboni API and returns it."""

    def __init__(self, **options):
        super(APIReader, self).__init__(**options)
        self.endpoint = options['endpoint']
        self.type = options['type']
        self.field = options['field']
        self.limit = options.get('limit', '500')
        self.options = options
        self.dimensions = [dimension.strip() for dimension in
                           options.get('dimensions', 'user-agent').split(',')]
        self.updatable_fields = [field.strip() for field in
                                 options.get('updatable_fields',
                                             '').split(',')]

    def purge(self, start_date, end_date):
        if self.options.get('purge_data', False):
            end_date = end_date + timedelta(days=1)
            params = {'key': self.type,
                      'limit': self.limit,
                      'start': start_date.isoformat(),
                      'end': end_date.isoformat()}

            res = self.delete(self.endpoint, params=params)
            res.raise_for_status()

    def _update_fields(self, fields, new_values):
        for key, value in new_values.items():
            if key not in fields:
                fields[key] = value
            else:
                if key not in self.updatable_fields:
                    continue

                if isinstance(value, (int, float)):
                    # sum
                    fields[key] += value
                else:
                    # overwrite
                    fields[key] = value

    def extract(self, start_date, end_date):
        end_date = end_date + timedelta(days=1)
        params = {
            'key': self.type,
            'limit': self.limit,
            'start': start_date.isoformat(),
            'end': end_date.isoformat()
        }
        data = self.read_api(self.endpoint, params)

        logger.debug("Received from %s %s -- %s" % (self.endpoint, params,
                                                    data))
        # building counts grouped by date & dimensions
        results = {}

        for item in data:
            timestamp = iso2datetime(item['recorded'])
            day = date(timestamp.year, timestamp.month, timestamp.day)

            values = item.pop('value')
            values['_date'] = day
            key = [('_date', day)]

            for dimension in self.dimensions:
                if dimension in values:
                    key.append((dimension, values[dimension]))

            key.sort()
            key = tuple(key)

            count = values.get('count', 1)

            if key not in results:
                results[key] = values
                results[key]['_type'] = self.type
                results[key][self.field] = count
            else:
                self._update_fields(results[key], values)
                results[key][self.field] += count

        # rendering the result
        for line in results.values():
            yield line
        logger.debug("%s completed" % (self.type,))
