from aggregator.plugins import Plugin
import csv
from datetime import date
from time import mktime, strptime


fields = {'mmo_total_visitors': 'visits',
          'apps_count_installed': 'app_installs',
          'apps_review_count_new': 'review_count',
          'mmo_user_count_new': 'user_count',
          'apps_count_new': 'app_count',
          'mmo_user_count_total': 'total_user_count'}


# UTC ??
class CSVReader(Plugin):

    def __init__(self, **options):
        super(CSVReader, self).__init__(**options)
        self._filename = options['filename']
        self.type = options['type']

    def extract(self, start_date, end_date):
        with open('global_stats.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t')

            for index, row in enumerate(reader):
                if index == 0:
                    continue
                id, name, count, _date = row
                count = int(count)
                _date = date.fromtimestamp(mktime(strptime(_date, '%Y-%m-%d')))

                if _date >= start_date and date <= end_date and name in fields:
                    data = {'_date': _date, '_type': self.type}
                    data[fields[name]] = count
                    yield data
