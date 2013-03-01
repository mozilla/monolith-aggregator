import datetime
import re

import requests
from monolith.aggregator.plugins import Plugin


class FileReader(Plugin):

    def __init__(self, parser, **options):
        metrics_config = dict(parser.items('metrics'))
        self._options = options

        self._auth = (metrics_config['username'],
                      metrics_config['password'])
        self._baseurl = metrics_config['url']
        self._filename_format = options['filename_format']
        self._data_format = re.compile(options['data_format'])
        self._type = options['type']

    def extract(self, start_date, end_date):
        date = start_date
        while date <= end_date:
            url = self._baseurl + date.strftime(self._filename_format)
            resp = requests.get(url, auth=self._auth)
            if resp.status_code == 200:
                for item in self._parse_data(resp.content, date):
                    yield item

            date += datetime.timedelta(days=1)

    def _parse_data(self, content, date):
        def _get_item(data):
            item = {'_type': self._type, '_date': date}
            item.update(data.groupdict())
            return item

        return (_get_item(d) for d in self._data_format.finditer(content))
