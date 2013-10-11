import csv
import decimal
import os
from collections import defaultdict
from ConfigParser import ConfigParser

from boto.s3.connection import S3Connection

from monolith.aggregator import logger
from monolith.aggregator.exception import ServerError
from monolith.aggregator.plugins.utils import TastypieReader
from monolith.aggregator.util import date_range


class SolitudeReader(TastypieReader):

    def __init__(self, **options):
        super(SolitudeReader, self).__init__(**options)
        self.options = options
        self.endpoint = options['endpoint']
        self.type = options['type']
        self.filename_format = '%Y-%m-%d.revenue.log'

        keys = options['keys-file']
        if not os.path.exists(keys):
            raise ValueError('%r not found.' % keys)

        parser = ConfigParser()
        parser.read(keys)
        self.access_key = parser.get('auth', 'access_key', None)
        self.secret_key = parser.get('auth', 'secret_key', None)
        self.bucket_name = parser.get('auth', 'bucket', None)
        self.bucket = None

    def get_s3_file(self, date):
        """
        Connects to S3 bucket and looks for a file with name formatted like
        'YYYY-MM-DD.revenue.log'. If found, returns the content as a list of
        lists, one list of items per record.
        """
        if not self.bucket:
            conn = S3Connection(self.access_key, self.secret_key)
            self.bucket = conn.get_bucket(self.bucket_name)

        key = self.bucket.get_key(date.strftime(self.filename_format))
        content = []
        if key:
            # Remove any trailing newlines, split, and skip 1st row.
            rows = csv.reader(key.get_contents_as_string().splitlines())
            rows.next()  # Eat the 1st row, which is the column headers.
            for row in rows:
                content.append(row)

        return content

    def read_api(self, url, params=None, data=None):
        """Gets transaction data from Marketplace Transaction API.

        http://firefox-marketplace-api.readthedocs.org/en/latest/topics/transactions.html

        """
        if data is None:
            data = {}
        if not params:
            params = {}

        resp = self.session.get(url, params=params)

        if 400 <= resp.status_code <= 499:
            logger.error('API 4xx Error: {0} Url: {1}'.format(
                (resp.json()['detail'], url)))
            return None

        if 500 <= resp.status_code <= 599:
            logger.error('API 5xx Error: {0} Url: {1}'.format(
                (resp.text, url)))
            raise ServerError(resp.status_code)

        return resp.json()

    def extract(self, start_date, end_date):
        for current in date_range(start_date, end_date):
            # Results are keyed by app ID to perform a sum before inserting
            # into Monolith.
            results = defaultdict(list)

            content = self.get_s3_file(current)
            for line in content:
                uuid = line[1]
                source = line[9]

                # TODO: Handle in-app payments.
                if source == 'marketplace':
                    url = self.endpoint.replace(':transaction_id', uuid)
                    tx_data = self.read_api(url)
                    if tx_data:
                        results[tx_data['app_id']].append(
                            tx_data['amount_USD'])

            for app_id, prices in results.items():
                yield {'_date': current,
                       '_type': self.type,
                       'gross_revenue': sum(map(decimal.Decimal, prices)),
                       'app-id': app_id}
