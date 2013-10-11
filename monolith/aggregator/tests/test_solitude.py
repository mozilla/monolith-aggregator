import datetime
import decimal
import os
from unittest2 import TestCase

import mock
from nose.tools import eq_

from monolith.aggregator.plugins.solitude import SolitudeReader


class TestSolitudeReader(TestCase):

    def setUp(self, *args, **kwargs):
        super(TestSolitudeReader, self).setUp(*args, **kwargs)
        here = os.path.dirname(__file__)
        self.reader = SolitudeReader(**{
            'endpoint': 'http://testserver/:transaction_id/',
            'type': 'gross_revenue',
            'keys-file': '%s/aws_keys.ini' % here,
        })

        # Mock the calls to S3.
        get_s3_file = mock.Mock()
        get_s3_file.return_value = [
            # Columns: version, uuid, created, modified, amount, currency,
            #          status, buyer, seller, source.
            ['v2', 'webpay:a1b2c3d4-1234-5678-9a1b-2c3d4e5f678a',
             '2013-10-10T12:34:56', '2013-10-10T12:34:56', '0.89', 'EUR',
             '1', '', 'eb58ab59-3afa-4a67-a60d-457db9b8bc2f', 'marketplace'],
            ['v2', 'webpay:a1b2c3d4-1234-5678-9a1b-2c3d4e5f678b',
             '2013-10-10T12:34:56', '2013-10-10T12:34:56', '0.89', 'EUR',
             '1', '', 'eb58ab59-3afa-4a67-a60d-457db9b8bc2f', 'marketplace'],
        ]
        self.reader.get_s3_file = get_s3_file

        # Mock the calls to Marketplace API.
        read_api = mock.Mock()
        read_api.return_value = {
            u'amount_USD': u'1.99',
            u'app_id': 4321,
            u'id': u'webpay:a1b2c3d4-1234-5678-9a1b-2c3d4e5f678a',
            u'type': u'Purchase',
        }
        self.reader.read_api = read_api

    def test_extract(self):
        start_date = datetime.date(2013, 10, 10)
        end_date = datetime.date(2013, 10, 10)

        res = list(self.reader.extract(start_date, end_date))
        eq_(len(res), 1)
        eq_(res[0]['gross_revenue'], decimal.Decimal('3.98'))
        eq_(res[0]['app-id'], 4321)
