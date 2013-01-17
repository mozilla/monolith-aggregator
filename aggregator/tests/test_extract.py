import os
from unittest2 import TestCase
from aggregator.extract import extract


class TestExtract(TestCase):

    def setUp(self):
        self.config = os.path.join(os.path.dirname(__file__), 'config.ini')

    def test_extract(self):
        extract(self.config)
