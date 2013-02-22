import datetime
from unittest2 import TestCase

from aggregator.plugins.randomizer import RandomGenerator


class TestRandomGenerator(TestCase):

    def test_lenght_is_correct(self):
        start_date = datetime.datetime(year=2013, month=01, day=28)
        end_date = datetime.datetime(year=2013, month=02, day=28)

        gen = RandomGenerator(addons=1)
        self.assertEquals(len(list(gen.extract(start_date, end_date))),
                          (end_date - start_date).days)
