from unittest2 import TestCase

from aggregator.util import all_


class TestUtils(TestCase):

    def test_all(self):
        self.assertFalse(all_([1, 2, 3], 1))
        self.assertTrue(all_((1, 1, 1), 1))
