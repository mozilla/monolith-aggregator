from aggregator.db import Database


class SQLInjecter(object):
    """SQL"""

    def __init__(self, **options):
        self.db = Database(sqluri=options['database'])

    def __call__(self, data, **options):
        self.db.put(category='', **data)
