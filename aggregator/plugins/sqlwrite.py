from aggregator.db import Database


class SQLInjecter(object):
    """SQL"""

    def __init__(self, database, **options):
        self.db = Database(sqluri=database)

    def __call__(self, data, **options):
        self.db.put(category='', **data)
