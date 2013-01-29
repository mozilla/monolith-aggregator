from aggregator.db import Database


class SQLInjecter(object):
    """SQL"""

    def __init__(self, **options):
        self.db = Database(sqluri=options['database'])

    def __call__(self, batch):
        for data in batch:
            self.db.put(category='', **data)
