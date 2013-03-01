from monolith.aggregator.db import Database


class SQLInjecter(object):
    """SQL"""

    def __init__(self, **options):
        self.db = Database(sqluri=options['database'])

    def inject(self, batch, overwrite=False):
        self.db.put(batch, overwrite)

    def start_transaction(self):
        self.db.start_transaction()

    def commit_transaction(self):
        self.db.commit_transaction()

    def rollback_transaction(self):
        self.db.rollback_transaction()
