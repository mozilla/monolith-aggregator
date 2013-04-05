from monolith.aggregator.db import Database
from monolith.aggregator.plugins import Plugin


class SQLInjecter(Plugin):

    def __init__(self, **options):
        super(SQLInjecter, self).__init__(**options)
        self.db = Database(sqluri=options['database'])

    def inject(self, batch):
        self.db.put(batch)

    def clear(self, start_date, end_date, source_ids):
        self.db.clear(start_date, end_date, source_ids)

    def start_transaction(self):
        self.db.start_transaction()

    def commit_transaction(self):
        self.db.commit_transaction()

    def rollback_transaction(self):
        self.db.rollback_transaction()
