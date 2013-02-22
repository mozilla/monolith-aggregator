from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Binary, Date, Column
from sqlalchemy.types import BINARY
from sqlalchemy.sql import text

from aggregator.util import json_dumps, all_, urlsafe_uuid, Transactional


_Model = declarative_base()


class Record(_Model):
    __tablename__ = 'record'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
        'mysql_row_format': 'compressed',
        'mysql_key_block_size': '4',
    }

    uid = Column(BINARY(24), primary_key=True)
    date = Column(Date, nullable=False)
    category = Column(String(256), nullable=False)
    value = Column(Binary)
    source = Column(String(32), nullable=False)


record = Record.__table__

PUT_QUERY = text("""\
insert into record
    (uid, date, category, value, source)
values
    (:uid, :date, :category, :value, :source)
""")


class Database(Transactional):

    def __init__(self, engine=None, sqluri=None, **params):
        super(Database, self).__init__(engine, sqluri, **params)
        record.metadata.bind = self.engine
        record.create(checkfirst=True)

    def put(self, batch):
        with self.transaction() as session:
            for source_id, item in batch:
                item = dict(item)
                # remove date / category from the item dump,
                # fail with KeyError if they aren't present
                date = item.pop('date')
                category = item.pop('category')
                session.add(Record(uid=urlsafe_uuid(date),
                                   date=date, category=category,
                                   value=json_dumps(item),
                                   source=source_id))

    def get(self, category=None, start_date=None, end_date=None,
            source_id=None):
        if all_((category, start_date, end_date, source_id), None):
            raise ValueError("You need to filter something")

        query = self.session.query(Record)

        if category is not None:
            query = query.filter(Record.category == category)

        if start_date is not None:
            query = query.filter(Record.date >= start_date)

        if end_date is not None:
            query = query.filter(Record.date <= end_date)

        if source_id is not None:
            query = query.filter(Record.source_id == source_id)

        return query
