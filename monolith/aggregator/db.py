from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Binary, Date, Column
from sqlalchemy.types import BINARY
from sqlalchemy.sql import text

from monolith.aggregator.util import json_dumps, all_, Transactional
from monolith.aggregator.uid import urlsafe_uid


_Model = declarative_base()


class Record(_Model):
    __tablename__ = 'record'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
        'mysql_row_format': 'compressed',
        'mysql_key_block_size': '4',
    }

    id = Column(BINARY(24), primary_key=True)
    date = Column(Date, nullable=False)
    type = Column(String(256), nullable=False)
    source_id = Column(String(32), nullable=False)
    value = Column(Binary)


record = Record.__table__

PUT_QUERY = text("""\
insert into record
    (id, date, type, source_id, value)
values
    (:id, :date, :type, :source_id, :value)
""")


class Database(Transactional):

    def __init__(self, engine=None, sqluri=None, **params):
        super(Database, self).__init__(engine, sqluri, **params)
        record.metadata.bind = self.engine
        record.create(checkfirst=True)

    def put(self, batch, overwrite=False):

        with self.transaction() as session:
            # XXX the real solution here is to implement a
            # low-level ON DUPLICATE KEY UPDATE call
            # but for now we'll use merge() and move to
            # ON DUPLICATE KEY UPDATE if this becomes a
            # bottleneck
            if overwrite:
                add = session.merge
            else:
                add = session.add

            for source_id, item in batch:
                item = dict(item)
                date = item.pop('_date')
                type = item.pop('_type')
                add(Record(id=urlsafe_uid(date),
                           date=date, type=type,
                           source_id=source_id,
                           value=json_dumps(item)))

    def get(self, start_date=None, end_date=None,
            type=None, source_id=None):
        if all_((start_date, end_date, type, source_id), None):
            raise ValueError("You need to filter something")

        query = self.session.query(Record)

        if start_date is not None:
            query = query.filter(Record.date >= start_date)

        if end_date is not None:
            query = query.filter(Record.date <= end_date)

        if type is not None:
            query = query.filter(Record.type == type)

        if source_id is not None:
            query = query.filter(Record.source_id == source_id)

        return query
