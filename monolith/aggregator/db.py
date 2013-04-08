from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Binary, Date, Column
from sqlalchemy.types import BINARY
from sqlalchemy.sql import text

from monolith.aggregator.plugins import Plugin
from monolith.aggregator.util import json_dumps, Transactional
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


record_table = Record.__table__

PUT_QUERY = text("""\
insert into record
    (id, date, type, source_id, value)
values
    (:id, :date, :type, :source_id, :value)
""")


class Database(Transactional, Plugin):

    def __init__(self, **options):
        Plugin.__init__(self, **options)
        Transactional.__init__(self, engine=None, sqluri=options['database'])
        record_table.metadata.bind = self.engine
        record_table.create(checkfirst=True)

    def inject(self, batch):
        with self.transaction() as session:
            records = []
            for source_id, item in batch:
                item = dict(item)
                date = item.pop('_date')
                type = item.pop('_type')
                records.append(
                    Record(id=urlsafe_uid(date),
                           date=date, type=type,
                           source_id=source_id,
                           value=json_dumps(item)))
            session.add_all(records)

    def get(self, start_date=None, end_date=None,
            type=None, source_id=None):
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

    def clear(self, start_date, end_date, source_ids):
        with self.transaction() as session:
            query = session.query(Record).filter(
                Record.source_id.in_(source_ids)).filter(
                    Record.date >= start_date).filter(
                        Record.date <= end_date)
            count = query.delete(synchronize_session=False)
        return count
