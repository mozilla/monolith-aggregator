import datetime

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Binary, Date, Column
from sqlalchemy.types import BINARY
from sqlalchemy.sql import text

from monolith.aggregator.plugins import Plugin
from monolith.aggregator.util import json_dumps, json_loads
from monolith.aggregator.util import Transactional
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
        self.sqluri = options['database']
        Transactional.__init__(self, engine=None, sqluri=self.sqluri)

        if 'query' in options:
            # used as a read plugin
            self.query = text(options['query'])
            self.json_fields = [field.strip() for field in
                                options.get('json_fields', '').split(',')]
        else:
            # used as a write plugin
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

    def _check(self, data):
        data = dict(data)

        for field in self.json_fields:
            if field not in data:
                continue
            value = data[field]
            if isinstance(value, buffer):
                value = str(value)
            data.update(json_loads(value))
            del data[field]

        if self.mysql:
            return data

        # deal with sqlite returning buffers
        for key, value in data.items():
            if isinstance(value, buffer):
                data[key] = str(value)

        # cope with SQLite not having a date type
        for field in ('date', '_date'):
            if field in data:
                date = data[field]
                if isinstance(date, basestring):
                    data[field] = datetime.datetime.strptime(date, '%Y-%m-%d')

        return data

    def extract(self, start_date, end_date):
        query_params = {}
        unwanted = ('database', 'parser', 'here', 'query')

        for key, val in self.options.items():
            if key in unwanted:
                continue
            query_params[key] = val

        query_params['start_date'] = start_date
        query_params['end_date'] = end_date
        data = self.engine.execute(self.query, **query_params)
        return (self._check(line) for line in data)

    def get(self, start_date=None, end_date=None,
            type=None, source_id=None):
        with self.transaction() as session:
            query = session.query(Record)

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
