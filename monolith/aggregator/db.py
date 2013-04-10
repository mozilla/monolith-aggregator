import datetime

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Binary, Date, Column, Integer
from sqlalchemy.types import BINARY
from sqlalchemy.sql import text

from monolith.aggregator.plugins import Plugin
from monolith.aggregator.util import json_dumps, json_loads
from monolith.aggregator.util import Transactional
from monolith.aggregator.uid import urlsafe_uid

_Model = declarative_base()


def today():
    return datetime.datetime.utcnow().date()


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


class Transaction(_Model):
    __tablename__ = 'monolith_transaction'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, default=today(), nullable=False)
    source = Column(String(256), nullable=False)


record_table = Record.__table__
transaction_table = Transaction.__table__

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

        value = data['value']
        if isinstance(value, buffer):
            value = str(value)
        data.update(json_loads(value))
        del data['value']

        if self.mysql:
            return data

        # deal with sqlite returning buffers
        for key, value in data.items():
            if isinstance(value, buffer):
                data[key] = str(value)

        # cope with SQLite not having a date type
        date = data['date']
        if isinstance(date, basestring):
            data['date'] = datetime.datetime.strptime(date, '%Y-%m-%d')

        return data

    def extract(self, start_date, end_date):
        query = text(
            'select id AS _id, type AS _type, source_id, date, value '
            'from record where date BETWEEN :start_date and :end_date'
        )
        data = self.engine.execute(query,
            start_date=start_date, end_date=end_date)
        return (self._check(line) for line in data)

    def clear(self, start_date, end_date, source_ids):
        with self.transaction() as session:
            query = session.query(Record).filter(
                Record.source_id.in_(source_ids)).filter(
                    Record.date >= start_date).filter(
                        Record.date <= end_date)
            count = query.delete(synchronize_session=False)
        return count


class History(Transactional):

    def __init__(self, engine=None, sqluri=None, **params):
        super(History, self).__init__(engine, sqluri, **params)
        transaction_table.metadata.bind = self.engine
        transaction_table.create(checkfirst=True)

    def add_entry(self, sources, start_date, end_date=None, num=0):
        with self.transaction() as session:
            if end_date is None:
                drange = (start_date,)
            else:
                day_count = (end_date - start_date).days
                if day_count == 0:
                    drange = [start_date]
                else:
                    drange = (start_date + datetime.timedelta(n)
                              for n in range(day_count))

            for date in drange:
                for source in sources:
                    session.add(Transaction(source=source.get_id(),
                                            date=date))

    def exists(self, source, start_date, end_date):
        query = self.session.query(Transaction)
        query = query.filter(Transaction.source == source.get_id())
        query = query.filter(Transaction.date >= start_date)
        query = query.filter(Transaction.date <= end_date)
        return query.first() is not None
