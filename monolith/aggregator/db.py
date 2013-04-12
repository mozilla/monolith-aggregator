from contextlib import contextmanager
import datetime

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import String, Binary, Date, Column, Integer
from sqlalchemy.types import BINARY
from sqlalchemy.sql import text

from monolith.aggregator.plugins import Plugin
from monolith.aggregator.util import json_dumps, json_loads, date_range
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


class Transaction(_Model):
    __tablename__ = 'monolith_transaction'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    source = Column(String(256), nullable=False)


record_table = Record.__table__
transaction_table = Transaction.__table__


def get_engine(sqluri, pool_size=100, pool_recycle=60, pool_timeout=30):
    extras = {}
    if not sqluri.startswith('sqlite'):
        extras['pool_size'] = pool_size
        extras['pool_timeout'] = pool_timeout
        extras['pool_recycle'] = pool_recycle

    return create_engine(sqluri, **extras)


class Database(Plugin):

    def __init__(self, **options):
        Plugin.__init__(self, **options)
        self.sqluri = options['database']
        self.engine = get_engine(self.sqluri)
        self.mysql = 'mysql' in self.engine.driver
        self.session_factory = sessionmaker(bind=self.engine, autocommit=False,
                                            autoflush=False)
        self.session = self.session_factory()
        self._transaction = None

        record_table.metadata.bind = self.engine
        record_table.create(checkfirst=True)
        transaction_table.metadata.bind = self.engine
        transaction_table.create(checkfirst=True)

    @contextmanager
    def transaction(self):
        if not self.in_transaction():
            self.start_transaction()
            explicit_transaction = True
        else:
            explicit_transaction = False

        try:
            yield self._transaction
        except Exception:
            if explicit_transaction:
                self.rollback_transaction()
            raise
        else:
            if explicit_transaction:
                self.commit_transaction()

    def start_transaction(self):
        if self._transaction is not None:
            raise ValueError('A transaction is already running')
        self._transaction = self.session_factory()

    def commit_transaction(self):
        try:
            self._transaction.commit()
        finally:
            self._transaction = None

    def rollback_transaction(self):
        try:
            self._transaction.rollback()
        finally:
            self._transaction = None

    def in_transaction(self):
        return self._transaction is not None

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
        count = 0
        with self.transaction() as session:
            query = session.query(Record).filter(
                Record.source_id.in_(source_ids)).filter(
                    Record.date >= start_date).filter(
                        Record.date <= end_date)
            count = query.delete(synchronize_session=False)
        return count

    def add_entry(self, sources, start_date, end_date=None, num=0):
        with self.transaction() as session:
            if end_date is None:
                drange = (start_date,)
            else:
                drange = date_range(start_date, end_date)

            for date in drange:
                for source in sources:
                    session.add(Transaction(source=source.get_id(),
                                            date=date))

    def exists(self, source, start_date, end_date):
        count = 0
        with self.transaction() as session:
            query = session.query(Transaction)
            query = query.filter(Transaction.source == source.get_id())
            query = query.filter(Transaction.date >= start_date)
            query = query.filter(Transaction.date <= end_date)
            count = query.count()
        return count > 0
