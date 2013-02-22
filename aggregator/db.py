from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Binary, Date, Column
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BINARY
from sqlalchemy.sql import text

from aggregator.util import json_dumps, all_, urlsafe_uuid


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


def get_engine(sqluri, pool_size=100, pool_recycle=60, pool_timeout=30):
    extras = {}
    if not sqluri.startswith('sqlite'):
        extras['pool_size'] = pool_size
        extras['pool_timeout'] = pool_timeout
        extras['pool_recycle'] = pool_recycle

    return create_engine(sqluri, **extras)


class Database(object):

    def __init__(self, engine=None, sqluri=None, **params):
        self.engine = engine or get_engine(sqluri, **params)
        self.mysql = 'mysql' in self.engine.driver

        if self.mysql:
            # mysql specific settings
            # XXX you need to be SUPER user to do these calls.
            self.engine.execute("SET GLOBAL innodb_file_format='Barracuda'")
            self.engine.execute("SET GLOBAL innodb_file_per_table=1")

        record.metadata.bind = self.engine
        record.create(checkfirst=True)
        self.session_factory = sessionmaker(bind=self.engine)
        self.session = self.session_factory()
        self._transaction = None

    #
    # transaction management
    #
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

    def put(self, batch):
        if not self.in_transaction():
            self.start_transaction()
            explicit_transaction = True
        else:
            explicit_transaction = False

        session = self._transaction
        try:
            for source_id, item in batch:
                item = dict(item)
                date = item.pop('date')
                category = item.pop('category')
                session.add(Record(uid=urlsafe_uuid(date),
                                   date=date, category=category,
                                   value=json_dumps(item),
                                   source=source_id))
        except Exception:
            if explicit_transaction:
                self.rollback_transaction()
            raise
        else:
            if explicit_transaction:
                self.commit_transaction()

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
