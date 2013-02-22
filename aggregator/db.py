import datetime

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Binary, Date, Column
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BINARY
from sqlalchemy.sql import text

from aggregator.util import json_dumps, all_, urlsafe_uuid


_Model = declarative_base()


def today():
    return datetime.date.today()


class Record(_Model):
    __tablename__ = 'record'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
        'mysql_row_format': 'compressed',
        'mysql_key_block_size': '4',
    }

    uid = Column(BINARY(24), primary_key=True)
    date = Column(Date, default=today(), nullable=False)
    category = Column(String(256), nullable=False)
    value = Column(Binary)


record = Record.__table__

PUT_QUERY = text("""\
insert into record
    (uid, date, category, value)
values
    (:uid, :date, :category, :value)
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

    def put(self, uid=None, category="unknown", date=None, **data):
        if date is None:
            date = today()
        if uid is None:
            uid = urlsafe_uuid(date)

        data.setdefault('date', date)

        # store in db
        # XXX try..except etc
        self.engine.execute(PUT_QUERY, uid=uid, date=date, category=category,
                            value=json_dumps(data))

    def put_batch(self, batch):
        session = self.session_factory()
        now = today()
        for source_id, item in batch:
            # XXX use source_id as a key with dates for updates
            item = dict(item)
            date = item.pop('date', now)
            uid = item.pop('uid', urlsafe_uuid(date))
            category = item.pop('category', 'unknown')
            session.add(Record(uid=uid, date=date, category=category,
                               value=json_dumps(item)))
        session.commit()

    def get(self, category=None, start_date=None, end_date=None):
        if all_((category, start_date, end_date), None):
            raise ValueError("You need to filter something")

        query = self.session.query(Record)

        if category is not None:
            query = query.filter(Record.category == category)

        if start_date is not None:
            query = query.filter(Record.date >= start_date)

        if end_date is not None:
            query = query.filter(Record.date <= end_date)

        return query
