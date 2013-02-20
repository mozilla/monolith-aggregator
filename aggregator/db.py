from datetime import datetime
from uuid import uuid1

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Binary, DateTime, Column
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BINARY

from aggregator.util import json_dumps, all_


_Model = declarative_base()


class Record(_Model):
    __tablename__ = 'record'

    uid = Column(BINARY(32), primary_key=True)
    date = Column(DateTime, default=datetime.now())
    category = Column(String(256), nullable=False)
    value = Column(Binary)


record = Record.__table__

PUT_QUERY = """\
insert into record
    (uid, date, category, value)
values
    (:uid, :date, :category, :value)
"""


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
        record.metadata.bind = self.engine
        record.create(checkfirst=True)
        self.session_factory = sessionmaker(bind=self.engine)
        self.session = self.session_factory()

    def put(self, uid=None, category="unknown", date=None, **data):
        if uid is None:
            uid = uuid1().hex
        if date is None:
            date = datetime.now()

        data.setdefault('date', date)

        # store in db
        # XXX try..except etc
        self.engine.execute(PUT_QUERY, uid=uid, date=date, category=category,
                            value=json_dumps(data))

    def put_batch(self, batch):
        session = self.session_factory()
        now = datetime.now()
        for item in batch:
            item = dict(item)
            uid = item.pop('uid', uuid1().hex)
            date = item.pop('date', now)
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
