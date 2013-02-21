import datetime
from hashlib import md5

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Date, Column, Integer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


_Model = declarative_base()


def today():
    return datetime.date.today()


class Transaction(_Model):
    __tablename__ = 'monolith_transaction'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, default=today(), nullable=False)
    source = Column(String(256), nullable=False)


transaction = Transaction.__table__


PUT_QUERY = """\
insert into monolith_transaction
    (date, source)
values
    (:date, :source)
"""


def get_engine(sqluri, pool_size=100, pool_recycle=60, pool_timeout=30):
    extras = {}
    if not sqluri.startswith('sqlite'):
        extras['pool_size'] = pool_size
        extras['pool_timeout'] = pool_timeout
        extras['pool_recycle'] = pool_recycle

    return create_engine(sqluri, **extras)


class History(object):

    def __init__(self, engine=None, sqluri=None, **params):
        self.engine = engine or get_engine(sqluri, **params)
        transaction.metadata.bind = self.engine
        transaction.create(checkfirst=True)
        self.session_factory = sessionmaker(bind=self.engine)
        self.session = self.session_factory()

    def _get_digest(self, source):
        unwanted = ('parser', 'here')
        digest = md5()
        items = source.options.items()
        items.sort()

        for key, value in items:
            if key in unwanted:
                continue
            digest.update(value)

        return digest.hexdigest()

    def add_entry(self, sources, start_date, end_date=None):
        if end_date is None:
            drange = (start_date,)
        else:
            day_count = (end_date - start_date).days
            drange = (start_date + datetime.timedelta(n)
                      for n in range(day_count))

        # XXX single batch query?
        for date in drange:
            for source in sources:
                # store in db
                # XXX try..except etc
                self.engine.execute(PUT_QUERY, date=date,
                                    source=self._get_digest(source))

    def exists(self, source, start_date, end_date):
        query = self.session.query(Transaction)
        query = query.filter(Transaction.source == self._get_digest(source))
        query = query.filter(Transaction.date >= start_date)
        query = query.filter(Transaction.date <= end_date)
        return query.first() is not None
