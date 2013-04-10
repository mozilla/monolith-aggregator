import datetime

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Date, Column, Integer

from monolith.aggregator.util import Transactional


_Model = declarative_base()


def today():
    return datetime.datetime.utcnow().date()


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


class History(Transactional):

    def __init__(self, engine=None, sqluri=None, **params):
        super(History, self).__init__(engine, sqluri, **params)
        transaction.metadata.bind = self.engine
        transaction.create(checkfirst=True)

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
