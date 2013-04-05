from calendar import monthrange
from contextlib import contextmanager
from datetime import date, datetime, timedelta
import fcntl
import json
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

try:
    import simplejson as json
except ImportError:
    import json


def encode_date(obj):
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%dT%H:%M:%S.%f')
    elif isinstance(obj, date):
        return obj.strftime('%Y-%m-%d')
    raise TypeError(repr(obj) + " is not JSON serializable")


def json_loads(obj):
    return json.loads(obj)


def json_dumps(obj):
    return json.dumps(obj, default=encode_date)


LOG_LEVELS = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG}

LOG_FMT = r"%(asctime)s [%(process)d] [%(levelname)s] %(message)s"
LOG_DATE_FMT = r"%Y-%m-%d %H:%M:%S"


def close_on_exec(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    flags |= fcntl.FD_CLOEXEC
    fcntl.fcntl(fd, fcntl.F_SETFD, flags)


def configure_logger(logger, level='INFO', output="-"):
    loglevel = LOG_LEVELS.get(level.lower(), logging.INFO)
    logger.setLevel(loglevel)
    if output == "-":
        h = logging.StreamHandler()
    else:
        h = logging.FileHandler(output)
        close_on_exec(h.stream.fileno())
    fmt = logging.Formatter(LOG_FMT, LOG_DATE_FMT)
    h.setFormatter(fmt)
    logger.addHandler(h)


def word2daterange(datestr):
    """Returns a range of dates (tuple of two dates) given a "word".

    Implemented words:

    - today
    - yesterday
    - last-week
    - last-month
    - last-year
    """
    today = date.today()

    if datestr == 'today':
        return today, today
    elif datestr == 'yesterday':
        yesterday = today - timedelta(days=1)
        return yesterday, yesterday
    elif datestr == 'last-week':
        first_day = today - timedelta(days=today.weekday() + 7)
        last_day = first_day + timedelta(days=7)
        return first_day, last_day
    elif datestr == 'last-month':
        # getting the first day of previous month
        current_month = today.month
        if current_month == 1:
            month = 12
            year = today.year - 1
        else:
            month = current_month - 1
            year = today.year

        last_day_of_month = monthrange(year, month)[1]
        return date(year, month, 1), date(year, month, last_day_of_month)
    elif datestr == 'last-year':
        year = today.year - 1
        last_day_of_month = monthrange(year, 12)[1]
        return date(year, 1, 1), date(year, 12, last_day_of_month)

    raise NotImplementedError(datestr)


def get_engine(sqluri, pool_size=100, pool_recycle=60, pool_timeout=30):
    extras = {}
    if not sqluri.startswith('sqlite'):
        extras['pool_size'] = pool_size
        extras['pool_timeout'] = pool_timeout
        extras['pool_recycle'] = pool_recycle

    return create_engine(sqluri, **extras)


class Transactional(object):
    def __init__(self, engine=None, sqluri=None, **params):
        self.engine = engine or get_engine(sqluri, **params)
        self.mysql = 'mysql' in self.engine.driver
        self.session_factory = sessionmaker(bind=self.engine, autocommit=False,
                                            autoflush=False)
        self.session = self.session_factory()
        self._transaction = None

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
