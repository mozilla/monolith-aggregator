import json
from base64 import urlsafe_b64encode
from time import mktime
import random
import logging
import fcntl
from datetime import date, datetime, timedelta
from calendar import monthrange, timegm
import uuid
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def json_dumps(obj):
    return json.dumps(obj, cls=JSONEncoder)


class JSONEncoder(json.JSONEncoder):
    """A JSON encoder takking care of dates"""

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return int(mktime(obj.timetuple()))

        return json.JSONEncoder.default(self, obj)


def all_(iterable, value):
    """like the builtin all, but compares to the value you pass"""
    for element in iterable:
        if element is not value:
            return False
    return True


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

_randrange = random.Random().randrange
_node = uuid.getnode()


def urlsafe_uuid(_date=None):
    """
    A simplified version of uuid1 - optimized for usage as a
    MySQL primary key and ElasticSearch id.
    """
    if _date is None:
        _date = date.today()
    # take the passed in date, and add the current time to it
    # that way we preserve the right coarse-grained date, but also
    # make duplicates less likely by having a non-constant time part
    now = datetime.utcnow()
    timestamp = timegm(_date.timetuple()[:3] + now.timetuple()[3:])
    timestamp = int((timestamp * 1e6) + now.microsecond)
    timestamp = (timestamp * 10) + 0x01b21dd213814000L

    time_low = timestamp & 0xffffffffL  # changes every 100 nano-seconds
    time_mid = (timestamp >> 32L) & 0xffffL  # changes every ~15 minutes
    time_hi = (timestamp >> 48L) & 0x0fffL  # changes every ~3 years
    clock_seq = _randrange(1 << 14L)

    # order so we get a stable prefix per node and time, which helps
    # data locality in BTree inserts
    int_ = ((_node << 80L) | (time_hi << 64L) | (time_mid << 48L) |
            (time_low << 16L) | clock_seq)

    bytes = ''
    for shift in range(0, 128, 8):
        bytes = chr((int_ >> shift) & 0xff) + bytes

    return urlsafe_b64encode(bytes)


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
        if self.mysql:
            # mysql specific settings
            # XXX you need to be SUPER user to do these calls.
            self.engine.execute("SET GLOBAL innodb_file_format='Barracuda'")
            self.engine.execute("SET GLOBAL innodb_file_per_table=1")
        self.session_factory = sessionmaker(bind=self.engine)
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
