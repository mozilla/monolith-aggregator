import json
from time import mktime
from time import time
import random
import sys
import logging
import fcntl
from datetime import date, datetime, timedelta
from calendar import monthrange
import uuid

try:
    from importlib import import_module         # NOQA
except ImportError:
    def _resolve_name(name, package, level):
        """Returns the absolute name of the module to be imported. """
        if not hasattr(package, 'rindex'):
            raise ValueError("'package' not set to a string")
        dot = len(package)
        for x in xrange(level, 1, -1):
            try:
                dot = package.rindex('.', 0, dot)
            except ValueError:
                raise ValueError("attempted relative import beyond top-level "
                                 "package")
        return "%s.%s" % (package[:dot], name)

    def import_module(name, package=None):      # NOQA
        """Import a module.
        The 'package' argument is required when performing a relative import.
        It specifies the package to use as the anchor point from which to
        resolve the relative import to an absolute import."""
        if name.startswith('.'):
            if not package:
                raise TypeError("relative imports require the 'package' "
                                "argument")
            level = 0
            for character in name:
                if character != '.':
                    break
                level += 1
            name = _resolve_name(name[level:], package, level)
        __import__(name)
        return sys.modules[name]


# taken from werkzeug
class ImportStringError(ImportError):
    """Provides information about a failed :func:`import_string` attempt."""

    #: String in dotted notation that failed to be imported.
    import_name = None
    #: Wrapped exception.
    exception = None

    def __init__(self, import_name, exception):
        self.import_name = import_name
        self.exception = exception

        msg = (
            'import_string() failed for %r. Possible reasons are:\n\n'
            '- missing __init__.py in a package;\n'
            '- package or module path not included in sys.path;\n'
            '- duplicated package or module name taking precedence in '
            'sys.path;\n'
            '- missing module, class, function or variable;\n\n'
            'Debugged import:\n\n%s\n\n'
            'Original exception:\n\n%s: %s')

        name = ''
        tracked = []
        for part in import_name.replace(':', '.').split('.'):
            name += (name and '.') + part
            imported = resolve_name(name, silent=True)
            if imported:
                tracked.append((name, getattr(imported, '__file__', None)))
            else:
                track = ['- %r found in %r.' % (n, i) for n, i in tracked]
                track.append('- %r not found.' % name)
                msg = msg % (import_name, '\n'.join(track),
                             exception.__class__.__name__, str(exception))
                break

        ImportError.__init__(self, msg)

    def __repr__(self):
        return '<%s(%r, %r)>' % (self.__class__.__name__, self.import_name,
                                 self.exception)


def resolve_name(import_name, silent=False):
    """Imports an object based on a string.  This is useful if you want to
    use import paths as endpoints or something similar.  An import path can
    be specified either in dotted notation (``xml.sax.saxutils.escape``)
    or with a colon as object delimiter (``xml.sax.saxutils:escape``).

    If `silent` is True the return value will be `None` if the import fails.

    For better debugging we recommend the new :func:`import_module`
    function to be used instead.

    :param import_name: the dotted name for the object to import.
    :param silent: if set to `True` import errors are ignored and
                   `None` is returned instead.
    :return: imported object
    """
    # force the import name to automatically convert to strings
    if isinstance(import_name, unicode):
        import_name = str(import_name)
    try:
        if ':' in import_name:
            module, obj = import_name.split(':', 1)
        elif '.' in import_name:
            module, obj = import_name.rsplit('.', 1)
        else:
            return __import__(import_name)
            # __import__ is not able to handle unicode strings in the fromlist
        # if the module is a package
        if isinstance(obj, unicode):
            obj = obj.encode('utf-8')
        try:
            return getattr(__import__(module, None, None, [obj]), obj)
        except (ImportError, AttributeError):
            # support importing modules not yet set up by the parent module
            # (or package for that matter)
            modname = module + '.' + obj
            __import__(modname)
            return sys.modules[modname]
    except ImportError, e:
        if not silent:
            raise ImportStringError(import_name, e), None, sys.exc_info()[2]


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

_last_timestamp = 0
_randrange = random.Random().randrange
_node = uuid.getnode()


def urlsafe_uuid():
    """
    A simplified version of uuid1 - optimized for usage as a
    MySQL primary key and ElasticSearch id.
    """
    global _last_timestamp
    timestamp = int(time() * 1e7) + 0x01b21dd213814000L
    if timestamp <= _last_timestamp:
        timestamp = _last_timestamp + 1
    _last_timestamp = timestamp

    time_low = timestamp & 0xffffffffL
    time_mid = (timestamp >> 32L) & 0xffffL
    time_hi_version = (timestamp >> 48L) & 0x0fffL
    clock_seq = _randrange(1 << 14L)

    int_ = ((time_low << 96L) | (time_mid << 80L) |
            (time_hi_version << 64L) | (clock_seq << 48L) | _node)

    return '%032x' % int_
