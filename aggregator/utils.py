import json
from time import mktime
import datetime


def json_dumps(obj):
    return json.dumps(obj, cls=JSONEncoder)


class JSONEncoder(json.JSONEncoder):
    """A JSON encoder takking care of dates"""

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return int(mktime(obj.timetuple()))

        return json.JSONEncoder.default(self, obj)


def all_(iterable, value):
    """like the builtin all, but compares to the value you pass"""
    for element in iterable:
        if element is not value:
            return False
    return True

