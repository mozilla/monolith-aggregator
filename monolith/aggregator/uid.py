from base64 import urlsafe_b64encode
from calendar import timegm
from datetime import date, datetime
import random
import uuid


_randrange = random.Random().randrange
_node = uuid.getnode()


def urlsafe_uid(_date=None):
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
