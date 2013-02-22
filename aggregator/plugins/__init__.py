import sys
from hashlib import md5

from aggregator.util import json_dumps


class Plugin(object):
    def __init__(self, **options):
        self.options = options
        self._digest = None

    def __call__(self, *args):
        raise NotImplementedError

    def digest(self):
        if self._digest is not None:
            return self._digest

        unwanted = ('parser', 'here')
        digest = md5()
        items = self.options.items()
        items.sort()
        for key, value in items:
            if key in unwanted:
                continue
            digest.update(value)

        self._digest = digest.hexdigest()
        return self._digest


class _FuncPlugin(Plugin):
    def __call__(self, *args):
        return self.func(*args)


def plugin(func):
    def __func(self, *args, **kw):
        return func(*args, **kw)

    return type(func.__name__.upper(), (_FuncPlugin,), {'func': __func})


@plugin
def stdout(batch):
    for data in batch:
        sys.stdout.write(json_dumps(data))
        sys.stdout.flush()
