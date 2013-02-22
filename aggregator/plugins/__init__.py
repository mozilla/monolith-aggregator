import sys
from aggregator.util import json_dumps


class Plugin(object):
    def __init__(self, **options):
        self.options = options

    def extract(self, start_date, end_date):
        raise NotImplementedError(self)

    def inject(self, batch):
        raise NotImplementedError(self)

    def purge(self, *args):
        raise NotImplementedError(self)

    def get_id(self):
        return self.options['id']


class _FuncPlugin(Plugin):
    def extract(self, *args):
        return self.func(*args)
    inject = extract


def extract(func):
    def __func(self, *args, **kw):
        return func(*args, **kw)

    return type(func.__name__.upper(), (_FuncPlugin,), {'func': __func})


def inject(func):
    def __func(self, *args, **kw):
        return func(*args, **kw)

    return type(func.__name__.upper(), (_FuncPlugin,), {'func': __func})


@inject
def stdout(batch):
    for data in batch:
        sys.stdout.write(json_dumps(data))
        sys.stdout.flush()
