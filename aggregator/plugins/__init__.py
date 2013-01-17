from functools import wraps


class Plugin(object):
    def __init__(self, **options):
        self.options = options

    def __call__(self, *args, **options):
        raise NotImplementerError


class _FuncPlugin(object):

    def __init__(self, **options):
        self.options = options

    def __call__(self, *args, **options):
        return self.func(*args, **options)


def plugin(func):
    def __func(self, *args, **kw):
        return func(*args, **kw)

    return type(func.__name__.upper(), (_FuncPlugin,), {'func': __func})
