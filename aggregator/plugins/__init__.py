

class Plugin(object):
    def __init__(self, **options):
        self.options = options

    def __call__(self, *args):
        raise NotImplementedError


class _FuncPlugin(object):

    def __init__(self, **options):
        self.options = options

    def __call__(self, *args):
        return self.func(*args)


def plugin(func):
    def __func(self, *args, **kw):
        return func(*args, **kw)

    return type(func.__name__.upper(), (_FuncPlugin,), {'func': __func})
