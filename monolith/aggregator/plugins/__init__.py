class Plugin(object):
    def __init__(self, **options):
        self.options = options

    def extract(self, start_date, end_date):
        raise NotImplementedError(self)

    def inject(self, batch):
        raise NotImplementedError(self)

    def clear(self, start_date, end_date, source_ids):
        pass

    def purge(self, *args):
        pass

    def get_id(self):
        return self.options['id']

    def start_transaction(self):
        pass

    def commit_transaction(self):
        pass

    def rollback_transaction(self):
        pass


class _FuncPlugin(Plugin):
    def extract(self, *args, **kw):
        return self.func(*args, **kw)


def extract(func):
    def __func(self, *args, **kw):
        return func(*args, **kw)

    return type(func.__name__.upper(), (_FuncPlugin,), {'func': __func})
