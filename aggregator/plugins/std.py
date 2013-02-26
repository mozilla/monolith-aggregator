import sys


class Out(object):
    """Out."""
    def __init__(self, *args, **kw):
        pass

    def inject(self, batch, overwrite=False):
        for item in batch:
            print(item)

    def start_transaction(self):
        pass

    def commit_transaction(self):
        sys.stdout.flush()

    def rollback_transaction(self):
        pass
