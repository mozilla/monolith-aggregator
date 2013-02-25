

class Out(object):
    """Out."""
    def __init__(self, *args, **kw):
        pass

    def inject(self, batch, overwrite=False):
        for item in batch:
            print(item)
