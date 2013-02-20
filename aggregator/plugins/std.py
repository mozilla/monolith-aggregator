
class Out(object):
    """Out."""
    def __init__(self, *args, **kw):
        pass

    def __call__(self, batch):
        for item in batch:
            print(item)
