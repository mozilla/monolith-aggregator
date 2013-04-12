import sys
from monolith.aggregator.plugins import Plugin


class Out(Plugin):
    """Out."""
    def inject(self, batch):
        for item in batch:
            print(item)

    def commit_transaction(self):
        sys.stdout.flush()
