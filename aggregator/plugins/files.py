from aggregator.plugins import Plugin
from aggregator.util import json_dumps


class FileWriter(Plugin):

    def __init__(self, **options):
        self._filename = options['filename']
        self._file = open(self._filename, 'w+')

    def inject(self, batch):
        for data in batch:
            self._file.write(json_dumps(data))
