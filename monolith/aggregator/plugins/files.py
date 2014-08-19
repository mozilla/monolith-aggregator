from monolith.aggregator.plugins import Plugin
from monolith.aggregator.util import json_dumps


class FileWriter(Plugin):

    def __init__(self, **options):
        self._filename = options['filename']
        self._file = open(self._filename, 'w+')

    def inject(self, batch):
        for source, data in batch:
            self._file.write('%s\n' % json_dumps(data))
