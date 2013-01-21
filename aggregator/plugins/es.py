import json

from pyes import ES

from aggregator.plugins import Plugin


class ESWrite(Plugin):

    def __init__(self, **options):
        self.options = options
        self.url = options['url']
        self.client = ES(self.url)

    def __call__(self, data, **options):
        return self.client.index(
            json.dumps(data), 'monolith_2013-01', 'downloads')
