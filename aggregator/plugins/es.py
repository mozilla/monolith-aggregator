from pyelasticsearch import ElasticSearch

from aggregator.plugins import Plugin


class ESWrite(Plugin):

    def __init__(self, **options):
        self.options = options
        self.url = options['url']
        self.client = ElasticSearch(self.url)

    def __call__(self, data, **options):
        return self.client.index(
            'monolith_2013-01', 'downloads', data)
