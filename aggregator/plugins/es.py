from pyelasticsearch import ElasticSearch

from aggregator.plugins import Plugin


class ESSetup(object):

    def __init__(self, client):
        self.client = client

    def create_index(self, name):
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 1,
            'refresh_interval': '60s',
        }
        self.client.create_index(name, settings=settings)


class ESWrite(Plugin):

    def __init__(self, **options):
        self.options = options
        self.url = options['url']
        self.client = ElasticSearch(self.url)
        self.setup = ESSetup(self.client)

    def __call__(self, data, **options):
        self.setup.create_index('monolith_2013-01')
        return self.client.index(
            'monolith_2013-01', 'downloads', data)
