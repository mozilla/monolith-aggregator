from pyelasticsearch import ElasticSearch

from aggregator.plugins import Plugin


class ESSetup(object):

    def __init__(self, client):
        self.client = client

    def create_index(self, name):
        """Create an index with our custom settings.
        """
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 1,
            'refresh_interval': '10s',
            'analysis': {
                'analyzer': {
                    'default': {
                        'type': 'custom',
                        'tokenizer': 'keyword',
                    }
                },
            },
        }
        return self.client.create_index(name, settings=settings)

    def optimize(self, name):
        """Fully optimize an index down to one segment.
        """
        return self.client.optimize(
            name, max_num_segments=1, wait_for_merge=True)


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
