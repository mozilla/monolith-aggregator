from pyelasticsearch import ElasticSearch

from aggregator.plugins import Plugin


class ExtendedClient(ElasticSearch):
    """Wrapper around pyelasticsearch's client to add some missing
    API's. These should be merged upstream.
    """

    def create_template(self, name, settings):
        return self.send_request('PUT', ['_template', name], settings)

    def delete_template(self, name):
        return self.send_request('DELETE', ['_template', name])

    def get_template(self, name):
        return self.send_request('GET', ['_template', name])


class ESSetup(object):

    def __init__(self, client):
        self.client = client

    @property
    def _settings(self):
        return {
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
            'store': {
                'compress': {
                    'stored': 'true',
                    'tv': 'true',
                }
            }
        }

    def create_index(self, name):
        """Create an index with our custom settings.
        """
        return self.client.create_index(name, settings=self._settings)

    def optimize_index(self, name):
        """Fully optimize an index down to one segment.
        """
        return self.client.optimize(
            name, max_num_segments=1, wait_for_merge=True)


class ESWrite(Plugin):

    def __init__(self, **options):
        self.options = options
        self.url = options['url']
        self.client = ExtendedClient(self.url)
        self.setup = ESSetup(self.client)

    def __call__(self, data, **options):
        self.setup.create_index('monolith_2013-01')
        return self.client.index(
            'monolith_2013-01', 'downloads', data)
