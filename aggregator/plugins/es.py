import datetime

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

    def list_templates(self):
        res = self.send_request(
            'GET', ['_cluster', 'state'], query_params={
                'filter_routing_table': True,
                'filter_nodes': True,
                'filter_blocks': True,
            })
        return res['metadata']['templates']


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

    def configure_templates(self):
        res = self.client.get_template('time_1')
        if not res:
            # TODO: update/merge template settings
            self.client.create_template('monolith_1', {
                'template': 'monolith_*',
                'settings': self._settings,
                'mappings': {
                    '_default_': {
                        '_all': {'enabled': False},
                    },
                    'dynamic_templates': {
                        'string_template': {
                            'match': '*',
                            'mapping': {
                                'type': 'string',
                                'index': 'not_analyzed',
                            },
                            'match_mapping_type': 'string',
                        },
                    },
                    'properties': {
                        'category': {
                            'type': 'string',
                            'index': 'not_analyzed',
                        },
                        'date': {
                            'type': 'date',
                        },
                    }
                }
            })

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
        self.setup.configure_templates()

    def _index_name(self, date):
        return 'monolith_%.4d-%.2d' % (date.year, date.month)

    def __call__(self, data, **options):
        category = data.get('category', 'unknown')
        date = data.get('date', datetime.date.today())
        return self.client.index(
            self._index_name(date), category, data,
            replication='async',
        )
