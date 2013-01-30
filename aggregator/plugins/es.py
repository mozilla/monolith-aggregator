from collections import defaultdict
import datetime

from pyelasticsearch import ElasticSearch
from pyelasticsearch.client import es_kwargs

from aggregator.plugins import Plugin


class ExtendedClient(ElasticSearch):
    """Wrapper around pyelasticsearch's client to add some missing
    API's. These should be merged upstream.
    """

    @es_kwargs()
    def create_template(self, name, settings, query_params=None):
        """
        Create an index template.

        :arg name: The name of the template.
        :arg settings: A dictionary of settings.

        See `ES's index-template API`_ for more detail.

        .. _`ES's index-template API`:
            http://www.elasticsearch.org/guide/reference/api/admin-indices-templates.html
        """
        return self.send_request('PUT', ['_template', name], settings,
            query_params=query_params)

    @es_kwargs()
    def delete_template(self, name, query_params=None):
        """
        Delete an index template.

        :arg name: The name of the template.

        See `ES's index-template API`_ for more detail.

        .. _`ES's index-template API`:
            http://www.elasticsearch.org/guide/reference/api/admin-indices-templates.html
        """
        return self.send_request('DELETE', ['_template', name],
            query_params=query_params)

    @es_kwargs()
    def get_template(self, name, query_params=None):
        """
        Get the settings of an index template.

        :arg name: The name of the template.

        See `ES's index-template API`_ for more detail.

        .. _`ES's index-template API`:
            http://www.elasticsearch.org/guide/reference/api/admin-indices-templates.html
        """
        return self.send_request('GET', ['_template', name],
            query_params=query_params)

    def list_templates(self):
        """
        Get a dictionary with all index template settings.

        See `ES's index-template API`_ for more detail.

        .. _`ES's index-template API`:
            http://www.elasticsearch.org/guide/reference/api/admin-indices-templates.html
        """
        res = self.cluster_state(filter_routing_table=True,
            filter_nodes=True, filter_blocks=True)
        return res['metadata']['templates']

    @es_kwargs('filter_nodes', 'filter_routing_table', 'filter_metadata',
               'filter_blocks', 'filter_indices')
    def cluster_state(self, query_params=None):
        """
        The cluster state API allows to get a comprehensive state
        information of the whole cluster.

        :arg query_params: A map of querystring param names to values or
            ``None``

        See `ES's cluster-state API`_ for more detail.

        .. _`ES's cluster-state API`:
            http://www.elasticsearch.org/guide/reference/api/admin-cluster-state.html
        """
        return self.send_request(
            'GET', ['_cluster', 'state'], query_params=query_params)


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

    def _bulk_index(self, index, doc_type, docs):
        # an optimized version of the bulk_index, avoiding
        # repetition of index and doc_type in each action line
        _encode_json = self.client._encode_json
        action_encoded = _encode_json({'index': {}})
        body_bits = []
        for doc in docs:
            body_bits.extend([action_encoded, _encode_json(doc)])

        # Need the trailing newline.
        body = '\n'.join(body_bits) + '\n'
        return self.client.send_request('POST',
            [index, doc_type, '_bulk'],
            body,
            encode_body=False,
        )

    def __call__(self, batch):
        holder = defaultdict(list)
        today = datetime.date.today()
        # sort data into index/type buckets
        for item in batch:
            date = item.get('date', today)
            index = self._index_name(date)
            category = item.pop('category', 'unknown')
            holder[(index, category)].append(item)
        # submit one bulk request per index/type combination
        for key, docs in holder.items():
            self._bulk_index(key[0], key[1], docs)
