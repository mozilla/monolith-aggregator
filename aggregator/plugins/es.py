from collections import defaultdict
import datetime

from pyelasticsearch import ElasticHttpNotFoundError
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

    @es_kwargs()
    def multi_get(self, index=None, doc_type=None, body=None,
                  query_params=None):
        if not body:
            # keep index and doc_type as first arguments,
            # but require body
            raise ValueError('A body is required.')

        return self.send_request(
            'GET',
            [self._concat(index), self._concat(doc_type), '_mget'],
            body,
            query_params=query_params)


class ESSetup(object):

    def __init__(self, client):
        self.client = client

    def configure_templates(self):
        res = self.client.get_template('time_1')
        if res:
            try:
                self.client.delete_template('time_1')
            except Exception:
                pass
        self.client.create_template('time_1', {
            'template': 'time_*',
            'settings': {
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
            },
            'mappings': {
                '_default_': {
                    '_all': {'enabled': False},
                },
            }
        })

        # setup template for totals index
        res = self.client.get_template('total_1')
        if res:
            try:
                self.client.delete_template('total_1')
            except Exception:
                pass
        self.client.create_template('total_1', {
            'template': 'totals',
            'settings': {
                'number_of_shards': 6,
                'number_of_replicas': 0,
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
            },
            'mappings': {
                '_default_': {
                    '_all': {'enabled': False},
                },
            }
        })

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
        return 'time_%.4d-%.2d' % (date.year, date.month)

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

    def get_app_totals(self, app_ids):
        # do one multi-get call for all apps
        try:
            res = self.client.multi_get('totals', 'apps', {'ids': app_ids})
        except ElasticHttpNotFoundError:
            found = {}
        else:
            found = dict([(d['_id'], d) for d in res['docs'] if d['exists']])
        return found

    def update_app_totals(self, apps, found):
        # and one index call per item
        for id_, value in apps.items():
            res = found.get(id_)
            if res:
                version = res['_version']
                source = res['_source']
                source['downloads'] += value['downloads']
                source['users'] += value['users']
            else:
                version = 0
                source = value
            self.client.index('totals', 'apps', source,
                id=id_, es_version=version)

    def sum_up_app(self, item, apps):
        if ('app_uuid' in item and
           ('downloads_count' in item or 'users_count' in item)):
            id_ = item['app_uuid']
            apps[id_]['downloads'] += item.get('downloads_count', 0)
            apps[id_]['users'] += item.get('users_count', 0)

    def __call__(self, batch):
        holder = defaultdict(list)
        apps = defaultdict(lambda: dict(downloads=0, users=0))
        today = datetime.date.today()

        # sort data into index/type buckets
        for item in batch:
            item = dict(item)
            date = item.get('date', today)
            index = self._index_name(date)
            category = item.pop('category', 'unknown')
            holder[(index, category)].append(item)
            # upsert totals data for app download/users
            self.sum_up_app(item, apps)

        # submit one bulk request per index/type combination
        for key, docs in holder.items():
            self._bulk_index(key[0], key[1], docs)

        # do we need to update total counts?
        if apps:
            found = self.get_app_totals(apps.keys())
            self.update_app_totals(apps, found)
