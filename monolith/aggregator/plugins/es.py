from collections import defaultdict

import elasticsearch
from elasticsearch import helpers

from monolith.aggregator.plugins import Plugin


class ESSetup(object):

    def __init__(self, client):
        self.client = client

    def _default_settings(self):
        return {
            'settings': {
                'refresh_interval': '10s',
                'default_field': '_id',
                'analysis': {
                    'analyzer': {
                        'default': {
                            'type': 'custom',
                            'tokenizer': 'keyword',
                        },
                    },
                },
                'store': {
                    'compress': {
                        'stored': 'true',
                        'tv': 'true',
                    },
                },
                'cache': {
                    'field': {
                        'type': 'soft',
                    },
                },
            },
            'mappings': {
                '_default_': {
                    '_all': {'enabled': False},
                    'dynamic_templates': [{
                        'disable_string_analyzing': {
                            'match': '*',
                            'match_mapping_type': 'string',
                            'mapping': {
                                'type': 'string',
                                'index': 'not_analyzed',
                            },
                        },
                    }],
                },
            },
        }

    def configure_templates(self):
        # setup template for time-slice index
        try:
            res = self.client.indices.get_template(name='time_1')
        except elasticsearch.ElasticsearchException:
            res = None
        if res:  # pragma: no cover
            try:
                self.client.indices.delete_template(name='time_1')
            except elasticsearch.ElasticsearchException:
                pass
        time_settings = self._default_settings()
        time_settings['template'] = '*time_*'
        time_settings['settings']['number_of_shards'] = 1
        time_settings['settings']['number_of_replicas'] = 1
        self.client.indices.put_template(name='time_1', body=time_settings)

    def optimize_index(self, index):
        """Fully optimize an index down to one segment."""
        return self.client.indices.optimize(
            index=index, max_num_segments=1, wait_for_merge=True)


class ESWrite(Plugin):

    def __init__(self, **options):
        self.options = options
        self.url = options['url']
        self.prefix = options.get('prefix', '')
        self.client = elasticsearch.Elasticsearch(hosts=[self.url])
        self.setup = ESSetup(self.client)
        self.setup.configure_templates()

    def _index_name(self, date):
        return '%stime_%.4d-%.2d' % (self.prefix, date.year, date.month)

    def _bulk_index(self, index, doc_type, docs, id_field='id'):
        actions = [
            {'_index': index, '_type': doc_type, '_id': doc.pop(id_field),
             '_source': doc} for doc in docs]

        return helpers.bulk(self.client, actions)

    def inject(self, batch):
        holder = defaultdict(list)
        # sort data into index/type buckets
        for source_id, item in batch:
            # XXX use source_id as a key with dates for updates
            item = dict(item)
            date = item['date']
            index = self._index_name(date)
            _type = item.pop('_type')
            holder[(index, _type)].append(item)

        # submit one bulk request per index/type combination
        for key, docs in holder.items():
            actions = [
                {'_index': key[0], '_type': key[1], '_id': doc.pop('_id'),
                 '_source': doc} for doc in docs]
            resp = helpers.bulk(self.client, actions)
            for res in resp[1]:
                if res['index'].get('ok'):
                    continue
                error = res['index'].get('error')
                if error is not None:
                    msg = 'Could not index %s' % str(docs[index])
                    msg += '\nES Error:\n'
                    msg += error
                    msg += '\n The data may have been partially imported.'
                    raise ValueError(msg)

    def clear(self, start_date, end_date, source_ids):
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        query = {'filtered': {
            'query': {'match_all': {}},
            'filter': {
                'and': [
                    {'range': {
                        'date': {
                            'gte': start_date_str,
                            'lte': end_date_str,
                        },
                        '_cache': False,
                    }},
                    {'terms': {
                        'source_id': source_ids,
                        '_cache': False,
                    }},
                ]
            }
        }}
        self.client.indices.refresh(index='%stime_*' % self.prefix)
        self.client.delete_by_query(index='%stime_*' % self.prefix, body=query)
