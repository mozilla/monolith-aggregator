import datetime

from pyelastictest import IsolatedTestCase


class TestESSetup(IsolatedTestCase):

    def _make_one(self):
        from monolith.aggregator.plugins import es
        client = es.ExtendedClient(self.es_cluster.urls)
        return es.ESSetup(client)

    def test_configure_templates(self):
        setup = self._make_one()
        client = setup.client
        setup.configure_templates()
        # directly use the client, which should pick up the template settings
        client.create_index('time_2013-01')
        self.assertEqual(
            client.status('time_2013-01')['_shards']['total'], 2)
        for i in range(1, 32):
            client.index('time_2013-01', 'downloads', {
                'date': datetime.datetime(2013, 01, i),
                'count': i % 5,
            })
        client.refresh()
        # integers should stay as ints, and not be converted to strings
        res = client.search(
            {'facets': {'facet1': {'terms': {'field': 'count'}}},
             'sort': [{"date": {"order": "asc"}}]},
            index='time_*')
        for ft in [t['term'] for t in res['facets']['facet1']['terms']]:
            self.assertTrue(isinstance(ft, int))
        # and dates should be in their typical ES format
        first = res['hits']['hits'][0]['_source']['date']
        self.assertEqual(first, '2013-01-01T00:00:00')

    def test_create_index_no_string_analysis(self):
        setup = self._make_one()
        client = setup.client
        setup.configure_templates()
        client.create_index('time_2011-11')
        client.index('time_2011-11', 'test', {'a': 'Foo bar', 'b': 1})
        client.index('time_2011-11', 'test', {'a': 'foo baz', 'b': 2})
        client.refresh()
        # make sure we get facets for the two exact strings we indexed
        res = client.search(
            {'query': {'match_all': {}},
             'facets': {'facet1': {'terms': {'field': 'a'}}}})
        facet1 = res['facets']['facet1']
        self.assertEqual(set([f['term'] for f in facet1['terms']]),
                         set(['Foo bar', 'foo baz']))

    def test_optimize_index(self):
        setup = self._make_one()
        client = setup.client
        client.create_index('foo_2011-11')
        client.index('foo_2011-11', 'test', {'foo': 1})
        res = client.index('foo_2011-11', 'test', {'foo': 2})
        client.delete('foo_2011-11', 'test', res['_id'])
        setup.optimize_index('foo_2011-11')
        res = client.status('foo_2011-11')['indices']['foo_2011-11']
        # the deleted doc was merged away
        self.assertEqual(res['docs']['deleted_docs'], 0)


class TestESWrite(IsolatedTestCase):

    def _make_one(self):
        from monolith.aggregator.plugins import es
        options = {'url': self.es_cluster.urls}
        return es.ESWrite(**options)

    def test_constructor(self):
        plugin = self._make_one()
        self.assertEqual(len(plugin.client.servers.live), 1)

    def test_call(self):
        plugin = self._make_one()
        data = ('source_id', {
            '_id': 'abc123',
            '_type': 'downloads',
            'date': datetime.datetime(2012, 7, 4),
            'foo': 'bar',
            'baz': 2,
        })
        plugin.inject([data])
        self.es_client.refresh()
        res = self.es_client.search({'query': {'match_all': {}}})
        source = res['hits']['hits'][0]['_source']
        for field in ('foo', 'baz'):
            self.assertEqual(source[field], data[1][field])
        self.assertEqual(source['date'], '2012-07-04T00:00:00')
