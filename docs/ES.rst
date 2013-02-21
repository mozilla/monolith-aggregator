Monolith ElasticSearch
======================

Notes on configuration and setup of ElasticSearch for monolith.

Assumptions
:::::::::::

In order to gain performance, we can lower precision and latency for a lot
of the metrics. But looking forward, being near real-time and high precision
would still be nice.

Currently external systems do some of the aggregation, but in the future we
might want to do this work inside monolith to gain more real-time and
precision.

Time-series indexes
:::::::::::::::::::

Multiple indexes are created to cover a distinct time period each. The
best time unit depends on the exact data volume and retention policies.
High-volume log/event setups use daily indexes, but that's likely complete
overkill for our concerns.

Currently all stats are kept in a single (sharded) index with roughly 45gb
of raw data in total.

The suggested starting setup is to use monthly indexes. Old months can
then be easily archived / rolled up into lower granularity (1 minute / daily /
weekly etc. time precision). So there would be indexes like::

    time_2013-01
    time_2012-12
    time_2012-11
    time_2012-10

All writes would happen to the *correct* index based on the timestamp of each
entry.

In addition each index can be split up into multiple shards, to distribute load
across different servers. At first a shard size of 1 is used, so each shard
holds roughly one month of data. Typical queries are for the last 30 days, so
usually involve queries for the current and former month. In this setup that's
querying two index shards.

Replication isn't needed to gain protection against data-loss, as ES isn't the
primary persistent storage. We'd still use a replication factor of 1 (meaning
there's two copies of all data) to spread read-load across multiple servers.
Depending on the load, we could increase the shard count for the current and
last month, as these are likely queried a lot more often than the older data.

**Note** These index/shard settings are aimed to keep the data per index at a
manageable size (for example for the JVM / memory requirements) per server. And
at the same time minimize the number of indexes involved in each query, to
avoid the associated overhead. In addition it's easy to drop out or replace old
data, as its just disabling an index, but there's no need to rewrite/update any
data. All but the current index can also be highly compacted / optimized
(down to one lucene segment), as they'll never change and backup tools likely
appreciate a large amount of static data as well.

Note that you don't need to manually specify the indexes yourself, but
Elastic Search allows you to read from `_all` or `time_*` indexes at once.
We hide the index details in our REST API, so the client side only has to care
about the REST endpoint like `GET /v1/time`.

Totals indexes
::::::::::::::

In addition to time-based data, we are also keeping track of some lifetime
totals, like total downloads or users per application. In the future this might
extend to total number of reviews, or be more specific, like grouped by region.

While it's possible to calculate those totals from the time series data, it is
extremely inefficient. As an example consider the total downloads for an old
addon. There's probably four download entries per day (per language/version/os)
and data exists for five years. So we have to query 4 * 365 * 5 entries from
all indexes/shards to calculate the result and handle an intermediate result
of 7300 rows. As these counters are prominently displayed on each addon page,
these queries should be fast. As there's a huge long-tail of addons and daily
updates to most of the data, a caching scheme seems sub-optimal. In addition
there's some reports which want to sort other data on "app rank" - which is
defined as "total downloads".

ElasticSearch unfortunately has no concept akin to materialized views, so we
have to design and update these aggregates ourselves.

As all of the data is constantly changing, there's little benefit in splicing
the data on a time-basis. So we'll use a single index for all totals and use
sharding instead. Access to this data is by application / addon id. So we can
store one entry for each app and have it contain the total values for multiple
metrics, making this a key (app id) to value (JSON document) storage. As each
app has a uuid, we can take those and use them as the ES _id. This way data
lookup becomes a `es_client.get('totals', 'apps' app_uuid)` call and we avoid
a search.

elasticsearch.yml
:::::::::::::::::

We don't need to have any custom elasticsearch.yml settings, as we are managing
all of these settings via index templates and cluster API calls.

Articles / videos
:::::::::::::::::

* http://blog.bugsense.com/post/35580279634/indexing-bigdata-with-elasticsearch
* http://edgeofsanity.net/article/2012/12/26/elasticsearch-for-logging.html
* https://github.com/logstash/logstash/wiki/Elasticsearch-Storage-Optimization
* Shay Banon at BB 2012 - http://vimeo.com/44716955
* Shay Banon at BB 2011 - http://vimeo.com/26710663
