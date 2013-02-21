=================
High-availability
=================

Multi-DC setup
==============

ElasticSearch doesn't have any good built-in multi-DC support. Inside a single
DC you can configure various degrees of HA via setting up replica counts for
each index. In our planned 3-node cluster we use a single replica, and thus
can loose a single node out of three.

For monolith, we can use a trick for the multi-DC case: Since all the data is
stored in MySQL, we can simply run two (or any number) completely separate
ElasticSearch clusters in each DC. And then have a job to update the data in
each DC from the single MySQL source. Since we introduced per-record uuid's we
can run integrate checks and push new / remove old records in each ES cluster.

Connection problems or downtimes of some of the DC's can lead to stale data for
some period of time, but won't cause any data-integrity issues.

The MySQL source is only setup in a single DC (though with a master-master)
setup. A downtime of the DC can lead to stale data, but won't cause downtime or
complete service unavailability for the end-user facing site.

For MySQL we can run a slave in each extra DC. The replication can be async,
to prevent it from slowing down or impacting availability in the primary DC.
With that setup, we ship all new data as large chunks of compressed MySQL data
to each DC. We can then run the MySQL to ES load job in each DC. This avoids
running many HTTP connections across inter-DC network links, to fill data from
MySQL to each ES-cluster.

In a later stage we can look at Cassandra as a replacement for MySQL to have
a reliable cross-DC setup for the persistent storage component of the system.
