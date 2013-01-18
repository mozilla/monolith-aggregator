Which Database system ?
=======================

Data size = 12M lines / year. We need to keep all of it.

- data insertion:
    - flat lines (timestamp + category + raw data)

- data queries
    - XXX list all dashboard queries here


XXX

+---------------------------------------------------------------------------------+
| Name              |   Transparent Sharding    | Webdev Ops XP | Clustering      |
+-------------------+---------------------------+---------------+-----------------+
| Postgres w/hstore | No                        | No            | via replication |
+-------------------+---------------------------+---------------+-----------------+
| ES + MySQL        | Yes (for queries)         | Yes           | Yes             |
+-------------------+---------------------------+---------------+-----------------+
| CouchDB           | Yes                       | No            | Yes             |
+-------------------+---------------------------+---------------+-----------------+
+ CouchBase         +
+-------------------+---------------------------+---------------+-----------------+


