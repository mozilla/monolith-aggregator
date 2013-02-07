Monolith MySQL
==============

MySQL role in monolith is just to keep the data in case we want to
rebuild various Elastic Searches indexes.

We will have 2 types of interactions with the database:

1. the script that grabs data from various sources and feeds the Database
2. the script that queries the Database on specific date ranges and
   feed the Elastic Search indexes.

Note that Elastic Search will be feed live when 1. occurs,
and that 2. will happen only on reindexations.

All metrics that are collected from various sources are stored into
MySQL, in a single table that has a category and a date field.
The data itself is stored as-is in a binary field.

We're planning to store roughly 1M lines per month, so 12M lines per
year, and eventually shard the storage into one table per year - so
we limit the size of the table to 12M lines.

The sharding will not impact metrics queries that are made through
Elastic Search - but the reindexation script will have to take into
account this sharding.

