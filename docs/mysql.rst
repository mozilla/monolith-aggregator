Monolith MySQL
==============

MySQL role in Monolith is just to keep the data in case we want to
rebuild various Elastic Searches indexes.

The database stores directly JSON objects, as blobs. It has the following
fields:

- **id**: a unique id per row.
- **date**: the date of the data.
- **type**: the type of data we are storing.
- **source_id**: a unique identifier of the source
- **data**: the JSON object, as a blob.

We have two types of interactions with the database:

1. the script that grabs data from various sources and feeds the Database
2. the script that queries the Database on specific date ranges and
   feed the Elastic Search indexes.

All metrics that are collected from various sources are stored into
MySQL, in a single table that has a **type**, a **date** and
a **source_id** field. The data itself is stored as-is in a binary
field.

We're planning to store roughly 1M lines per month, so 12M lines per
year, and eventually shard the storage into one table per year - so
we limit the size of the table to 12M lines.

The sharding will not impact metrics queries that are made through
Elastic Search - but the reindexation script will have to take into
account this sharding.


MySQL configuration
-------------------

::

    SET GLOBAL innodb_file_format='Barracuda'
    SET GLOBAL innodb_file_per_table=1

