monolith-aggregator
===================

**monolith-aggregator** is the script that feeds the Monolith Database.

Here's the high-level overview of the whole system:

.. image:: https://raw.github.com/mozilla/monolith-aggregator/master/docs/monolith-sequence.png

This script could be replaced at some point by `Heka
<https://heka-docs.readthedocs.org/>`_.

**monolith-aggregator** maintains a database and provides a command-line
script that interact with it:

- *monolith-extract*: the script that grabs data from all defined sources and
  pour them in the SQL Database and ElasticSearch.

A typical setup is to run *monolith-extract* every hour.

The database
::::::::::::

The database stores directly JSON objects, as blobs. It has the following
fields:

- type (the type of data we are storing, TBD)
- data (the JSON object, as a blob)
- date (a SQL date type)

monolith-extract
::::::::::::::::

*monolith-extract* is based on a configuration file and plugins. Each source
is defined in a configuration file, and for each source a section starting by
*source:* is defined::

    [monolith]
    timeout = 10
    database = pymysql://user:password@localhost/monolith

    [target:elasticsearch]
    user = monolithic.plugins.elasticsearch
    url = http://es/is/here

    [source:google-analytics]
    use = monolithic.plugins.ganalytics
    url = http://google.com/analytic
    user = moz@moz.com
    password = sesame

    [source:solitude]
    use = monolithic.plugins.generic_rest
    url = http://solitude.service/get_stats

    [source:marketplace]
    use = monolithic.plugins.market_place
    database = pymysql://user:password@mkt/marketplace


**use** points to a callable that will be invoked with all the other variables
of the section and the variables defined in **monolith** to perform the work.

*monolith-extract* invokes in parallel every callable, which also receives
an open SQL connector to the storage.

*monolith-extract* can run against all sources, or a specific list of sources.
This let us configure several crons if we need to extract data from sources
at specific times.
