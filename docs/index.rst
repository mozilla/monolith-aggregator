.. include:: ../README.rst

Details
=======

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

Detailed Documentation
======================

.. toctree::
   :maxdepth: 1

   install
   mysql
   ES
   high-availability
   Changelog <changelog>

Source Code
===========

All source code is available on `github under monolith-aggregator
<https://github.com/mozilla/monolith-aggregator>`_.

Further information
===================

You an read up on some `background and motivation
<https://wiki.mozilla.org/Marketplace/MetricsServer>`_.

There is also a `detailed product specification <https://docs.google.com/document/d/1tlNQqgsCCGC3B4S1lstKx5sbWbTdGOhzxekL6i1XimM/edit?pli=1>`_
available to the public.

We also keep some `progress information in an etherpad
<https://etherpad.mozilla.org/monolith>`_.

License
=======

``monolith-aggregator`` is offered under the MPL 2.0.
