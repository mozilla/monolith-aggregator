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

- category (the category of data we are storing)
- data (the JSON object, as a blob)
- date (the date of the data)

A few definitions
:::::::::::::::::

**monolith-aggregator** works with **sequences**. A **sequence** is a
list of **phases**.

A **phase** defines a list of **sources** and a list of **targets**.
Each target and each source is defined by a plugin class, that
receives a few options to run.


monolith-extract
::::::::::::::::

*monolith-extract* is based on a configuration file and plugins.

Each source and each target are defined in a section *prefixed*
by **source:** or **target**.

The file also defines **phases**, that reunite a list of **sources**
and **targets**.

Last, a **sequence** is built using a list of **phases**.

Everything is run asynchronously but the system makes sure
a phase is over when starting a new one in a sequence.

This is useful when you need a two-phase strategy.

.. code-block:: ini

    [monolith]
    timeout = 10
    database = pymysql://user:password@localhost/monolith
    sequence = extract, load

    [phase:extract]
    sources = google-analytics, solitude, marketplace
    targets = sql

    [phase:load]
    sources = sql
    targets = elasticsearch

    [target:elasticsearch]
    user = monolithic.plugins.elasticsearch
    url = http://es/is/here

    [source:sql]
    use = aggregator.plugins.sqlread.SQLRead
    database = mysql+pymysql://monolith:monolith@localhost/monolith
    query = select * from record where date BETWEEN :start_date and :end_date

    [target:sql]
    use = aggregator.plugins.sqlwrite.SQLInjecter
    database = mysql+pymysql://monolith:monolith@localhost/monolith

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

*monolith-extract* can run the predefined sequence, or one passed as an option.
This is useful when you just need to replay a specific phase.


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
