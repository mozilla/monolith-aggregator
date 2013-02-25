.. include:: ../README.rst


**monolith-aggregator** adds data to a MySQL database, using a
command-line script called *monolith-extract*.

This script that grabs data from various sources and adds them in
the SQL Database.

The script also indexes all the content in Elastic Search, which
can be used to do time-series queries on the data.

The SQL database is the *single source of truthe* of Monolith.
The Elastic Search can be recreated at anytime for any date range,
using the SQL database.

A typical setup is to run *monolith-extract* every day as a cron.

Detailed Documentation
======================

.. toctree::
   :maxdepth: 1

   install
   configuration
   mysql
   ES
   high-availability
   plugin
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
