===================
Monolith Aggregator
===================

The monolith aggregator is part of the general monolith application, which
is providing statistic gathering, aggregation, a web-service API and a
dashboard.

The first consumer of monolith is the `Firefox marketplace
<https://marketplace.firefox.com/>`_. Statistics include amongst others public
global page views / hits, application specific downloads and even payment
related information.

This aggregator part deals with gathering data from multiple sources, bringing
them into a common format and storing them. Currently data is stored in MySQL
for durable archival and `ElasticSearch <http://www.elasticsearch.org/>`_ to
provide the actual data access for the web-service and dashboard.

The web-service and dashboard are implemented in `monolith
<https://github.com/mozilla/monolith>`_.

Detailed Documentation
======================

.. toctree::
   :maxdepth: 1

   install
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
