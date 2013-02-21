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

Here's the high-level overview of the whole system:

.. image:: https://raw.github.com/mozilla/monolith-aggregator/master/docs/monolith-big-picture.png
