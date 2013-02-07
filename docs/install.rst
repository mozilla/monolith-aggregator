============
Installation
============

Prerequisites
=============

Currently we only support Linux and Mac OS.

* Python 2.7
* virtualenv (available as such on the `$PATH`)
* Java Runtime Environment 6 or 7
* libevent-dev
* Git

Steps
=====

Get the code::

    git clone git://github.com/mozilla/monolith-aggregator.git

Build::

    cd monolith-aggregator
    make

Run tests::

    make test

To build the docs available in ``./docs/_build/html/index.html``::

    make docs
