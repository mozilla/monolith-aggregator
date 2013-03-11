HERE = $(shell pwd)
BIN = $(HERE)/bin
PYTHON = $(BIN)/python

INSTALL = $(BIN)/pip install --no-deps
VTENV_OPTS ?= --distribute
ES_VERSION ?= 0.20.5

BUILD_DIRS = bin build elasticsearch include lib lib64 man share

.PHONY: all clean docs test

all: build

$(PYTHON):
	virtualenv $(VTENV_OPTS) .

build: $(PYTHON) elasticsearch
	$(INSTALL) -r requirements/prod.txt
	$(INSTALL) -r requirements/dev.txt
	$(INSTALL) -r requirements/test.txt
	$(PYTHON) setup.py develop

clean:
	rm -rf $(BUILD_DIRS)

docs:
	cd docs && make html

test: build
	ES_PATH=$(HERE)/elasticsearch \
	$(BIN)/nosetests -s -d -v --with-coverage --cover-package monolith.aggregator monolith/aggregator

elasticsearch:
	curl -C - http://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-$(ES_VERSION).tar.gz | tar -zx
	mv elasticsearch-$(ES_VERSION) elasticsearch
	chmod a+x elasticsearch/bin/elasticsearch
	mv elasticsearch/config/elasticsearch.yml elasticsearch/config/elasticsearch.in.yml
	cp elasticsearch.yml elasticsearch/config/elasticsearch.yml
