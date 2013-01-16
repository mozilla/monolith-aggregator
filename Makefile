HERE = $(shell pwd)
BIN = $(HERE)/bin
PYTHON = $(BIN)/python

PIP_DOWNLOAD_CACHE ?= $(HERE)/.pip_cache
INSTALL = $(BIN)/pip install
INSTALL += --download-cache $(PIP_DOWNLOAD_CACHE) --use-mirrors
VTENV_OPTS ?= "--no-site-packages --distribute"

BUILD_DIRS = bin build include lib lib64 man share

.PHONY: all build clean test

all: build

$(PYTHON):
	virtualenv $(VTENV_OPTS) .

build: $(PYTHON)
	$(PYTHON) setup.py develop
	$(INSTALL) monolith-aggregator[test]

clean:
	rm -rf $(BUILD_DIRS)

test:
	$(BIN)/nosetests -d -v --with-coverage --cover-package aggregator aggregator
