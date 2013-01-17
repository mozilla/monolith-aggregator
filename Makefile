HERE = $(shell pwd)
BIN = $(HERE)/bin
PYTHON = $(BIN)/python

INSTALL = $(BIN)/pip install
VTENV_OPTS ?= --distribute

BUILD_DIRS = bin build include lib lib64 man share

.PHONY: all clean test

all: build

$(PYTHON):
	virtualenv $(VTENV_OPTS) .

build: $(PYTHON)
	$(PYTHON) setup.py develop
	$(INSTALL) monolith-aggregator[test]

clean:
	rm -rf $(BUILD_DIRS)

test: build
	$(BIN)/nosetests -d -v --with-coverage --cover-package aggregator aggregator
