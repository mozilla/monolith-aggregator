#!/bin/sh
set -e

echo "Starting build on executor $EXECUTOR_NUMBER..."

# Make sure there's no old pyc files around.
find . -name '*.pyc' -exec rm {} \;

echo "Running make build..."

make build

echo "Starting tests..."

bin/nosetests -s -d -v --with-xunit --with-coverage --cover-package aggregator aggregator
bin/coverage xml $(find aggregator -name '*.py')

echo "FIN"
