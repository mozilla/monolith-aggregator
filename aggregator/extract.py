import logging
import argparse
from ConfigParser import ConfigParser
import sys

import gevent
from gevent.queue import JoinableQueue
from gevent.pool import Group

from aggregator import __version__
from aggregator.util import resolve_name, configure_logger, LOG_LEVELS
from aggregator.db import Database


logger = logging.getLogger('aggregator')


def _get_data(queue, callable, options):
    logger.info('Getting from %s' % callable.__doc__)
    for item in callable(**options):
        queue.put(item)
    queue.put('END')


def _put_data(callable, data, **options):
    logger.info('Pushing to %s' % callable.__doc__)
    return callable(data, **options)


def _push_to_target(queue, targets):
    data = queue.get()
    if data == 'END':
        return False

    greenlets = Group()
    try:
        for callable, options in targets.items():
            greenlets.spawn(_put_data, callable, data, **options)
        greenlets.join()
    finally:
        queue.task_done()

    return True


def extract(config):
    """Reads the configuration file and does the job.
    """
    parser = ConfigParser()
    parser.read(config)

    # parsing the sources and targets
    sources = {}
    targets = {}

    for section in parser.sections():
        if section.startswith('source:'):
            options = dict(parser.items(section))
            callable = resolve_name(options['use'])
            del options['use']
            sources[callable] = options

        elif section.startswith('target:'):
            options = dict(parser.items(section))
            callable = resolve_name(options['use'])
            del options['use']
            targets[callable] = options

    queue = JoinableQueue()

    # run the extraction
    num_sources = len(sources)

    # each callable will push its result in the queue
    for callable, options in sources.items():
        gevent.spawn(_get_data, queue, callable, options)

    # looking at the queue
    processed = 0
    while processed < num_sources:
        if not _push_to_target(queue, targets):
            processed += 1


def main():
    parser = argparse.ArgumentParser(description='Monolith Aggregator')

    parser.add_argument('--version', action='store_true', default=False,
                        help='Displays version and exits.')
    parser.add_argument('config', help='Configuration file.',)
    parser.add_argument('--log-level', dest='loglevel', default='info',
                        choices=LOG_LEVELS.keys() + [key.upper() for key in
                                                     LOG_LEVELS.keys()],
                        help="log level")
    parser.add_argument('--log-output', dest='logoutput', default='-',
                        help="log output")

    args = parser.parse_args()

    if args.version:
        print(__version__)
        sys.exit(0)

    configure_logger(logger, args.loglevel, args.logoutput)
    extract(args.config)


if __name__ == '__main__':
    main()
