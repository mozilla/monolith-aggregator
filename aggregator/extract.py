import os
import logging
import argparse
from ConfigParser import ConfigParser
from ConfigParser import NoOptionError
import sys
from datetime import datetime

import gevent
from gevent.queue import JoinableQueue
from gevent.pool import Group

from aggregator import __version__
from aggregator.util import (resolve_name, configure_logger, LOG_LEVELS,
                             word2daterange)


logger = logging.getLogger('aggregator')


def _mkdate(datestring):
    return datetime.strptime(datestring, '%Y-%m-%d').date()


def _get_data(queue, callable, start_date, end_date):
    #logger.debug('Getting from %s' % callable.__doc__)
    for item in callable(start_date, end_date):
        queue.put(item)
    queue.put('END')


def _put_data(callable, data):
    #logger.debug('Pushing to %s' % callable.__doc__)
    return callable(data)


def _push_to_target(queue, targets, batch_size):
    """Get a batch of elements from the queue, and push it to the targets.

    This function returns False if it proceeded all the elements in the queue,
    and there isn't anything more to read.
    """
    batch = []
    while len(batch) < batch_size:
        item = queue.get()
        if item == 'END':
            break
        batch.append(item)

    if len(batch) != 0:
        logger.debug('pushing %s items', len(batch))
        greenlets = Group()
        try:
            for plugin in targets:
                greenlets.spawn(_put_data, plugin, batch)
            greenlets.join()
        finally:
            queue.task_done()

    if item == 'END':
        return False
    return True


def extract(config, start_date, end_date, valid_sources=None,
            valid_targets=None, batch_size=None):
    """Reads the configuration file and does the job.
    """
    parser = ConfigParser(defaults={'here': os.path.dirname(config)})
    parser.read(config)

    if not batch_size:
        try:
            batch_size = parser.get('monolith', 'batch_size')
        except NoOptionError:
            batch_size = 100
        else:
            batch_size = int(batch_size)
    logger.debug('size of the batches: %s', batch_size)

    # parsing the sources and targets
    sources = []
    targets = []

    for section in parser.sections():
        if section.startswith('source:'):
            if valid_sources is None or section.endswith(valid_sources):
                logger.debug('loading %s' % section)
                options = dict(parser.items(section))
                plugin = resolve_name(options['use'])
                options['parser'] = parser
                del options['use']
                sources.append(plugin(**options))

        elif section.startswith('target:'):
            if valid_targets is None or section.endswith(valid_targets):
                options = dict(parser.items(section))
                logger.debug('loading %s' % section)
                plugin = resolve_name(options['use'])
                options['parser'] = parser
                del options['use']
                targets.append(plugin(**options))

    queue = JoinableQueue()

    # run the extraction
    # each callable will push its result in the queue
    for plugin in sources:
        gevent.spawn(_get_data, queue, plugin, start_date, end_date)

    # looking at the queue
    processed = 0
    while processed < len(sources):
        if not _push_to_target(queue, targets, batch_size):
            processed += 1


_DATES = ['today', 'yesterday', 'last-week', 'last-month',
          'last-year']


def main():
    parser = argparse.ArgumentParser(description='Monolith Aggregator')

    parser.add_argument('--version', action='store_true', default=False,
                        help='Displays version and exits.')

    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument('--date', default=None, choices=_DATES,
                            help='Date')
    date_group.add_argument('--start-date', default=None, type=_mkdate,
                            help='Start date.')
    parser.add_argument('--end-date', default=None, type=_mkdate,
                        help='End date.')
    parser.add_argument('config', help='Configuration file.',)
    parser.add_argument('--log-level', dest='loglevel', default='info',
                        choices=LOG_LEVELS.keys() + [key.upper() for key in
                                                     LOG_LEVELS.keys()],
                        help="log level")
    parser.add_argument('--log-output', dest='logoutput', default='-',
                        help="log output")
    parser.add_argument('--source', dest='source', default=None,
                        help='A comma-separated list of sources')
    parser.add_argument('--target', dest='target', default=None,
                        help='A comma-separated list of targets')
    parser.add_argument('--batch-size', dest='batch_size', default=None,
                        type=int,
                        help='The size of the batch when writing')

    args = parser.parse_args()

    if args.version:
        print(__version__)
        sys.exit(0)

    if args.date is not None:
        start, end = word2daterange(args.date)
    else:
        start, end = args.start_date, args.end_date

    source = args.source
    if args.source is not None:
        source = tuple(args.source.split(','))

    target = args.target
    if args.target is not None:
        target = tuple(args.target.split(','))

    configure_logger(logger, args.loglevel, args.logoutput)
    extract(args.config, start, end, source, target, args.batch_size)


if __name__ == '__main__':
    main()
