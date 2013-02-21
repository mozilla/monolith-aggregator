import os
import logging
import argparse
from ConfigParser import ConfigParser, NoOptionError
import sys
from datetime import datetime
from collections import defaultdict

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
    #logger.info('Getting from %s' % callable)
    try:
        for item in callable(start_date, end_date):
            queue.put(item)
    finally:
        queue.put('END')


def _put_data(callable, data):
    #logger.info('Pushing to %s' % callable)
    return callable(data)


def _push_to_target(queue, targets, batch_size):
    """Get a batch of elements from the queue, and push it to the targets.

    This function returns True if it proceeded all the elements in the queue,
    and there isn't anything more to read.
    """
    if queue.empty():
        return False    # nothing

    batch = []
    eoq = False

    # collecting a batch
    while len(batch) < batch_size:
        try:
            item = queue.get()
            if item == 'END':
                # reached the end
                eoq = True
                break
            batch.append(item)
        finally:
            queue.task_done()


    if len(batch) != 0:
        #logger.info('Pushing %s items', len(batch))
        greenlets = Group()
        for plugin in targets:
            greenlets.spawn(_put_data, plugin, batch)
        greenlets.join()

    return eoq


def extract(config, start_date, end_date, sequence=None, batch_size=None):
    """Reads the configuration file and does the job.
    """
    parser = ConfigParser(defaults={'here': os.path.dirname(config)})
    parser.read(config)

    try:
        batch_size = parser.get('monolith', 'batch_size')
    except NoOptionError:
        # using the default value
        if batch_size is None:
            batch_size = 100

    logger.info('size of the batches: %s', batch_size)

    # parsing the sequence, phases, sources and targets
    if sequence is None:
        try:
            sequence = parser.get('monolith', 'sequence')
        except NoOptionError:
            raise ValueError("You need to define a sequence.")

    sequence = [phase.strip() for phase in sequence.split(',')]
    config = defaultdict(dict)
    keys = ('phase', 'source', 'target')

    for section in parser.sections():
        for key in keys:
            prefix = key + ':'
            if section.startswith(prefix):
                name = section[len(prefix):]
                config[key][name] = dict(parser.items(section))

    # (XXX should move this to a class)
    # let's load all the plugins we need for the sequence now
    plugins = {}

    def _load_plugin(type_, name, options):
        key = type_, name

        if key in plugins:
            return plugins[key]

        options = dict(options)
        try:
            plugin = resolve_name(options['use'])
        except KeyError:
            msg = "Missing the 'use' option for plugin %r" % name
            msg += '\nGot: %s' %  str(options)
            raise KeyError(msg)

        options['parser'] = parser
        del options['use']
        instance = plugin(**options)
        plugins[key] = instance
        return instance

    def _build_phase(phase):
        def _load(name, type_):
            name = name.strip()
            if name not in config[type_]:
                raise ValueError('%r %s is undefined' % (name, type_))
            logger.info('Loading %s:%s' % (type_, name))
            return _load_plugin(type_, name, config[type_][name])

        if phase not in config['phase']:
            raise ValueError('%r phase is undefined' % phase)

        options = config['phase'][phase]
        targets = [_load(target, 'target')
                   for target in options['targets'].split(',')]
        sources = [_load(source, 'source')
                   for source in options['sources'].split(',')]
        return phase, sources, targets

    # a sequence is made of phases (XXX should move this to a class)
    sequence = [_build_phase(phase) for phase in sequence]


    # run the sequence by phase
    queue = JoinableQueue()

    for phase, sources, targets in sequence:
        logger.info('Running phase %r' % phase)
        greenlets = Group()

        # each callable will push its result in the queue
        for source in sources:
            greenlets.spawn(_get_data, queue, source, start_date, end_date)

        # looking at the queue
        processed = 0
        while processed < len(sources):
            eoq = _push_to_target(queue, targets, batch_size)
            if eoq:
                processed += 1
            gevent.sleep(0)

        greenlets.join()


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
    parser.add_argument('--sequence', dest='sequence', default=None,
                        help='A comma-separated list of sequences.')
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

    configure_logger(logger, args.loglevel, args.logoutput)
    extract(args.config, start, end, args.sequence, args.batch_size)


if __name__ == '__main__':
    main()
