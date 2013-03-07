import os
import argparse
from ConfigParser import ConfigParser, NoOptionError
import sys
from datetime import datetime

from monolith.aggregator import __version__, logger
from monolith.aggregator.util import (configure_logger, LOG_LEVELS,
                                      word2daterange)
from monolith.aggregator.history import History
from monolith.aggregator.sequence import Sequence
from monolith.aggregator.engine import Engine


def _mkdate(datestring):
    return datetime.strptime(datestring, '%Y-%m-%d').date()


def extract(config, start_date, end_date, sequence=None, batch_size=None,
            force=False, purge_only=False, retries=3):
    """Reads the configuration file and does the job.
    """
    defaults = {'here': os.path.abspath(os.path.dirname(config))}
    parser = ConfigParser(defaults=defaults)
    parser.read(config)

    try:
        batch_size = parser.get('monolith', 'batch_size')
    except NoOptionError:
        # using the default value
        if batch_size is None:
            batch_size = 100

    # creating the sequence
    sequence = Sequence(parser, sequence)

    # load the history
    try:
        history_db = parser.get('monolith', 'history')
    except NoOptionError:
        raise ValueError("You need a history db option")

    history = History(sqluri=history_db)

    # run the engine
    engine = Engine(sequence, history, batch_size=batch_size, force=force,
                    retries=retries)
    return engine.run(start_date, end_date, purge_only)


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
    parser.add_argument('--force', action='store_true', default=False,
                        help='Forces a run')
    parser.add_argument('--purge-only', action='store_true', default=False,
                        help='Only run the purge of sources.')
    parser.add_argument('--retries', default=3, type=int,
                        help='Number of retries')
    args = parser.parse_args()

    if args.version:
        print(__version__)
        sys.exit(0)

    if args.date is not None:
        start, end = word2daterange(args.date)
    elif args.start_date is None:
        start, end = word2daterange('yesterday')
    else:
        start, end = args.start_date, args.end_date

    configure_logger(logger, args.loglevel, args.logoutput)
    res = extract(args.config, start, end, args.sequence, args.batch_size,
                  args.force, args.purge_only)

    if res == 0:
        logger.info('SUCCESS')
    else:
        logger.info('ERROR')

    sys.exit(res)


if __name__ == '__main__':
    main()
