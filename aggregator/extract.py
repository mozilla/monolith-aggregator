import logging
import argparse
from ConfigParser import ConfigParser

from aggregator import __version__
from aggregator.util import resolve_name
from aggregator.db import Database


logger = logging.getLogger('aggregator')


def extract(config):
    """Reads the configuration file and does the job.
    """
    parser = ConfigParser()
    parser.read(config)

    # getting the DB
    db = parser.get('monolith', 'database')
    db = Database(db)

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

    # run the extraction
    # XXX



def main():
    parser = argparse.ArgumentParser(description='Monolith Aggregator')

    parser.add_argument('--version', action='store_true', default=False,
                        help='Displays version and exits.')
    parser.add_argument('config', help='Configuration file.',)

    args = parser.parse_args()

    if args.version:
        print(__version__)
        sys.exit(0)

    extract(args.config)


if __name__ == '__main__':
    main()
