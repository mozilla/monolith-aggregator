import logging
import argparse

from aggregator import __version__
from aggregator.util import resolve_name


logger = logging.getLogger('aggregator')


def extract(config):
    """Reads the configuration file and does the job.
    """
    pass


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
