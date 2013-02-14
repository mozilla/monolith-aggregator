# This plugins exposes a way to read data fro mthe zamboni database. it uses
# the autoload feature from SQL Alchemy in order to be able to read information
# from any other database.

# The plugin need to evolve at the same pace the zamboni code evolves (so if we
# have new arguments in the tables, they need to be put here as well.
import itertools

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Table
from sqlalchemy.orm import sessionmaker

from aggregator.plugins import Plugin

TABLES = {'collection_counts': 'stats_addons_collections_counts'}


class DownloadCountReader(Plugin):

    def _get_table(self, table):
        """
        Iterates on the dict and attach a sqla table to the
        object so it can be used to read content.

        :param tables: a dict of name => table_name.
        """
        return Table(table, self._base.metadata,
                schema='marketplace', autoload=True,
                autoload_with=self.engine)

    def __init__(self, parser, **kwargs):
        self.engine = create_engine(parser.get('zamboni', 'db'))

        self.session_factory = sessionmaker(bind=self.engine)
        self.session = self.session_factory()
        self._base = declarative_base()

    def __call__(self, *args, **kwargs):
        dl_counts = self.get_download_counts(*args, **kwargs)

        return itertools.chain(dl_counts)

    def get_download_counts(self, start_date, end_date):
        dl_counts = self._get_table('download_counts')
        columns = dl_counts.columns.keys()

        query = self.session.query(dl_counts)

        if start_date is not None:
            query = query.filter(dl_counts.columns['date'] >= start_date)

        if end_date is not None:
            query = query.filter(dl_counts.columns['date'] <= end_date)

        iterator = (dict(zip(columns, d)) for d in query.all())
        return iterator
