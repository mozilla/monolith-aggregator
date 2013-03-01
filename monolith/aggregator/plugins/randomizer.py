import datetime
import random
from uuid import uuid1

from monolith.aggregator.plugins import Plugin


class RandomGenerator(Plugin):

    def __init__(self, **options):
        self.options = options

    def extract(self, start_date, end_date):
        platforms = self.options.get('platforms')
        addons = int(self.options.get('addons', 100))
        if platforms is None:
            platforms = ('Mac OS X', 'Windows 8', 'Ubuntu')
        else:
            platforms = platforms.split(', ')

        uuids = {}
        for addon in range(addons):
            uuids[addon] = uuid1().hex

        for addon in range(addons):
            for delta in range((end_date - start_date).days):
                date = start_date + datetime.timedelta(days=delta)
                yield {'_date': date,
                       '_type': 'downloads',
                       'os': random.choice(platforms),
                       'downloads_count': random.randint(1000, 1500),
                       'users_count': random.randint(10000, 15000),
                       'add_on': addon + 1,
                       'app_uuid': uuids.get(addon)}
