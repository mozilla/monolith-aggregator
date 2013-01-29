import datetime
import random

from aggregator.plugins import Plugin


class RandomGenerator(Plugin):

    def __init__(self, **options):
        self.options = options

    def __call__(self, start_date, end_date):
        platforms = self.options.get('platforms')
        addons = int(self.options.get('addons', 100))
        if platforms is None:
            platforms = ('Mac OS X', 'Windows 8', 'Ubuntu')
        else:
            platforms = platforms.split(', ')

        for addon in range(addons):
            for delta in range((end_date - start_date).days):
                yield {'date': start_date + datetime.timedelta(days=delta),
                       'category': 'downloads',
                       'os': random.choice(platforms),
                       'downloads_count': random.randint(1000, 1500),
                       'users_count': random.randint(10000, 15000),
                       'add_on': addon + 1}
