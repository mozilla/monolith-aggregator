import datetime
import random

from aggregator.plugins import Plugin


class RandomGenerator(Plugin):

    def __call__(self, start_date, end_date, *args, **options):
        platforms = options.get('platforms')
        if platforms is None:
            platforms = ('Mac OS X', 'Windows 8', 'Ubuntu')
        else:
            platforms = platforms.split(', ')

        for delta in range((end_date - start_date).days):
            yield {'date': start_date + datetime.timedelta(days=delta),
                   'os': random.choice(platforms),
                   'downloads_count': random.randint(1000, 1500),
                   'users_count': random.randint(10000, 15000),
                   'add_on': 1}
