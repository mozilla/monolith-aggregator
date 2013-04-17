import os
from datetime import date, timedelta

last_year = date.today() - timedelta(days=365)


query = "select * from  %(table)s where date > %(date)s"

options = {'user':  'addons_dev',
           'password': 'XXXXw',
           'host': 'db-amo-dev-cluster-rw',
           'db': 'addons_dev_allizom_org_new',
           'date': last_year.strftime('%Y-%m-%d')}

cmd = "mysql -u%(user)s -p%(password)s --host %(host)s %(db)s -e '%(query)s' | sed -e 's/    /,/g'  > /tmp/%(table)s.csv"

tables = ['stats_addons_collections_counts',
          'stats_collections',
          'stats_collections_counts',
          'stats_collections_share_counts',
          'stats_share_counts',
          'global_stats']


for table in tables:
    options['table'] = table
    options['query'] = query % options
    print cmd % options
    os.system(cmd % options)

