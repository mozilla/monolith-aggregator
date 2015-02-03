from os.path import join as pjoin
from fabric.api import env, execute, lcd, local, task
from fabdeploytools import helpers
import fabdeploytools.envs
import deploysettings as settings
import ConfigParser
import time

env.key_filename = settings.SSH_KEY
fabdeploytools.envs.loadenv(settings.CLUSTER)

ROOT, MONOLITH = helpers.get_app_dirs(__file__)

VIRTUALENV = pjoin(ROOT, 'venv')
PYTHON = pjoin(VIRTUALENV, 'bin', 'python')

config = ConfigParser.ConfigParser()
config.readfp(open('aggregator.ini'))


@task
def create_virtualenv():
    helpers.create_venv(VIRTUALENV, settings.PYREPO,
                        pjoin(MONOLITH, 'requirements/prod.txt'))


@task
def deploy():
    helpers.deploy(
        name='monolith-aggregator',
        env=settings.ENV,
        cluster=settings.CLUSTER,
        domain=settings.DOMAIN,
        root=ROOT,
        deploy_roles='local',
        package_dirs=['monolith-aggregator', 'venv'])


@task
def pre_update(ref):
    execute(helpers.git_update, MONOLITH, ref)


@task
def update():
    execute(create_virtualenv)
    with lcd(MONOLITH):
        local('%s setup.py develop --no-deps' % PYTHON)
        local('%s /usr/bin/virtualenv --relocatable %s' % (PYTHON, VIRTUALENV))


@task
def build():
    execute(create_virtualenv)
    with lcd(MONOLITH):
        local('%s setup.py develop --no-deps' % PYTHON)
        local('%s /usr/bin/virtualenv --relocatable %s' % (PYTHON, VIRTUALENV))


@task
def deploy_jenkins():
    rpm = helpers.build_rpm(name='monolith-aggregator',
                            env=settings.ENV,
                            cluster=settings.CLUSTER,
                            domain=settings.DOMAIN,
                            package_dirs=['monolith-aggregator', 'venv'],
                            root=ROOT)

    rpm.local_install()


@task
def reindex(startdate, enddate=None):
    """
    By default reindex all events from startdate to today.
    If limiting range using enddate it must be set to the last day of a month.
    """

    from datetime import datetime, date

    startdate = datetime.strptime(startdate, '%Y-%m-%d').date()

    if enddate:
        enddate = datetime.strptime(enddate, '%Y-%m-%d').date()
    else:
        enddate = date.today()

    delete_indices(startdate, enddate)
    delete_records(startdate, enddate)
    index_dates(startdate, enddate)


def delete_indices(startdate, enddate):
    from datetime import datetime, date

    ES_URL = config.get('target:es', 'url')
    ES_PREFIX = ''

    if config.has_option('target:es', 'prefix'):
        ES_PREFIX = config.get('target:es', 'prefix')

    # delete old indicies
    for month in range(startdate.month, enddate.month + 1):
        local('curl -XDELETE %s/%stime_%s-%s' %
              (ES_URL, ES_PREFIX, startdate.year, "%02d" % (month,)))


def delete_records(startdate, enddate):
    from datetime import datetime, date
    from sqlalchemy import create_engine, MetaData, Table

    db_uri = config.get('monolith', 'history')
    engine = create_engine(db_uri, echo=False)
    metadata = MetaData()

    record = Table('record', metadata, autoload=True, autoload_with=engine)
    transaction = Table('monolith_transaction', metadata,
                        autoload=True, autoload_with=engine)

    delete_record = record.delete(record.c.date.between(startdate, enddate))
    delete_transaction = transaction.delete(transaction.c.date.between(startdate, enddate))
    engine.execute(delete_record)
    engine.execute(delete_transaction)


def index_dates(startdate, enddate=None):
    from datetime import date, datetime, timedelta

    delta = enddate - startdate

    for i in range(delta.days + 1):
        rundate = startdate + timedelta(days=i)
        with lcd(MONOLITH):
            local('%s ../venv/bin/monolith-extract aggregator.ini '
                  '--log-level debug --start-date %s '
                  '--end-date %s' % (PYTHON, rundate, rundate))
