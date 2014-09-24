from os.path import join as pjoin

from fabric.api import env, execute, lcd, local, task

from fabdeploytools import helpers
import fabdeploytools.envs

import deploysettings as settings


env.key_filename = settings.SSH_KEY
fabdeploytools.envs.loadenv(settings.CLUSTER)

ROOT, MONOLITH = helpers.get_app_dirs(__file__)

VIRTUALENV = pjoin(ROOT, 'venv')
PYTHON = pjoin(VIRTUALENV, 'bin', 'python')


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
        deploy_roles=['web'],
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
def reindex(startdate):
    delete_indices(startdate)
    delete_records(startdate)
    index_dates(startdate)


@task
def delete_indices(startdate):
    from datetime import datetime, date

    ES_URL = config.get('target:es', 'url')
    ES_PREFIX = config.get('target:es', 'prefix')

    today = date.today()
    previous = datetime.strptime(startdate, '%Y-%m-%d')

    # delete old indicies
    for month in range(previous.month, today.month + 1):
        local('curl -XDELETE %s/%stime_%s-%s' %
              (ES_URL, ES_PREFIX, today.year, "%02d" % (month,)))


@task
def delete_records(startdate):
    from datetime import datetime, date
    from sqlalchemy import create_engine, MetaData, Table

    db_uri = config.get('monolith', 'history')
    engine = create_engine(db_uri, echo=False)
    metadata = MetaData()

    today = date.today()

    record = Table('record', metadata, autoload=True, autoload_with=engine)
    transaction = Table('monolith_transaction', metadata,
                        autoload=True, autoload_with=engine)

    delete_record = record.delete(record.c.date.between(startdate, today))
    delete_transaction = transaction.delete(transaction.c.date.between(startdate, today))
    engine.execute(delete_record)
    engine.execute(delete_transaction)


@task
def index_dates(startdate):
    from datetime import date, datetime, timedelta

    today = date.today()
    previous = datetime.strptime(startdate, '%Y-%m-%d').date()

    delta = today - previous

    for i in range(delta.days + 1):
        rundate = previous + timedelta(days=i)
        with lcd(MONOLITH):
            local('%s ../venv/bin/monolith-extract aggregator.ini '
                  '--log-level debug --start-date %s '
                  '--end-date %s' % (PYTHON, rundate, rundate))
