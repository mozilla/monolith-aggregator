import os
import sys
from setuptools import setup, find_packages

from monolith.aggregator import __version__


here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()
with open(os.path.join(here, 'CHANGES.rst')) as f:
    CHANGES = f.read()

PYTHON26 = sys.version_info < (2, 7)

requires = [
    'gevent==1.0rc2',
    'oauth2client==1.0',
    'pyelasticsearch==0.3',
    'PyMySQL==0.5',
    'python-gflags==2.0',
    'SQLAlchemy==0.8.0b2',
    'google-api-python-client==1.0',
    'requests==1.1.0',
    'requests-oauth==0.4.1',
]


if PYTHON26:
    requires.extend([
        'argparse==1.2.1',
        'simplejson==3.1.0',
    ])

test_requires = requires + [
    'coverage',
    'nose',
    'pyelastictest',
    'Sphinx',
    'unittest2',
    'HTTPretty',
]


setup(name='monolith.aggregator',
      version=__version__,
      description='Mozilla Monolith Aggregator',
      long_description=README + '\n\n' + CHANGES,
      classifiers=[
          "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
          "Programming Language :: Python",
          "Programming Language :: Python :: 2",
          "Programming Language :: Python :: 2.6",
          "Programming Language :: Python :: 2.7",
      ],
      author='Mozilla Services',
      author_email='services-dev@mozilla.org',
      url='https://github.com/mozilla/monolith-aggregator',
      license="MPLv2.0",
      packages=find_packages(),
      namespace_packages=['monolith'],
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=test_requires,
      test_suite="monolith.aggregator.tests",
      extras_require={'test': test_requires},
      entry_points="""
      [console_scripts]
      monolith-extract = monolith.aggregator.extract:main
      monolith-ga-oauth = tools.auth_google_analytics:main
      """)
