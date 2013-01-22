import os
from setuptools import setup, find_packages
from aggregator import __version__


here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

requires = [
    'pyes',
    'SQLAlchemy',
    'gevent',
    'gdata'
]

test_requires = requires + [
    'coverage',
    'nose',
    'unittest2',
]


setup(name='monolith-aggregator',
      version=__version__,
      description='The monolith aggregator',
      long_description=README,
      classifiers=[
          "Programming Language :: Python",
          "Programming Language :: Python :: 2",
          "Programming Language :: Python :: 2.6",
          "Programming Language :: Python :: 2.7",
      ],
      author='Mozilla Services',
      author_email='services-dev@mozilla.org',
      url='https://github.com/mozilla/monolith-aggregator',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=test_requires,
      test_suite="aggregator",
      extras_require={'test': test_requires},
      entry_points="""
      [console_scripts]
      monolith-extract = aggregator.extract:main
      """)
