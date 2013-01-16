import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

requires = ['sqlalchemy', 'bson']
test_requires = requires + ['unittest2', ]

setup(
    name='monolith-aggregator',
    version='0.1',
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
    extras_require={
        'test': test_requires,
    },
    tests_require=test_requires,
    test_suite="aggregator",
)
