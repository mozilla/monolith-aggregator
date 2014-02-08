# Some utility that different plugins share.
import os
import hashlib

from ConfigParser import ConfigParser
from datetime import datetime
from urlparse import parse_qsl, urlparse

from requests import Session
from requests_oauthlib import OAuth1Session

from monolith.aggregator import logger
from monolith.aggregator.plugins import Plugin
from monolith.aggregator.exception import ServerError


_ISO_DATETIME = '%Y-%m-%dT%H:%M:%S'
_ISO_DATE = '%Y-%m-%d'


def iso2datetime(data):
    try:
        return datetime.strptime(data, _ISO_DATETIME)
    except ValueError:
        # Maybe it's a date instead of a datetime.
        return datetime.strptime(data, _ISO_DATE)


class TastypieReader(Plugin):

    def __init__(self, **options):
        super(TastypieReader, self).__init__(**options)
        self.session = self._get_session(**options)

    def _get_session(self, **kwargs):
        if 'password-file' in kwargs:
            passwd = kwargs['password-file']
            if not os.path.exists(passwd):
                raise ValueError('%r not found.' % passwd)

            parser = ConfigParser()
            parser.read(passwd)
            username = parser.get('auth', 'username', None)
            password = parser.get('auth', 'password', None)

            if username and password:
                key = hashlib.sha512(password + username + 'key')
                secret = hashlib.sha512(password + username + 'secret')

                return OAuth1Session(key.hexdigest(), secret.hexdigest())
        else:
            return Session()

    def delete(self, url, params):
        return self.session.delete(url, params=params)

    def read_api(self, url, params=None, data=None):
        """Reads an API, follows pagination and return the resulting objects.

        :param url: Url to read from.
        :param params: List of params to pass in the querystring (filters).
        :param data: Used to handle recursive calls. The data being retrieved.

        """
        if data is None:
            data = []
        if not params:
            params = {}

        orig_params = params.copy()

        while True:
            resp = self.session.get(url, params=params)

            if 400 <= resp.status_code <= 499:
                logger.error('API 4xx Error: %s Url: %s' %
                             (resp.json()['reason'], url))
                return data

            if 500 <= resp.status_code <= 599:
                logger.error('API 5xx Error: %s Url: %s' % (resp.text, url))
                raise ServerError(resp.status_code)

            res = resp.json()
            data.extend(res['objects'])

            # we can have paginated elements, so we need to get them all
            next_ = None
            if 'meta' in res:
                next_ = res['meta'].get('next')

            if next_:
                # Update the params to pick up the new offset.
                params = orig_params.copy()
                qs = urlparse(next_).query
                params.update(dict(parse_qsl(qs)))
            else:
                return data
