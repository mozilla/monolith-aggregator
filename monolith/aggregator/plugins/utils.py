# Some utility that different plugins share.
import os
import hashlib

from ConfigParser import ConfigParser
from datetime import datetime
from oauth_hook import OAuthHook
from urlparse import urljoin

from requests import Request, Session

from monolith.aggregator import logger
from monolith.aggregator.plugins import Plugin

_ISO = '%Y-%m-%dT%H:%M:%S'


def iso2datetime(data):
    return datetime.strptime(data, _ISO)


class TastypieReader(Plugin):

    def __init__(self, **options):
        super(TastypieReader, self).__init__(**options)
        self.session = Session()
        self.create_oauth_hook(**options)

    def create_oauth_hook(self, **kwargs):
        self.oauth_hook = None

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

                self.oauth_hook = OAuthHook(consumer_key=key.hexdigest(),
                                            consumer_secret=secret.hexdigest(),
                                            header_auth=True)

    def delete(self, url, params):
        req = Request('DELETE', url, params=params)
        if self.oauth_hook:
            self.oauth_hook(req)

        return self.session.send(req.prepare())

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

        req = Request('GET', url, params=params)
        if self.oauth_hook:
            self.oauth_hook(req)

        resp = self.session.send(req.prepare())

        if 400 <= resp.status_code <= 499:
            logger.error('API 4xx Error: %s Url: %s' %
                         (resp.json()['reason'], url))
            return data

        if 500 <= resp.status_code <= 599:
            logger.error('API 5xx Error: %s Url: %s' % (resp.text, url))
            return data

        res = resp.json()
        data.extend(res['objects'])

        # we can have paginated elements, so we need to get them all
        if 'meta' in res and res['meta']['next']:
            self.read_api(urljoin(url, res['meta']['next']), data=data)
        return data
