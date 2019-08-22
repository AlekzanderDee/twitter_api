import logging
import urllib.parse as urlparse
from abc import ABC, abstractmethod

from requests_oauthlib import OAuth1Session

logger = logging.getLogger(__name__)

ROOT_TWITTER_URL = 'https://api.twitter.com/'
OAUTH_FRAGMENT = '/oauth/'


class BaseAuthenticator(ABC):
    """
    Base authentication class.
    """

    @abstractmethod
    def authenticate(self):
        raise NotImplemented('Method not implemented')

    @abstractmethod
    def provide_auth(self):
        raise NotImplemented('Method not implemented')


class PINAuthenticator(BaseAuthenticator):

    def __init__(self, api_key, api_secret_key):
        """
        PINAuthenticator implements PIN-based OAuth

        Args:
            api_key: application API Key
            api_secret_key: application API secret Key
        """
        self.api_key = api_key
        self.api_secret_key = api_secret_key
        self.oauth_token = None
        self.oauth_token_secret = None
        self.access_token = None
        self.access_token_secret = None

        self.oauth1 = OAuth1Session(self.api_key, client_secret=self.api_secret_key)
        self.oauth_urls = {
            'request_token': urlparse.urljoin(ROOT_TWITTER_URL, OAUTH_FRAGMENT + 'request_token'),
            'access_token': urlparse.urljoin(ROOT_TWITTER_URL, OAUTH_FRAGMENT + 'access_token'),
            'authorize': urlparse.urljoin(ROOT_TWITTER_URL, OAUTH_FRAGMENT + 'authorize'),
        }

    def _fetch_request_token(self):
        """
        Fetch and set request tokens, which will be used during the access token acquisition

        Returns:
            tuple of token and secret token

        """
        logger.info('Fetching a request token...')
        resp = self.oauth1.fetch_request_token(self.oauth_urls['request_token'])
        self.oauth_token = resp.get('oauth_token')
        self.oauth_token_secret = resp.get('oauth_token_secret')
        logger.info('Request token is fetched')
        return self.oauth_token, self.oauth_token_secret

    def _fetch_access_token(self, pin):
        """
        Fetch access token using the request tokens.

        Args:
            pin: A verifier string to prove authorization was granted.

        Returns:
            tuple of access token and secret access token

        """
        logger.info('Fetching a access token...')
        self.oauth1 = OAuth1Session(self.api_key, client_secret=self.api_secret_key, verifier=pin,
                                    resource_owner_key=self.oauth_token, resource_owner_secret=self.oauth_token_secret)
        resp = self.oauth1.fetch_access_token(self.oauth_urls['access_token'])
        self.access_token = resp['oauth_token']
        self.access_token_secret = resp['oauth_token_secret']
        logger.info('Access token is fetched')
        return self.access_token, self.access_token_secret

    def authenticate(self):
        """
        Guide a user through the authentication flow.

        Returns:
            tuple of access token and secret access token

        """
        self._fetch_request_token()
        authorize_url = self.oauth_urls['authorize']
        print('Please visit this authorization URL to retrieve a PIN: ' + self.oauth1.authorization_url(authorize_url))
        pin = input('PIN: ').strip()
        return self._fetch_access_token(pin)

    def provide_auth(self):
        """
        Return the auth object that may be used for signing the HTTP requests.

        Returns:
            OAuth instance
        """
        return self.oauth1.auth
