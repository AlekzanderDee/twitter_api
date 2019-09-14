import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class User:
    """
    Basic User model
    """

    def __init__(self, id_str=None, name=None, screen_name=None, created_at=None, **kwargs):
        self.id_str = id_str
        self.name = name
        self.screen_name = screen_name
        self.created_at = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y')

        self._tweets = set()

    @classmethod
    def from_dict(cls, user_payload):
        user_fields = {'id_str', 'name', 'screen_name', 'created_at'}

        if not set(user_payload.keys()) >= user_fields:
            logger.error('Invalid payload supplied during the user creation: %s', user_payload)
            return None

        return cls(
            id_str=user_payload['id_str'],
            name=user_payload['name'],
            screen_name=user_payload['screen_name'],
            created_at=user_payload['created_at'],
        )

    def __hash__(self):
        return hash(self.id_str)

    def __eq__(self, other):
        return self.id_str == other.id_str

    def __repr__(self):
        return f'ID {self.id_str} {self.name}'

    def add_tweet(self, tweet):
        self._tweets.add(tweet)

    @property
    def tweets(self):
        return sorted(self._tweets, key=lambda tweet: tweet.created_at)


class Tweet:
    """
    Basic Tweet model
    """

    def __init__(self, id_str=None, created_at=None, text=None, **kwargs):
        self.id_str = id_str
        self.created_at = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y')
        self.text = text

    @classmethod
    def from_dict(cls, tweet_payload):
        tweet_fields = {'id_str', 'created_at', 'text'}

        if not set(tweet_payload.keys()) >= tweet_fields:
            logger.error('Invalid payload supplied during the tweet creation: %s', tweet_payload)
            return None

        return cls(
            id_str=tweet_payload['id_str'],
            created_at=tweet_payload['created_at'],
            text=tweet_payload['text'],
        )

    def __hash__(self):
        return hash(self.id_str)

    def __eq__(self, other):
        return self.id_str == other.id_str

    def __repr__(self):
        return f'ID {self.id_str} {self.created_at}'
