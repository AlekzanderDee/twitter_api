import logging

import requests


logger = logging.getLogger(__name__)


class TweetsStreamer:

    def __init__(self, api_key, api_secret_key, barrier, input_queue, stop_event,
                 stream_version='1.1', encoding='utf-8', auth_cls=None):
        """
        TweetsStreamer class provides a functionality to read from the tweets stream

        Args:
            api_key: client API key
            api_secret_key: client API secret key
            barrier: barrier: synchronization primitive to sync all threads
            input_queue: Queue instance to put all messages from the stream
            stop_event: threading Event instance. Is set when either time is out or a message queue is full
            stream_version: Twitter stream version
            encoding: encoding string
            auth_cls: authenticator class that provides OAuth layer
        """
        self.api_key = api_key
        self.api_secret_key = api_secret_key
        self.barrier = barrier
        self.input_queue = input_queue
        self.stop_event = stop_event
        self.auth = None
        self.session = None
        self.encoding = encoding
        self.auth_cls = auth_cls
        self.stream_root_url = f'https://stream.twitter.com/{stream_version}'

    def _authenticate(self):
        """
        Using the auth class, authenticate application for further streaming from protected endpoints

        Returns:
            None
        """
        if self.auth_cls and not self.auth:
            authenticator = self.auth_cls(self.api_key, self.api_secret_key)
            authenticator.authenticate()
            self.auth = authenticator.provide_auth()

    def _read_stream(self, url, body):
        """
        Read from stream and yield none-empty message (omitting ping-alive empty strings)
        Args:
            url: stream URL
            body: request body

        Returns:
            yields messages

        """
        logger.info('Reading from stream...')
        resp = requests.post(url, stream=True, auth=self.auth, data=body)
        for line in resp.iter_lines():
            if self.stop_event.is_set():
                logger.info('Exiting the stream')
                break
            # filter out 'keep-alive' empty lines
            if line:
                yield line

    def filter_tweets(self, track):
        """
        Fetch tweets from the streaming endpoint and store them in the queue

        Args:
            track: string that represents a comma-separated list of phrases which will be used to determine what
            Tweets will be delivered on the stream

        Returns:
            None
        """
        try:
            self._authenticate()
        except Exception:
            logger.exception('Authentication failed')
            self.stop_event.set()
            self.barrier.wait()
            return

        url = self.stream_root_url + '/statuses/filter.json'
        body = {'track': track}
        self.barrier.wait()

        for line in self._read_stream(url, body):
            self.input_queue.put(line)
