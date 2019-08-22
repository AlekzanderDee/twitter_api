import logging
import json
import queue

from .models import User, Tweet

logger = logging.getLogger(__name__)


class TweetsProcessor:
    def __init__(self, input_queue, message_queue, barrier, stop_event, encoding='utf-8', filename='./output.csv'):
        """
        TweetsProcessor class provides a functionality for processing fetched tweets and dumping them to the file

        Args:
            input_queue: input Queue instance (keeps fetched tweets)
            message_queue: message Queue instance (keeps only uniqueue messages)
            barrier: synchronization primitive to sync all threads
            stop_event: threading Event instance. Is set when either time is out or a message queue is full
            encoding: encoding string
            filename: filename to dump fetched tweets to
        """
        self.input_queue = input_queue
        self.message_queue = message_queue
        self.barrier = barrier
        self.stop_event = stop_event
        self.encoding = encoding
        self.message_ids = set()
        self.filename = filename

    @staticmethod
    def _check_is_tweet(data):
        """
        Check if a supplied payload looks like a tweet.
        Args:
            data: Python dict object with a payload that should be tested

        Returns:
            bool, True id the data looks like a tweet

        """
        event_keys = {'limit', 'disconnect', 'warning', 'delete', 'scrub_geo', 'status_withheld',
                      'user_withheld', 'event'}
        if set(data.keys()) & event_keys:
            return False
        return True

    def _accumulate_messages(self, stop_event):
        """
        Process the input queue and accumulate unique (based on ID) tweets in the message queue

        Args:
            stop_event: threading Event instance

        Returns:
            None
        """
        # While there is anything in the input queue OR STOP event is not set, process the data
        while not self.input_queue.empty() or not stop_event.is_set():
            try:
                message = self.input_queue.get(timeout=1)
            except queue.Empty:
                # let another iteration of the loop
                continue

            decoded_line = message.decode(self.encoding)
            json_message = json.loads(decoded_line)

            if not self._check_is_tweet(json_message):
                logger.warning('Do not process a message that seems like an event: %s', json_message)
                continue

            logger.info('Received a message (ID %s)', json_message['id_str'])
            if not self.message_queue.full() and json_message['id_str'] not in self.message_ids:
                self.message_queue.put(json_message)
                self.message_ids.add(json_message['id'])
            else:
                logger.error(
                    'Did not add message (ID %s) to the message_queue (possible duplicate or the message queue is full)',
                    json_message['id_str']
                )

    def _output_data(self):
        """
        Dump information from the message queue to a file

        Returns:
            None
        """
        logger.info('Exporting messages...')
        user_mapping = {}
        while not self.message_queue.empty():
            message = self.message_queue.get()

            user_id_str = message['user']['id_str']
            if user_id_str in user_mapping:
                user = user_mapping[user_id_str]
            else:
                user = User.from_dict(message['user'])
                if user:
                    user_mapping[user_id_str] = user
                else:
                    logger.error('Can not instantiate User object from the payload. Skipping entry...')
                    continue

            tweet = Tweet.from_dict(message)
            if tweet:
                user.add_tweet(tweet)
            else:
                logger.error('Can not instantiate Tweet object from the payload. Skipping entry...')

        users = sorted(user_mapping.values(), key=lambda user: user.created_at)
        separator = '\t'
        logger.info('Writing messages to the file "%s"', self.filename)
        with open(self.filename, 'w') as f:
            f.write(separator.join((
                'Message ID',
                'Message creation date (epoch)',
                'Text',
                'User ID',
                'User creation date (epoch)',
                'User name',
                'User screen name'
            )))
            for user in users:
                for tweet in user.tweets:
                    f.write('\r\n')
                    f.write(separator.join((
                        tweet.id_str,
                        str(tweet.created_at.timestamp()),
                        tweet.text.encode('unicode_escape').decode(self.encoding),
                        user.id_str,
                        str(user.created_at.timestamp()),
                        user.name,
                        user.screen_name
                    )))

    def start(self):
        """
        Start processor: accumulate messages til the STOP event is set, than dump them to the file

        Returns:
            None
        """
        self.barrier.wait()
        logger.info('Starting TweetsProcessor')
        self._accumulate_messages(self.stop_event)
        self._output_data()
