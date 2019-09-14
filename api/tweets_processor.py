import csv
import logging
import json
import queue

from .models import User, Tweet

logger = logging.getLogger(__name__)


class TweetsWriter(csv.DictWriter):

    def __init__(self, f, restval="", extrasaction="raise",
                 dialect="excel", *args, **kwds):
        fieldnames = ['tweet_str_id', 'tweet_creation_dt', 'tweet_text',
                      'user_str_id', 'user_creation_dt', 'user_name', 'user_screen_name']
        self.header = {
            'tweet_str_id': 'Message ID',
            'tweet_creation_dt': 'Message creation date (epoch)',
            'tweet_text': 'Text',
            'user_str_id': 'User ID',
            'user_creation_dt': 'User creation date (epoch)',
            'user_name': 'User name',
            'user_screen_name': 'User screen name'
        }

        super().__init__(f, fieldnames, restval, extrasaction, dialect, *args, **kwds)

    def writeheader(self):
        self.writerow(self.header)


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
        if not data or not isinstance(data, dict):
            return False
        data_keys_set = set(data.keys())

        event_keys = {'limit', 'disconnect', 'warning', 'delete', 'scrub_geo', 'status_withheld',
                      'user_withheld', 'event'}

        if not data or data_keys_set & event_keys:
            return False

        message_keys = {'id_str', 'created_at', 'text', 'user'}
        if not message_keys <= data_keys_set:
            return False

        return True

    def _accumulate_messages(self):
        """
        Process the input queue and accumulate unique (based on ID) tweets in the message queue

        Returns:
            None
        """

        while not self.stop_event.is_set():
            try:
                message = self.input_queue.get(timeout=1)
            except queue.Empty:
                # let another iteration of the loop
                continue

            decoded_line = message.decode(self.encoding)
            json_message = json.loads(decoded_line)

            if not self._check_is_tweet(json_message):
                logger.warning('A message does not look like an event: %s. Skipping that message.', json_message)
                continue

            logger.info('Received a message (ID %s)', json_message['id_str'])
            if json_message['id_str'] not in self.message_ids:
                try:
                    self.message_queue.put(json_message, timeout=0)
                except queue.Full:
                    logger.warning('The message queue is already full')
                    # In this case, mos likely, the STOP_EVENT will be set by Limiter on the next iteration
                    # and loop will quit
                    continue
                self.message_ids.add(json_message['id_str'])
            else:
                logger.error(
                    'Did not add duplicated message (ID %s) to the message_queue',
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
            # Expecting only one queue consumer
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
        logger.info('Writing messages to the file "%s"', self.filename)
        with open(self.filename, 'w', newline='') as csvfile:
            writer = TweetsWriter(csvfile, delimiter='\t', quoting=csv.QUOTE_NONNUMERIC)
            writer.writeheader()

            for user in users:
                for tweet in user.tweets:
                    writer.writerow({
                        'tweet_str_id': tweet.id_str,
                        'tweet_creation_dt': str(tweet.created_at.timestamp()),
                        'tweet_text': tweet.text.encode('unicode_escape').decode(self.encoding),
                        'user_str_id': user.id_str,
                        'user_creation_dt': str(user.created_at.timestamp()),
                        'user_name': user.name,
                        'user_screen_name': user.screen_name
                    })

    def start(self):
        """
        Start processor: accumulate messages til the STOP event is set, then dump them to the file

        Returns:
            None
        """
        self.barrier.wait()
        logger.info('Starting TweetsProcessor')
        self._accumulate_messages()
        self._output_data()
