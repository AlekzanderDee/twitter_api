import json
from queue import Queue
from threading import Event, Timer
from unittest import TestCase

from api.tweets_processor import TweetsProcessor


class ProcessorTestCase(TestCase):

    def test_check_is_tweet_false(self):
        """
        Test invalid payloads are not considered as tweets
        """
        self.assertFalse(TweetsProcessor._check_is_tweet({}))
        self.assertFalse(TweetsProcessor._check_is_tweet({'foo': 'bar'}))
        self.assertFalse(TweetsProcessor._check_is_tweet({'limit': {}}))
        self.assertFalse(TweetsProcessor._check_is_tweet({'disconnect': {}}))
        self.assertFalse(TweetsProcessor._check_is_tweet({'warning': {}}))
        self.assertFalse(TweetsProcessor._check_is_tweet({'delete': {}}))
        self.assertFalse(TweetsProcessor._check_is_tweet({'scrub_geo': {}}))
        self.assertFalse(TweetsProcessor._check_is_tweet({'status_withheld': {}}))
        self.assertFalse(TweetsProcessor._check_is_tweet({'user_withheld': {}}))
        self.assertFalse(TweetsProcessor._check_is_tweet({'event': {}}))
        self.assertFalse(TweetsProcessor._check_is_tweet(
            {'event': {},'id_str': '123', 'created_at': 'Foo', 'text': 'bar', 'user': {}})
        )

    def test_check_is_tweet_true(self):
        """
        Test valid payloads are considered as tweets
        """
        self.assertTrue(
            TweetsProcessor._check_is_tweet({'id_str': '123', 'created_at': 'Foo', 'text': 'bar', 'user': {}})
        )

    def test_message_queue_limit(self):
        """
        Test that processor terminates when the message queue is full
        """
        input_queue = Queue()
        message_queue = Queue(10)
        barrier = None
        stop_event = Event()

        # Add 15 valid messages to the input queue
        for ind in range(15):
            tweet = json.dumps({
                'id_str': str(ind),
                'created_at': 'today',
                'text': 'Foo Bar',
                'user': {}
            }).encode()
            input_queue.put(tweet)

        processor = TweetsProcessor(input_queue, message_queue, barrier, stop_event)
        processor._accumulate_messages()

        self.assertTrue(message_queue.full())
        # We know that the inout queue still must hold some tweets
        self.assertFalse(input_queue.empty())

    def test_message_queue_uique_tweets(self):
        """
        Test that processor sends only unique (by ID) tweets to the message queue
        """
        input_queue = Queue()
        message_queue = Queue(10)
        barrier = None
        stop_event = Event()

        # Add 15 valid messages to the input queue
        for _ in range(15):
            tweet = json.dumps({
                'id_str': '123',
                'created_at': 'today',
                'text': 'Foo Bar',
                'user': {}
            }).encode()
            input_queue.put(tweet)

        processor = TweetsProcessor(input_queue, message_queue, barrier, stop_event)

        # Set timer so we can terminate the loop
        def _set_event(e):
            e.set()

        timer = Timer(2, _set_event, (stop_event, ))
        timer.start()

        processor._accumulate_messages()

        self.assertFalse(message_queue.full())

        # Message queue must have only one element
        message_queue.get()

        # After we retrieve a single element, the queue must be empty
        self.assertTrue(message_queue.empty())

        # Input queue must be empty
        self.assertTrue(input_queue.empty())
