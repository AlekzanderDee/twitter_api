import logging
import queue
from threading import Thread, Event, Barrier

from .auth import PINAuthenticator
from .limiter import Limiter
from .tweets_processor import TweetsProcessor
from .tweets_streamer import TweetsStreamer

logger = logging.getLogger(__name__)


class TwitterAPI:
    def __init__(self, api_key, api_secret_key, time_limit=30, message_limit=100):
        """
        TwitterAPI class that provides a functionality to fetch tweets from the Streamer

        Args:
            api_key: client API Key
            api_secret_key: client API secret key
            time_limit: max number of seconds the streaming may run
            message_limit: max number of messages being fetched
        """
        self.api_key = api_key
        self.api_secret_key = api_secret_key
        self.streaming = False

        self.time_limit = time_limit
        self.message_limit = message_limit

        self.stop_event = Event()
        # will create 3 threads, to sync them we need a barrier for 3 parties
        self.barrier = Barrier(3)
        # thread responsible for handling stream request
        self.streamer_thread = None
        # thread responsible for processing fetched tweets
        self.processor_thread = None
        # thread responsible for limiting the fetching and processing
        self.limiter_thread = None

    def filter_tweets(self, track):
        """
        Fetch tweets from the stream endpoint with a use of multiple threads
        Args:
            track: string that represents a comma-separated list of phrases which will be used to determine
            what Tweets will be delivered on the stream

        Returns:
            None
        """
        if not track:
            return

        if self.streaming:
            logger.info("Prevent another attempt to start a stream: already streaming")
            return

        self.streaming = True

        logger.info('Creating queues...')
        input_queue = queue.Queue()
        messages_queue = queue.Queue(self.message_limit)

        logger.info('Initializing threads...')
        streamer = TweetsStreamer(self.api_key, self.api_secret_key, self.barrier, input_queue, self.stop_event,
                                  auth_cls=PINAuthenticator)
        self.streamer_thread = Thread(
            name='streamer',
            target=streamer.filter_tweets,
            args=(track, )
        )
        self.streamer_thread.start()

        limiter = Limiter(self.time_limit, self.message_limit, messages_queue, self.barrier, self.stop_event)
        self.limiter_thread = Thread(
            name='limiter',
            target=limiter.start,
        )
        self.limiter_thread.start()

        processor = TweetsProcessor(input_queue, messages_queue, self.barrier, self.stop_event)
        self.processor_thread = Thread(
            name='processor',
            target=processor.start,
        )
        self.processor_thread.start()

        logger.info('All threads started. Waiting for a completion...')
        self.streamer_thread.join()
        self.limiter_thread.join()
        self.processor_thread.join()
        logger.info('All threads completed')
        self.streaming = False
