import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class Limiter:

    def __init__(self, time_limit, message_queue, barrier, stop_event):
        """
        Limiter class constructor.

        Args:
            time_limit: amount of seconds that limiter instance should wait till setting the STOP event
            message_queue: limited (by size) message queue
            barrier: synchronization primitive to sync all threads
            stop_event: threading Event instance. Is set when either time is out or a message queue is full
        """
        self.time_limit = time_limit
        self.message_queue = message_queue
        self.barrier = barrier
        self.stop_event = stop_event
        logger.info('Limiter is set with a %s sec time limit and %s max messages number', time_limit, message_queue)

    def start(self):
        """
        Start the limiter and wait till the time goes out or the message queue is full.
        """
        self.barrier.wait()
        logger.info('Starting Limiter')
        end_dt = datetime.utcnow() + timedelta(seconds=self.time_limit)
        while datetime.utcnow() <= end_dt and not self.stop_event.is_set():
            if self.message_queue.full():
                logger.warning('Message queue is full. Stopping further streaming')
                break

        logger.info('Emitting STOP event')
        self.stop_event.set()
