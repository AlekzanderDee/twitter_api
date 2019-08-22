import logging

import click
from api.tweets_consumer import Consumer


logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s :: %(name)s :: %(lineno)d')
logger = logging.getLogger(__name__)


# Use a group so that these commands are only accessible from this file
@click.group()
def twitter_cli():
    pass


@twitter_cli.command('stream-tweets')
@click.option('-t', '--track', default=None, help='A comma-separated list of phrases which will be used to determine what Tweets will be delivered on the stream.')
@click.option('-k', '--key', default=None, help='Client API key')
@click.option('-s', '--secret_key', default=None, help='Client API secret key')
def stream_tweets(secret_key, key, track):
    if not all((secret_key, key, track)):
        logger.error('You must specify all parameters. Use --help option to get information about the inputs')
        return
    reader = Consumer(key, secret_key)
    reader.filter_tweets(track)


if __name__ == '__main__':
    twitter_cli()
