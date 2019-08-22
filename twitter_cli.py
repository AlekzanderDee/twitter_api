import logging

import click
from api.twitter_api import TwitterAPI


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
@click.option('-T', '--time_limit', default=30, help='Maximum number of seconds CLI will consume from the stream (default 30)')
@click.option('-m', '--message_limit', default=100, help='Maximum number of tweets CLI will consume from the stream (default 100)')
def stream_tweets(message_limit, time_limit, secret_key, key, track):
    # TODO: Make checks to be more specific and test for age cases
    if not all((message_limit, time_limit, secret_key, key, track)):
        logger.error('You must specify all parameters. Use --help option to get information about the inputs')
        return
    reader = TwitterAPI(key, secret_key, time_limit, message_limit)
    reader.filter_tweets(track)


if __name__ == '__main__':
    twitter_cli()
