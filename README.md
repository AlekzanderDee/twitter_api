## Twitter API (research)

This project is to research a set of Twitter APIs. 
Project provides a CLI tool that can be used to interact with the Twitter ecosystem.

## Language
Python 3.7.3

### Setup
1. Create Python virtual environment
2. Activate virtual environment and install 
all dependencies from `requirements.txt`
3. Make sure you have Twitter Consumer API Key (client key) and 
Twitter Consumer API Secret Key (secret key) available

### Running the CLI
You can get quick help information about the usage by typing and executing the following command:
```
python twitter_cli.py --help
```

#### Stream-tweets 
At the moment, CLI provides only a single command: `stream-tweets`.
Help information is also available for the command:
```
python twitter_cli.py stream-tweets --help
```

Command execution fetches tracking tweets from the Twitter stream endpoint 
and dumps received information into a tab-separated file. File contains the following headers:
- Message ID
- Message creation date (epoch)
- Text
- User ID
- User creation date (epoch)
- User name
- User screen name

The information is grouped by user and ordered ascending by the  `User creation date (epoch)` and then by the `Message creation date (epoch)` 
(please note that ordering is based on the creation data value, not the age of the user or wteet)

**Example usage**

Fetching all tweets that track "bieber":
```
python twitter_cli.py stream-tweets -t bieber -k APIKEYSTR -s APISECRETSTR
```
Please note, that you have to use your API key and API secret key values instead of `APIKEYSTR` and `APISECRETSTR`
The example output can be found in the `output.scv` file.

## TODO
- Allow passing more parameters to the CLI (filename)
- Improve constants management (URLs)
- Improve data serialization
- Add more error management (especially, around the code that works with `requests` module)
- Manage disconnections (add exponential back-off)
- Respect event messages from the stream
- Add more tests
