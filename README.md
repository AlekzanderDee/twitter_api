## Twitter API (research project)

This project is to research streaming Twitter APIs. 
Project provides a CLI that can be used to fetch tweets from the stream endpoint and saving them to a file.

## Language
Python 3.7.3

### Setup
1. Create Python virtual environment
2. Activate virtual environment and install 
all dependencies from `requirements.txt`
3. Make sure you have Twitter Consumer API Key (client key) and 
Twitter Consumer API Secret key (secret key) available

### Running the CLI
You can get a help about the usage by typing:
```
python twitter_cli.py --help
```

At the moment the CLI provides only single command: `stream-tweets`. 
The help information is also available for this command:
```
python twitter_cli.py stream-tweets --help
```

#### Example usage
Fetching all tweets that track "madonna":
```
python twitter_cli.py stream-tweets -t madonna -k APIKEYSTR -s APISECRETSTR
```
Please note, that you have to use your API key and API secret key values instead of `APIKEYSTR` and `APISECRETSTR`

## TODO
- Improve constants management (URLs)
- Improve serialization
- Manage disconnections (add exponential back-off)
- Respect event messages from the stream
- Add more tests
