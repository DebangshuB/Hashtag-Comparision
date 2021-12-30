from datetime import datetime
from pprint import pprint
from kafka import KafkaProducer
from api_keys import consumer_key, consumer_secret, access_token, access_token_secret
import tweepy

import json
parameters_json = open("../parameters.json")
parameters = json.load(parameters_json)


"""
    Defining the list of hashtags.

    Although they are hashtags,
    in a tweet the tagged hashtags do not have the # symbol.
"""

HASHTAGS_TEXT = set()

for tags in parameters["hashtags"]:
    HASHTAGS_TEXT.add(tags[1:])

"""
    KafkaPorducer -> Allows to send data
"""
producer = KafkaProducer(
    bootstrap_servers=parameters["bootstrap-servers"],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

"""
    Defining the parsing function for Tweet Object
"""


def process(data):

    # Decoding the raw byte string
    data = data.decode('UTF-8')

    # Parsing the JSON data
    data = json.loads(data)

    # Filling the return object with the required details
    obj = {}

    # obj["truncated"] = data["truncated"]
    # obj["retweeted"] = data.get("retweeted_status") != None

    # If tweet is truncated
    if data["truncated"]:
        obj["text"] = data["extended_tweet"]["full_text"]
        obj["hashtags"] = data["extended_tweet"]["entities"]["hashtags"]

    # If the tweet IS a retweet
    elif data.get("retweeted_status") is not None:

        # If the tweet IS a retweet and IS truncated
        if data["retweeted_status"]["truncated"]:
            obj["text"] = data["retweeted_status"]["extended_tweet"]["full_text"]
            obj["hashtags"] = data["retweeted_status"]["extended_tweet"]["entities"]["hashtags"]

        # If the tweet IS a retweet and NOT truncated
        else:
            obj["text"] = data["retweeted_status"]["text"]
            obj["hashtags"] = data["retweeted_status"]["entities"]["hashtags"]

    # If the tweet is NEITHER truncated NOR a retweet
    else:
        obj["text"] = data["text"]
        obj["hashtags"] = data["entities"]["hashtags"]

    # Tagged hashtags are in a weird format. Parsing those.
    curr_hashtags = []

    for d in obj["hashtags"]:
        if d["text"] in HASHTAGS_TEXT:
            curr_hashtags.append(d["text"])

    # If no hashtags are parsed, discard the tweet
    if len(curr_hashtags) == 0:
        return False

    obj["hashtags"] = curr_hashtags[0]
    return obj


"""
    Defining the Tweepy Stream Class
"""


class CustomStream(tweepy.Stream):

    def on_connect(self):
        print("Stream has connected!")

    def on_data(self, data):
        processed_data = process(data)

        if processed_data:
            producer.send(parameters["topic-name"]["tweets"], value=processed_data)
            print("Tweet sent at {}".format(datetime.now()))
            pprint(processed_data)

    def on_closed(self):
        print("Stream Closed.")

    def on_exception(self, exception):
        print(exception)

    def on_request_error(self, status_code):
        print(status_code)


"""
    Initializing the Tweepy Stream
"""

stream = CustomStream(
    consumer_key,
    consumer_secret,
    access_token,
    access_token_secret
)

stream.filter(track=parameters["hashtags"])
