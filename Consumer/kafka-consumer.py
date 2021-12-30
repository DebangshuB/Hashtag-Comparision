from kafka import KafkaConsumer
from pprint import pprint
from datetime import datetime
import pymongo

import json
parameters_json = open("../parameters.json")
parameters = json.load(parameters_json)
    
"""
    MongoDB Connections
"""

# Setting up the initial connection
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Creating if the database doesn't exist and connecting
db = client["hashtag-stats"]

# Creating if the collection doesn't exist and connecting
coll = db["tweets"]

"""
    Instantiating the Kafka Consumer
"""

consumer = KafkaConsumer(
    parameters["topic-name"]["batches"],
    bootstrap_servers=parameters["bootstrap-servers"],
    auto_offset_reset=parameters["auto-offset-reset"]["consumer"],
    enable_auto_commit=True
)

print("Consumer Connected!")

for message in consumer:
    message = message.value
    message = message.decode('UTF-8')
    message = json.loads(message)
    
    message["window"]["end"] = datetime.strptime(message["window"]["end"], "%Y-%m-%dT%H:%M:%S.%f%z")
    message["window"]["start"] = datetime.strptime(message["window"]["start"], "%Y-%m-%dT%H:%M:%S.%f%z")
    
    id_ = coll.insert_one(message)
    print("Inserted {} at {}".format(id_.inserted_id, datetime.now()))
