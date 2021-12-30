# Hashtag-Comparison
## Introduction
The system provides live count and average sentiment of tweets with a particular hashtag(s) over a specified period of time.

The following shows the result for a time period of 30 seconds for 2 hashtags.

![](https://img.shields.io/badge/python-3.8.11-brightgreen)
![](https://img.shields.io/badge/Spark-3.0.3-green)
![](https://img.shields.io/badge/kafka--python-2.0.2-yellowgreen)
![](https://img.shields.io/badge/tweepy-4.4.0-yellow)
![](https://img.shields.io/badge/textblob-0.15.3-orange)
![](https://img.shields.io/badge/pymongo-4.0.1-red)
![](https://img.shields.io/badge/MongoDB-5.0-lightgrey)
![](https://img.shields.io/badge/Kafka-3.0-blue)

![](https://github.com/DebangshuB/Hashtag-Comparison/blob/main/Images/1.png)

## Producer
Using Tweepy Streaming, live tweets on a pre-defined list of hashtags were parsed and sent to a Kafka topic.

## Processor
The live tweet stream is read from the Kafka topic and aggregated to provide the number of tweets in the given time frame and the average sentiment (using TextBlob, ranges from -1 to 1) for a given hashtag(s).
The aggregation is done on Spark using PySpark in python and sent to another Kafka topic.

## Consumer
The hashtag data is read from a Kafka topic and written into a MongoDB collection.

## Backend and Frontend
The NodeJS server gets the most relevant records from the MongoDB collection and displays it on the webpage (This part has a little work left).


