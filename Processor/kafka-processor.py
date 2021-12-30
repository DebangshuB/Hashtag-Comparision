import findspark
findspark.init("C:\\Spark\\spark-3.0.3-bin-hadoop2.7")

import json
parameters_json = open("../parameters.json")
parameters = json.load(parameters_json)

import datetime


from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from textblob import TextBlob

"""
    Run with

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 kafka-processor.py

    For Windows
"""

"""
    PySpark UDF to find the sentiment of a given line of text    
"""
udf_sentiment = udf(lambda text: TextBlob(text).sentiment.polarity)

spark = SparkSession \
.builder \
.appName("kafka-tweets-consumer") \
.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",parameters["bootstrap-servers"][0]) \
    .option("subscribe", parameters["topic-name"]["tweets"]) \
    .option("startingOffsets",parameters["auto-offset-reset"]["processor"]) \
    .option("failOnDataLoss", "false") \
    .load()

schema = StructType().add("hashtags", StringType()).add("text", StringType())

formatted_df = df \
    .select(col("timestamp") , from_json(col("value").cast("string"), schema)) \
    .select(col("timestamp") , col("from_json(CAST(value AS STRING)).*")) \
    .withColumn("text",udf_sentiment(col("text")).cast(DoubleType()))

aggregation = formatted_df \
    .withWatermark("timestamp", "30 seconds") \
    .groupby(window(col("timestamp"),"30 seconds","30 seconds"), col("hashtags")) \
    .agg(avg("text").alias("sentiment"), count("*").alias("count"))
    # .filter(col("window").start > datetime.datetime.now() - datetime.timedelta(seconds=29))
    

# Output to console for debugging

# query = aggregation \
#     .writeStream \
#     .format("console") \
#     .option("truncate", "false") \
#     .outputMode("append") \
#     .start()

query = aggregation \
    .select(to_json(struct(col("window"), col("hashtags"), col("sentiment"), col("count"))).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", parameters["bootstrap-servers"][0]) \
    .option("topic", parameters["topic-name"]["batches"]) \
    .option("checkpointLocation", "./checkpoint") \
    .option("failOnDataLoss", "false") \
    .start()
    
query.awaitTermination()
