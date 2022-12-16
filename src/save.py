from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType
import json
import pyspark
from pyspark.sql.functions import from_json, col
from datetime import datetime






scala_version = '2.12'
spark_version = '3.2.1'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]
spark = SparkSession.builder\
   .master("local")\
   .appName("kafka-example")\
   .config("spark.jars.packages", ",".join(packages))\
   .getOrCreate()
schema = StructType([
  StructField('date', StringType(), True),
  StructField('ID', StringType(), True),
  StructField('country', StringType(), True),
    StructField('name', StringType(), True),
    StructField('number', StringType(), True),
    StructField('CVC', StringType(), True),
    StructField('expire', StringType(), True),
    StructField('IsLegit', StringType(), True),
    StructField('reason', StringType(), True)
  ])
"""
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "t4") \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .load() \
  .select(from_json(col("value").cast("string"), schema).alias("json"))\
  .select("json.*")

df.writeStream \
  .trigger(processingTime='5 seconds')\
  .option("checkpointLocation", "../checkpoint/") \
  .option("path", "../" + datetime.now().strftime('%Y-%m-%d'))\
  .option("header","true") \
  .outputMode("append")\
  .format("csv")\
  .start().awaitTermination()

"""

import os

folder_name = "../" + datetime.now().strftime('%Y-%m-%d')

if not os.path.exists(folder_name):
        os.makedirs(folder_name)


with open("../" + datetime.now().strftime('%Y-%m-%d')+"/log.csv", "a+") as f:
  f.write("date,ID,country,name,number,CVC,expire,IsLegit,reason\n")
  consumer = KafkaConsumer('server')
  for msg in consumer:
    value = json.loads(msg.value)
    #if value["res"] == "False":
    f.write(str(value["date"]) + "," + str(value["ID"]) + "," + str(value["country"]) + "," + str(value["name"]) + "," + str(value["number"]) + "," + str(value["CVC"]) + "," + str(value["expire"]) + "," + str(value["IsLegit"]) + "," + str(value["reason"]) + "\n")
