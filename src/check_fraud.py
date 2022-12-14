from kafka import KafkaConsumer
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType
import json
import pyspark
from pyspark.sql.functions import from_json, col
from datetime import datetime
import time


def login_check(value):
    if value["country"] in banned_countries:
        return False
    return True

def check(row, key, expected):
    return row[key] == expected
    
def credit_card_check(value):
    
    reason = "Forbidden Country"
    if not login_check(value):
        return False, reason
        
    reason = "False Card"
    name = value["name"]
    number = value["number"]
    CVC = value["CVC"]
    expire = value["expire"]
    row = df.where(df.number==number).limit(1).collect()
    
    if len(row) == 0:
        return False, reason
    row = row[0]
    res = check(row, "number", number) and check(row, "CVC", CVC) and check(row, "expire", expire) and check(row, "name", name)
    if res:
        reason = "Card accepted"
    return res, reason
def send(msg, producer, topic):
    msg = json.dumps(msg)        
    producer.send(topic, msg.encode('utf-8'))
    producer.flush()


spark = SparkSession.builder.master("local").appName("FraudCheck").getOrCreate()
df = spark.read.option("header","true").csv("../DB/client.csv")
banned_countries = ['PRK', 'RUS', 'IRN', 'CUB', 'SDN', 'SYR', 'BEL']
producer = KafkaProducer(bootstrap_servers="localhost")

consumer = KafkaConsumer('t3')

for msg in consumer:
   value = json.loads(msg.value)
   res, reason = credit_card_check(value)
   time.sleep(0.3)
   part1 = {"IsLegit": str(res), "reason": reason}
   send({**value, **part1}, producer, "t4")