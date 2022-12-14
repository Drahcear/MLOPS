from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType
import json
import pyspark
from pyspark.sql.functions import from_json, col
from datetime import datetime
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import json
import plotly.express as px
from utils import *


spark = SparkSession.builder.master("local").appName("Analytics").getOrCreate()
df = spark.read.option("header","true").csv("../2022*/*.csv")

df = df.withColumn('date', to_timestamp(df.date, 'yyyy/MM/dd HH:mm:ss'))
df = df.withColumn("day", to_date(df.date))
df = df.withColumn("time", date_format(df.date, "HH:mm:ss"))


list_day = df.select('day').distinct().rdd.map(lambda x: x[0]).collect()
l = {}

total_count = 0
for i, day  in enumerate(list_day):
    sub_dict = {}
    # Total count, false true count
    sub_df = df.where(df.day==day)
    count, true_count, false_count = from_range(sub_df, day)
    sub_dict["count"] = count
    sub_dict["false"] = false_count
    sub_dict["true"] = true_count
    total_count += count
    l[str(day)] = sub_dict
    name, number, CVC, expire = get_most_used_false_card(sub_df)
    sub_dict["name"] = name
    sub_dict["number"] = number
    sub_dict["CVC"] = CVC
    sub_dict["expire"] = expire
    
    
    
    
with open("../Analytics.txt", "w") as outfile:
    
    json.dump(l, outfile)