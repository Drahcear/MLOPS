from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType
import json
import pyspark
from pyspark.sql.functions import from_json, col
from datetime import datetime, date, timedelta
from pyspark.sql.functions import *

def get_dataframe():
    spark = SparkSession.builder.master("local").appName("Analytics").getOrCreate()
    df = spark.read.option("header","true").csv("2022*/*.csv")
    df = df.withColumn('date', to_timestamp(df.date, 'yyyy/MM/dd HH:mm:ss'))
    df = df.withColumn("day", to_date(df.date))
    df = df.withColumn("time", date_format(df.date, "HH:mm:ss"))
    return df

def from_date(df, date):
    # date format : YYYY-MM-DD
    return df.filter(df.day == date)

# Get all transaction done after time
def from_time(df, time):
    # time format : HH:mm:ss
    return df.filter(df.time >= time)

def from_date_time(df, date, time):
    return df.filter((df.day == date) & (df.time >= time))

def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return (d2 - d1).days

def from_range(df, start, end):
    total_count = df.filter((df.day >= start) & (df.day <= end)).count()
    list_day = []
    true_count = []
    false_count = []
    nb_day = days_between(start, end)
    for i in range(nb_day + 1):
        date = datetime.strptime(start, '%Y-%m-%d')
        result = date + timedelta(days=i)
        list_day.append(result)
        true_count.append(from_date(df, result).filter(col("IsLegit") == True).count())
        false_count.append(from_date(df, result).filter(col("IsLegit") == False).count())
    return total_count, list_day, true_count, false_count