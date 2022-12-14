from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType
import json
import pyspark
from pyspark.sql.functions import from_json, col
from datetime import datetime, date, timedelta
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import plotly.express as px


def get_dataframe():
    spark = SparkSession.builder.master("local").appName("Analytics").getOrCreate()
    df = spark.read.option("header","true").csv("../2022*/*.csv")
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

def from_range(df, date):    
    list_day = []    
    true_count = from_date(df, date).filter(col("IsLegit") == True).count()
    false_count = from_date(df, date).filter(col("IsLegit") == False).count()
    return df.count(), true_count, false_count

def plot_per_coutry(df, date):
    if date != "":
        df = from_date(df, date)
    df.show()
    df_tmp = df.groupBy("country").count()
    df_country = df_tmp.select("country").collect()
    df_count = df_tmp.select("count").collect()
    df_tmp2 = df_tmp.withColumn("json", f.to_json(f.struct("country", "count")))
    country_dict = df_tmp2.select("json").rdd.map(lambda x: json.loads(x[0])).collect()
    if len(country_dict) == 0 :
        fig = px.scatter_geo()
    else:
        fig = px.scatter_geo(country_dict, locations='country', color='country',
                     size='count', title='Countries by Number')
    return fig


def get_most_used_false_card(df):    
    
    name = df.filter(df.IsLegit == "False").groupBy("name").count().sort(desc("count")).limit(1).collect()
    if len(name) == 0:
        return "", "", "", ""    
    name = name[0][0]
    res = df[df.name == name].limit(1).collect()[0]
    
    return res["name"], res["number"], res["CVC"], res["expire"]