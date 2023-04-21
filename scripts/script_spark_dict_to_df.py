#python
import requests
from datetime import datetime as dt
import json
#pyspark
from pyspark import SparkContext,SparkConf
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.context import *
from pyspark.sql.functions import explode,col,regexp_replace
from pyspark.sql import DataFrameWriter
from classPath.apiResponse import respApi,explodeFunc ,temp_celsius, temp_fahrenheit, today
from classPath.helloWorld import myfunc

###################################################### #pyspark ########################################################
myfunc()

sc = SparkContext()
spark = (SparkSession
         .builder
         .getOrCreate()
         )

response = respApi()
rdd = sc.parallelize([response])
df = spark.read.json(rdd)


temp_celsius, temp_fahrenheit, today
df = df.withColumn("temp_celsius", lit("{:.2f}".format(temp_celsius))).\
        withColumn("temp_fahrenheit", lit("{:.2f}".format(temp_fahrenheit))).\
        withColumn("prediction_date", lit(today))

df.printSchema()

print("DF antes do explode Func:\n")
df.show(truncate=False)
df.printSchema()

df = explodeFunc(df)

print("DF 1 depois do explode Func:\n")
df.show(truncate=False)
df.printSchema()