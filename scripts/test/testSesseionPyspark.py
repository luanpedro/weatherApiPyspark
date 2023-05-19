from pyspark.sql import SparkSession
from pyspark.context import SparkContext, SparkConf
import pyspark.sql.functions as F
import json

path = '/Users/luan/Documents/testingPysparkLocal/files/exemploWeatherApiResponse.json'

with open(path) as file:
    response = json.load(file)

sc = SparkContext()
spark = (SparkSession
         .builder
         .getOrCreate()
         )

#response = respApi()
rdd = sc.parallelize([response])

df = spark.createDataFrame(rdd)

dflulu = df.filter(condition)

df.show()