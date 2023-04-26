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
from classPath.apiResponse import respApi, explodeFunc ,temp_celsius, temp_kelvin, temp_fahrenheit, today
from classPath.apiResponseHist import respApiHist, kelvin_to_celsius_fahrenheit
from classPath.helloWorld import myfunc

#===================================================  PySpark ===================================================
sc = SparkContext()
spark = (SparkSession
         .builder
         .getOrCreate()
         )

response = respApi()

print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Checando dados da API today >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
print(f"Data da previsão: {today}\nTemperatura em kelvin: {temp_kelvin}\nTemperatura em Celsius: {temp_celsius}\nTemperatura em farenheit: {temp_fahrenheit}\n")

rdd = sc.parallelize([response])
df = spark.read.json(rdd)


#temp_celsius, temp_fahrenheit, today
df = df.withColumn("temp_celsius", lit("{:.2f}".format(temp_celsius))).\
        withColumn("temp_fahrenheit", lit("{:.2f}".format(temp_fahrenheit))).\
        withColumn("prediction_date", lit(today))

print("DF antes do explode Func:\n")
df.show(truncate=False)
df.printSchema()

df = explodeFunc(df)

print("DF 1 depois do explode Func:\n")
df.show(truncate=False)
df.printSchema()

#=================================================== Hist Prediction ===================================================

desiredDate = input("Inserir a data de previsão desejada no formato AAAA-MM-DD: ") #'2022-09-07'
#getting api response
respHist = respApiHist(desiredDate)

temp_kelvin_hist = respHist['data'][0]['temp']
feels_like_kelvin_hist = respHist['data'][0]['feels_like']
temp_celsius_hist, temp_fahrenheit_hist = kelvin_to_celsius_fahrenheit(temp_kelvin_hist)
feels_like_celsius_hist, feels_like_fahrenheit_hist = kelvin_to_celsius_fahrenheit(feels_like_kelvin_hist)
hist_day = respHist['data'][0]['dt']
hist_day = dt.fromtimestamp(hist_day).strftime('%Y-%m-%d')

print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Checando dados da API Hist >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
print(f"Data da previsão: {hist_day}\nTemperatura em kelvin: {temp_kelvin_hist}\nTemperatura em Celsius: {temp_celsius_hist}\nTemperatura em farenheit: {temp_fahrenheit_hist}\n")

rddHist = sc.parallelize([respHist])
dfHist = spark.read.json(rddHist)

print("DF Hist antes do explode Func:\n")
dfHist.show(truncate=False)
dfHist.printSchema()

dfHist = explodeFunc(dfHist)

#temp_celsius, temp_fahrenheit, today
dfHist = dfHist.withColumn("temp_celsius", lit("{:.2f}".format(temp_celsius_hist))).\
        withColumn("temp_fahrenheit", lit("{:.2f}".format(temp_fahrenheit_hist))).\
        withColumn("prediction_date", lit(hist_day))

print("DF Hist 1 depois do explode Func:\n")
dfHist.show(truncate=False)
dfHist.printSchema()