#pyspark
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import explode,col,regexp_replace
from functions.apiResponse import respApi, explodeFunc, temp_celsius, temp_kelvin, temp_fahrenheit, today
from functions.apiResponseHist import respApiHist, kelvin_to_celsius_fahrenheit
#python
import requests
from datetime import datetime as dt
import json
# from pyspark_test import assert_pyspark_df_equal


# Python Func
def kelvin_to_celsius(kelvin):
    celsius = kelvin - 273.15
    celsiusStr = "{:.2f}".format(celsius)
    return celsiusStr

# udf to transfor kelvin to celsius
kelvin_to_celsius_udf = udf(lambda x: kelvin_to_celsius(x), StringType())

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
df = explodeFunc(df)

df = df.withColumn("main_feels_like_celsius", kelvin_to_celsius_udf(col("main_feels_like"))).\
        withColumn("temp_min_celsius", kelvin_to_celsius_udf(col("main_temp_min"))).\
        withColumn("temp_max_celsius", kelvin_to_celsius_udf(col("main_temp_max"))).\
        withColumn("sys_sunrise", from_unixtime(col("sys_sunrise"),"yyyy-MM-dd HH:mm:ss")).\
        withColumn("sys_sunset", from_unixtime(col("sys_sunset"),"yyyy-MM-dd HH:mm:ss")).\
        withColumn("new_id", F.expr("uuid()")).\
        withColumnRenamed("name", "city_name")
df = df.select([col(c).cast("string") for c in df.columns])

cols = ["new_id","city_name","temp_celsius","prediction_date","coord_lat","coord_lon","sys_sunrise","sys_sunset","weather_description","weather_main","wind_deg","wind_speed","main_feels_like_celsius","temp_min_celsius","temp_max_celsius"]
df = df.select(*cols)

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
dfHist = explodeFunc(dfHist)

#temp_celsius, temp_fahrenheit, today
dfHist = dfHist.withColumn("temp_celsius", lit("{:.2f}".format(temp_celsius_hist))).\
        withColumn("temp_fahrenheit", lit("{:.2f}".format(temp_fahrenheit_hist))).\
        withColumn("prediction_date", lit(hist_day))

dfHist = dfHist.withColumn("temp_celsius", lit("{:.2f}".format(temp_celsius))).\
        withColumn("data_feels_like", kelvin_to_celsius_udf(col("data_feels_like"))).\
        withColumn("temp_min_celsius", lit(None)).\
        withColumn("temp_max_celsius", lit(None)).\
        withColumn("sys_sunrise", from_unixtime(col("data_sunrise"),"yyyy-MM-dd HH:mm:ss")).\
        withColumn("sys_sunset", from_unixtime(col("data_sunset"),"yyyy-MM-dd HH:mm:ss")).\
        withColumn("new_id", F.expr("uuid()")).\
        withColumn("city_name", lit("São Paulo")).\
        withColumn("prediction_date", lit(today))
dfHist = dfHist.select([col(c).cast("string") for c in dfHist.columns])

cols = ["new_id","city_name","temp_celsius","prediction_date","lat","lon","sys_sunrise","sys_sunset","data_weather_description","data_weather_main","data_wind_deg","data_wind_speed","data_feels_like","temp_min_celsius","temp_max_celsius"]
ddfHistf = dfHist.select(*cols)


print("DF Hist 1 depois do explode Func:\n")
dfHist.show(truncate=False)
dfHist.printSchema()