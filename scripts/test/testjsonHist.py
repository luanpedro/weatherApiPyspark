#pyspark
from pyspark.sql import *
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.context import *
from pyspark.sql.functions import explode,col,regexp_replace
from pyspark.sql import DataFrameWriter
#python
import requests
from datetime import datetime as dt, timezone
import json

path = '/Users/luan/Documents/testingPysparkLocal/files/histJson.json'

with open(path) as file:
    response = json.load(file)

print(response['data'][0]['dt'])

################################## functions ###################################
def kelvin_to_celsius_fahrenheit(kelvin):
    celsius = kelvin - 273.15
    celsiusStr = (f"{celsius}:.2f")
    fahrenheit = celsius * (9/5) + 32
    fahrenheitStr = (f"{fahrenheit}:.2f")
    return celsius, fahrenheit

def kelvin_to_celsius(kelvin):
    celsius = kelvin - 273.15
    # celsiusStr = (f"{celsius}:.2f")
    celsiusStr = "{:.2f}".format(celsius)
    return celsiusStr

def colsDictTp(df):
    dictCols = {}
    for col in df.dtypes:
        colName = col[0]
        colType = col[1].split('<')[0]
        #print(f"Coluna: {colName} : Tipo: {colType}")
        dictCols[colName] = colType
    return dictCols

def explodeFunc(df):
    dict = colsDictTp(df)
    for colu, tipo in dict.items():
        if tipo == "array":
            df = df.withColumn(colu, explode(colu))
    dict2 = colsDictTp(df)
    for colu, tipo in dict2.items():
        if tipo == "struct":
            #print(True)
            #print(colu)
            colsA = df.select(f"{colu}.*").columns
            #print(colsA)
            for c in colsA:
                #print(c)
                df = df.withColumn(f"{colu}_{c}", col(f"{colu}.{c}"))
            df = df.drop(colu)
    return df

temp_kelvin = response['data'][0]['temp']
feels_like_kelvin = response['data'][0]['feels_like']
temp_celsius, temp_fahrenheit = kelvin_to_celsius_fahrenheit(temp_kelvin)
feels_like_celsius, feels_like_fahrenheit = kelvin_to_celsius_fahrenheit(feels_like_kelvin)
today = response['data'][0]['dt']
today = dt.fromtimestamp(today).strftime('%Y-%m-%d')

DATE = "2020-09-07"
#ts = str(dt.strptime(DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
ts = str(dt.strptime(DATE, "%Y-%m-%d").timestamp())
ts = ts.replace('.0','')
print(ts)


print("<<<<<<<<<<<<<<<<<<Checando dados da API Hist>>>>>>>>>>>>>>>>>>>>")
print(f"Data da previsão: {today}\nTemperatura em kelvin: {temp_kelvin}\nTemperatura em Celsius: {temp_celsius}\nTemperatura em farenheit: {temp_fahrenheit}\n")

###################################################### #pyspark ########################################################
sc = SparkContext()
spark = (SparkSession
         .builder
         .getOrCreate()
         )

# udf to transfor kelvin to celsius
kelvin_to_celsius_udf = udf(lambda x: kelvin_to_celsius(x), StringType())

#response = respApi()
rdd = sc.parallelize([response])
df = spark.read.json(rdd)
print("DF Hist antes do explode Func:\n")
df.show(truncate=False)
df.printSchema()

df = explodeFunc(df)
df = explodeFunc(df)

df = df.withColumn("temp_celsius", lit("{:.2f}".format(temp_celsius))).\
        withColumn("data_feels_like", kelvin_to_celsius_udf(col("data_feels_like"))).\
        withColumn("temp_min_celsius", lit(None)).\
        withColumn("temp_max_celsius", lit(None)).\
        withColumn("sys_sunrise", from_unixtime(col("data_sunrise"),"yyyy-MM-dd HH:mm:ss")).\
        withColumn("sys_sunset", from_unixtime(col("data_sunset"),"yyyy-MM-dd HH:mm:ss")).\
        withColumn("new_id", F.expr("uuid()")).\
        withColumn("city_name", lit("São Paulo")).\
        withColumn("prediction_date", lit(today))
df = df.select([col(c).cast("string") for c in df.columns])

cols = ["new_id","city_name","temp_celsius","prediction_date","lat","lon","sys_sunrise","sys_sunset","data_weather_description","data_weather_main","data_wind_deg","data_wind_speed","data_feels_like","temp_min_celsius","temp_max_celsius"]
df = df.select(*cols)

print("DF Hist depois do explode Func:\n")
df.show(truncate=False)
df.printSchema()