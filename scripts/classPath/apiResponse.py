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


#getting credencias to use api
path = "apiKey.json"
with open(path) as file:
    data = json.load(file)

BASE_URL = "http://api.openweathermap.org/data/2.5/weather?"
api_key = data["key"]
city = "Sao Paulo"


################################### functions ###################################
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

################################### api mount and retrivieng ###################################
def respApi():
    #mounting url
    url = BASE_URL + "appid=" + api_key + "&q=" + city
    #getting response
    response = requests.get(url).json()
    return response

response = respApi()
temp_kelvin = response['main']['temp']
feels_like_kelvin = response['main']['feels_like']
temp_celsius, temp_fahrenheit = kelvin_to_celsius_fahrenheit(temp_kelvin)
feels_like_celsius, feels_like_fahrenheit = kelvin_to_celsius_fahrenheit(feels_like_kelvin)
today = response['dt']
today = dt.fromtimestamp(today).strftime('%Y-%m-%d')
kelvin_to_celsius_udf = udf(lambda x: kelvin_to_celsius(x), StringType())