#python
import requests
from datetime import datetime as dt, timezone
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

BASE_URL = "https://api.openweathermap.org/data/3.0/onecall/timemachine?"
api_key = data["key_new"]
city = "Sao Paulo"


################################### functions ###################################
def kelvin_to_celsius_fahrenheit(kelvin):
    celsius = kelvin - 273.15
    celsiusStr = (f"{celsius}:.2f")
    fahrenheit = celsius * (9/5) + 32
    fahrenheitStr = (f"{fahrenheit}:.2f")
    return celsius, fahrenheit

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
def respApiHist(time):
    #sp location lat e long
    lat = '-23.5489'
    lon = '-46.6388'
    datetime = time
    #timeX = str(dt.strptime(datetime, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    timeX = str(dt.strptime(datetime, "%Y-%m-%d").timestamp())
    timeX = timeX.replace('.0','')
    url =  f"{BASE_URL}lat={lat}&lon={lon}&dt={timeX}&appid={api_key}"
    #getting response
    response = requests.get(url).json()
    return response