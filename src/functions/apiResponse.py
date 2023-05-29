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
BASE_URL_HIST = "https://api.openweathermap.org/data/3.0/onecall/timemachine?"
api_key_hist = data["key_new"]


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

def explodeFuncMap(df):
    dictCols = colsDictTp(df)
    for colu, tipo in dictCols.items():
        if tipo == "map":
            colDf1 = 'joinId'
            coldDf2 = colu+'_joinId'
            keys_df = df.select(F.explode(F.map_keys(F.col(colu)))).distinct()
            keys = list(map(lambda row: row[0], keys_df.collect()))
            key_cols = list(map(lambda f: F.col(colu).getItem(f).alias(str(f)), keys))
            final_cols = [F.col("joinId")] + key_cols
            dfOut = df.select(final_cols)
            dfOut = dfOut.select([col(c).alias(f'{colu}_{c}') for c in dfOut.columns])
            df = df.alias("df1").join(dfOut.alias("df2"), col(colDf1) == col(coldDf2), "inner")
            df = df.drop(colu)
        elif tipo == "array":
            colDf1 = 'joinId'
            coldDf2 = colu+'_joinId'
            newCol = "new_"+colu
            dfA = df.select("joinId",colu, explode(colu).alias(f'{newCol}'))
            keys_df = dfA.select(F.explode(F.map_keys(F.col(newCol)))).distinct()
            keys = list(map(lambda row: row[0], keys_df.collect()))
            key_cols = list(map(lambda f: F.col(newCol).getItem(f).alias(str(f)), keys))
            final_cols = [F.col("joinId")] + key_cols
            dfA = dfA.select(final_cols)
            dfA = dfA.select([col(c).alias(f'{colu}_{c}') for c in dfA.columns])
            df = df.alias("df1").join(dfA.alias("df2"), col(colDf1) == col(coldDf2), "inner")
            df = df.drop(colu)
        else:
            pass
    return df

            

################################### api mount and retrivieng ###################################
def respApi():
    #mounting url
    url = BASE_URL + "appid=" + api_key + "&q=" + city
    #getting response
    response = requests.get(url).json()
    return response

def respApiHist(time):
    #sp location lat e long
    lat = '-23.5489'
    lon = '-46.6388'
    datetime = time
    #timeX = str(dt.strptime(datetime, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    timeX = str(dt.strptime(datetime, "%Y-%m-%d").timestamp())
    timeX = timeX.replace('.0','')
    url =  f"{BASE_URL_HIST}lat={lat}&lon={lon}&dt={timeX}&appid={api_key_hist}"
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