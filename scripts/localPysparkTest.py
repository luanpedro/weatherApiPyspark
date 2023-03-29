# paths
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.context import *
from pyspark.sql.functions import explode,col,regexp_replace
from pyspark.sql import DataFrameWriter
import pandas
# python
from datetime import datetime, timedelta
import re

spark = (SparkSession
         .builder
         .getOrCreate()
         )


# reading all jsons locally
format_input = "/Users/luan/Documents/testingPysparkLocal/files/*.parquet"

df = spark.read.parquet(format_input)

x = df.count()
print(x)
df.printSchema()