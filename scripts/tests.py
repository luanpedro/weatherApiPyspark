import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import *
from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .getOrCreate()
         )

data = ["123456789", "222222222EUR"]
transactionsDf = spark.createDataFrame(data, "string").toDF("transaction_information_cleaned")

transactionsDf.show()