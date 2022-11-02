import findspark

findspark.init()


import pyspark
import pytest
import logging
from chispa.column_comparer import assert_column_equality
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType
from pyspark.sql.functions import col
from pyspark.sql.functions import array_contains

# spark = SparkSession.builder.getOrCreate()

spark = (SparkSession.builder
         .master("local")
         .appName("BTC-pyspark-analysis")
         .getOrCreate())

# logging.basicConfig(filename="run.log", level=logging.info(), format='%(asctime)s:%(levelname)s:%(message)s')

# The CSV dataset is pointed to by the path1 and path2 variables.
path1 = "dataset_one.csv"
path2 = "dataset_two.csv"
df1 = spark.read.option("delimiter", ",").option("header", True).csv(path1)
df2 = spark.read.option("delimiter", ",").option("header", True).csv(path2)
# df1 = spark.read.csv(path1, header="True")


# Excluding the email column
df1 = df1.drop("email")

# Creating a list of the countries I am going to filter on
list_countries_filter = ["United Kingdom", "Netherlands"]

# Filtering the first dataset on UK and NL
df1 = df1.filter(df1.country.isin(list_countries_filter))

# Removing column which contains the credit card numbers
df2 = df2.drop("cc_n")

# Renaming columns
df2 = df2.select(col("id").alias("client_identifier"), col("btc_a").alias("btc_address"),
                 col("cc_t").alias("credit_card_type"))

# Joining the two datasets on ID as a new data frame
joinDF = df1.join(df2, df1.id == df2.client_identifier, 'inner').select(df1.id, df1.first_name, df1.last_name,
                                                                        df1.country, df2.client_identifier,
                                                                        df2.btc_address)
joinDF.show()

# Exporting the newly created data frame as a csv file
joinDF.write.option("header", True).csv('client_data')
