import findspark
findspark.init()

from chispa.dataframe_comparer import *


import pyspark
import pytest
from chispa.column_comparer import assert_column_equality
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType
from pyspark.sql.functions import col
from pyspark.sql.functions import array_contains


spark = (SparkSession.builder
  .master("local")
  .appName("BTC-pyspark-analysis-solution")
  .getOrCreate())


# The CSV dataset is pointed to by the path1 and path2 variables.
path1 = "dataset_one.csv"
path2 = "dataset_two.csv"
df1 = spark.read.option("inferSchema",True).option("delimiter", ",").option("header", True).csv(path1)
df2 = spark.read.option("inferSchema",True).option("delimiter", ",").option("header", True).csv(path2)
# df3 = spark.read.format("csv").load(path1)
# df4 = spark.read.csv(path1)

df1.printSchema()
sc=spark.sparkContext
rdd=sc.textFile(path1)
rdd.collect()
df2.printSchema()
# df3.printSchema()
# df4.printSchema()

from chispa.column_comparer import assert_column_equality
import pyspark.sql.functions as F

# def remove_non_word_characters(col):
#     return F.regexp_replace(col, "[^\\w\\s]+", "")
#
#
# def test_remove_non_word_characters_nice_error():
#     data = df1
#     df = (spark.createDataFrame(df1.collect(), df1.schema)
#         .withColumn("clean_name", remove_non_word_characters(F.col("email"))))
#     assert_column_equality(df, "clean_name", "expected_name")
#     df
#
# test_remove_non_word_characters_nice_error()