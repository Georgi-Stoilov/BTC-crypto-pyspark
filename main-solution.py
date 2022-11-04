from typing import re

import findspark
import quinn

findspark.init()

from chispa.dataframe_comparer import *
from quinn.extensions import *

import numpy as np
import pandas as pd
import pyspark
import pytest
from chispa.column_comparer import assert_column_equality
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField
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
df1 = spark.read.option("inferSchema", True).option("delimiter", ",").option("header", True).csv(path1)
df2 = spark.read.option("inferSchema", True).option("delimiter", ",").option("header", True).csv(path2)

sc = spark.sparkContext
rdd1 = sc.parallelize(path1)
rdd2 = sc.parallelize(path2)
rdd2

quinn_df = df1.withColumn("email", quinn.remove_non_word_characters(col("email")))
quinn_df.show()