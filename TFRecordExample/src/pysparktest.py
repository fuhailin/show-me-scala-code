#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName('my_first_app_name') \
    .getOrCreate()

path = "test-output.tfrecord"

fields = [StructField("id", IntegerType()), StructField("IntegerCol", IntegerType()),
          StructField("LongCol", LongType()), StructField("FloatCol", FloatType()),
          StructField("DoubleCol", DoubleType()), StructField("VectorCol", ArrayType(DoubleType(), True)),
          StructField("StringCol", StringType())]
schema = StructType(fields)
test_rows = [[11, 1, 23, 10.0, 14.0, [1.0, 2.0], "r1"], [21, 2, 24, 12.0, 15.0, [2.0, 2.0], "r2"]]
rdd = spark.sparkContext.parallelize(test_rows)
df = spark.createDataFrame(rdd, schema)
df.write.format("tfrecords").option("recordType", "Example").save(path)

df = spark.read.format("tfrecords").option("recordType", "Example").load(path)
df.show()