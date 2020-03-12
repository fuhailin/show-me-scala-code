import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName('my_first_app_name') \
    .getOrCreate()

stringrdd = spark.sparkContext.parallelize([
    (123, "Katie", 19, "brown"),
    (234, "Michael", 22, "green"),
    (345, "Simone", 23, "blue")
])
# 指定模式, StructField(name,dataType,nullable)
# 其中：
#   name: 该字段的名字，
#   dataType：该字段的数据类型，
#   nullable: 指示该字段的值是否为空
import pyspark.sql.types as typ
labels =  [('id',typ.LongType()),
          ('name',typ.StringType()),
          ('age',typ.LongType()),
          ('eyecolor',typ.StringType())]
schema = typ.StructType([typ.StructField(i[0],i[1],False)for i in labels])
# 对RDD应用该模式并且创建DataFrame
data = spark.createDataFrame(stringrdd,schema=schema)
# 利用DataFrame创建一个临时视图
data.registerTempTable("swimmers")
data.show()