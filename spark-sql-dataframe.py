from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


spark = SparkSession.builder.appName("SPSQL").getOrCreate()

schema = StructType([ \
                     StructField("CustID", IntegerType(), True), \
                     StructField("ItemID", IntegerType(), True), \
                     StructField("Price", FloatType(), True)])

people = spark.read.schema(schema).csv("file:///sparkcourse/fakefriends-header.csv")

people.printSchema()

peopleneed = people.select("CustID", "Price")

# Aggregate to find minimum temperature for every station
result = peopleneed.groupBy("CustID").agg(func.round(func.sum("Price"),2).alias("total_spent"))

resultsorted = result.sort(func.col("total_spent").desc()).show(result.count())


spark.stop()

