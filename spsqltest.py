from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("test").getOrCreate()

people = spark.read.option("header", "true").option('inferSchema',"true").csv("file:///sparkcourse/fakefriends-header.csv")

people.printSchema()

peoplefil = people.select("age","friends")

people.groupby("age").avg().select("age","avg(friends)").sort("age").show()

people.groupby("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show()

spark.stop()
