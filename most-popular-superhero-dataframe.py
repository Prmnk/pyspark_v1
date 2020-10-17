from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///sparkcourse/Marvel+names")

lines = spark.read.text("file:///sparkcourse/Marvel+graph")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
minconnect = connections.agg(func.min("connections")).first()[0]

minconnections = connections.filter(func.col("connections") == minconnect)

minconn_names = minconnections.join(names, "id")

minconn_names.select("name").show(minconn_names.count())

