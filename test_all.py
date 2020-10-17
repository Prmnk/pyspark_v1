from __future__ import print_function
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.linalg import vectors

if __name__ = "__main__":
    spark = SparkSession.builder.appName("LR").getOrCreate()

    inputLines = spark.sparkContext.textFile("regression.txt")
    data = inputLines.map(lambda x: x.split(",")).map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))

    colnames = ['label','feature']
    df = data.toDf(colnames)

    trainTest = df.randomSplit([0.6,0.4])
    trainingDf = trainTest[0]
    trstDf = trainTest[1]
    
    lr = LinearRegression(maxIter = 10, regParam = 0.3, elasticNetParam = 0.8)

    model = lr.fit(trainingDf)

    fullpred = model.transform(testDf).cache()

    predictions = fullpred.select("prediction").rdd.map(lambda x: x[0])

      predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("label").rdd.map(lambda x: x[0])

    # Zip them together
    predictionAndLabel = predictions.zip(labels).collect()

    # Print out the predicted and actual values for each point
    for prediction in predictionAndLabel:
      print(prediction)


    # Stop the session
    spark.stop()