
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

def parseline(Line):
    fields = Line.split(',')
    station = fields[0]
    entrytype = fields[2]
    temp = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return(station,entrytype,temp)


lines = sc.textFile("file:///SparkCourse/1800.csv")
prsedlines = lines.map(parseline)
maxtemp = prsedlines.filter(lambda x: "TMAX" in x[1])
stattiontemp = maxtemp.map(lambda x: (x[0],x[2]))
mxtemps = stattiontemp.reduceByKey(lambda x,y: max(x,y))
results = mxtemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))





'''from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
'''