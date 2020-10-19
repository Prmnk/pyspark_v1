from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("countcur")

sc= SparkContext(conf = conf)

def parseline(line):
    f1 = int(line.split(',')[0])
    f2 = float(line.split(',')[1])
    return f1, f2

data = sc.textFile("file:///sparkcourse/customer-orders.csv")

cnt = data.map(parseline)
cnt_bycust = cnt.reduceByKey(lambda x,y : x+y)
cntsort = cnt_bycust.map(lambda x: ( x[1], x[0] ) ).sortByKey(ascending = False)

results = cntsort.collect()

for x in results:
    print(x[1], x[0])