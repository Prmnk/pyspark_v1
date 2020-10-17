from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("countcust")

sc = SparkContext(conf = conf)

def parse(line):
    f1 = int(line.split(',')[0])
    f2 = float(line.split(',')[1])
    return f1, f2

data = sc.textFile("file:///sparkcourse/customer-orders.csv")
cnt = data.map(parse)
cntbycust = cnt.reduceByKey(lambda x,y: x+y)
cntsort = cntbycust.map(lambda x : (x[1],x[0])).sortByKey(ascending = False)
results = cntsort.collect()

for x in results:
    print(x[1], x[0])