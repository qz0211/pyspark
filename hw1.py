from pyspark import SparkConf, SparkContext

conf = SparkConf().set("spark.hadoop.validateOutputSpecs", "false").setMaster("local[*]").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

rawdata = sc.textFile("customer-orders.csv")
rawdata1 = rawdata.map(lambda line: (line.split(",")[0],float(line.split(",")[1])))\
    .reduceByKey(lambda x,y:x+y).map(lambda x_y: (x_y[1],x_y[0])).sortByKey(ascending = False).map(lambda x_y: (x_y[1],x_y[0]))

result = rawdata1.collect()

for item in result:
    print("customer "+item[0]+" spends "+str(item[1]))