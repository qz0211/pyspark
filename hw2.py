from pyspark import SparkConf, SparkContext


conf = SparkConf().set("spark.hadoop.validateOutputSpecs", "false").setMaster("local[*]").setAppName("jpinTest")
sc = SparkContext(conf = conf)

def getData(line):
    temp= line.split()
    return (temp[1])

data = sc.textFile("u.data").map(lambda x:(getData(x),1))

name = sc.textFile("u.item").map(lambda line:(line.split("|")[0],line.split("|")[1]))

fullData = data.join(name).values().map(lambda x:(x[1],x[0]))\
    .reduceByKey(lambda x,y:x+y)\
    .map(lambda x_y:(x_y[1],x_y[0]))\
    .sortByKey(False).take(10)



for item in fullData:
    print(item)

input("type sth")




