from pyspark import SparkConf, SparkContext


conf = SparkConf().set("spark.hadoop.validateOutputSpecs", "false").setMaster("local[*]").setAppName("BayBike")
sc = SparkContext(conf = conf)

