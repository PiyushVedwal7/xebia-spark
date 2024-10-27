from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("SimpleApp").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Create an RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Perform an action
result = rdd.reduce(lambda x, y: x + y)
print("Sum of elements:", result)

sc.stop()
