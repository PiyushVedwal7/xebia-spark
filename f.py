from pyspark import SparkContext

sc = SparkContext("local", "test")
rdd = sc.parallelize([1, 2, 3, 4, 5])
result = rdd.map(lambda x: x + 10).collect()
print(result)
