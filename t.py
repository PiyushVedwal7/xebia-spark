from pyspark import SparkConf, SparkContext
import time

conf = SparkConf().setAppName("YourAppName") \
    .set("spark.executor.memory", "4g") \
    .set("spark.driver.memory", "4g") \
    .set("spark.network.timeout", "600s") \
    .set("spark.executor.heartbeatInterval", "60s")

sc = SparkContext(conf=conf)



data = [1, 2, 3]
rdd = sc.parallelize(data)
result = rdd.collect()
print(result)



rdd2 = rdd.map(lambda x: x * 2)
result1 = rdd2.collect()
print(result1)


time.sleep(300)