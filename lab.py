from pyspark.sql import SparkSession
import time
import os
import shutil


path = r"C:\Users\ACER\OneDrive\Desktop\spark\text\data.csv"

spark = SparkSession.builder.appName("lab2").getOrCreate()

table_path = r"C:\Users\ACER\OneDrive\Desktop\spark_lab\spark-warehouse\my_table"


if os.path.exists(table_path):
    shutil.rmtree(table_path)


spark.sql("DROP TABLE IF EXISTS my_table")

df = spark.read.csv(path, header=True, inferSchema=True)

df.write.mode("overwrite").saveAsTable("my_table")


spark.sql("SELECT * FROM my_table WHERE country = 'United Kingdom' AND Description IS NOT NULL").show(10)

time.sleep(180)
