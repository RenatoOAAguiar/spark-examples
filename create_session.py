from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Example_Create_Session") \
    .getOrCreate()

df = spark.read.parquet("path")