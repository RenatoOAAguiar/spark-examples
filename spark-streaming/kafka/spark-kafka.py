import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import *

# Configure the logger
logging.getLogger('py4j').setLevel(logging.ERROR)
logging.getLogger('py4j.java_gateway').setLevel(logging.ERROR)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('EventHub_Consumer')


def main():

  spark = SparkSession \
      .builder \
      .appName("Kafka_Consumer") \
      .config("spark.streaming.stopGracefullyOnShutdown", "true") \
      .getOrCreate()

  # Show only error logs
  spark.sparkContext.setLogLevel("ERROR")


  schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("emails", ArrayType(StringType()))
  ])

  df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "topic") \
        .option("startingOffsets", "earliest") \
        .load()
  
  df_csv = df. \
      selectExpr("cast(body as string) as csv"). \
      select(from_json("csv", schema).alias("data")). \
      select("data.*")


  result_df = df_csv.writeStream. \
      outputMode("append"). \
      format("parquet"). \
      option("path", "result/"). \
      option("checkpointLocation", "checkpoint/consumer/" + "result_checkpoint"). \
      partitionBy("age"). \
      trigger(processingTime="30 seconds"). \
      start()

  try:
    result_df.awaitTermination()
  except Exception as e:
    logger.error(str(e))

if __name__ == '__main__':
    main()
