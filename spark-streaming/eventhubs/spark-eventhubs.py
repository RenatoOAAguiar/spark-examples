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

def parse_args():
  parser = argparse.ArgumentParser(description='exports the view assertividade')
  parser.add_argument('--eventHubEndpoint', action='store', help='event hub endpoint')
  parser.add_argument('--eventHubKeyName', action='store', help='event hub key name')
  parser.add_argument('--eventHubKey', action='store', help='event hub key')
  parser.add_argument('--eventHubName', action='store', help='event hub name to consume')

  return parser.parse_args()


def main():
  args = parse_args()

  spark = SparkSession \
      .builder \
      .appName("EventHub_Consumer") \
      .config("spark.streaming.stopGracefullyOnShutdown", "true") \
      .getOrCreate()

  # Show only error logs
  spark.sparkContext.setLogLevel("ERROR")
  sc = spark.sparkContext

  eventHubName = args.eventHubName

  connectionString = "Endpoint="+ args.eventHubEndpoint + \
                     ";SharedAccessKeyName=" + args.eventHubKeyName + \
                     ";SharedAccessKey=" + args.eventHubKey + \
                     ";EntityPath=" + eventHubName

  ehConf = {
    'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString),
    'maxEventsPerTrigger': 15000,
    'eventhubs.consumerGroup' : "$Default"
  }

  schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("emails", ArrayType(StringType()))
  ])

  df = spark.readStream.format("eventhubs"). \
        option("startingOffsets", "earliest"). \
        options(**ehConf). \
        load()
  
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
