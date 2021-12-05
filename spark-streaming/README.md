### About Spark Streaming

1. Spark Streaming do note infer schema automatically, so is necessary to infer schema first
2. Basic example of schema
```
from pyspark.sql.types *

schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("emails", ArrayType(StringType()))
])
```

### Spark Streaming consume from Kakfa

#### Notes

1. Necessary add this dependency: https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10, choose the current version based on your spark version and scala version
2. In my example i have used spark 2.4.4 and scala 2.11, so the correct package for me was: `org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4`
3. Running this script `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 spark-kafka.py`


### Spark Streaming consume from eventhubs

#### Notes

1. Rows consumed by trigger is 1000 * number of partitions
2. If necessary increase number of rows by trigger, necessary passing value for **maxEventsPerTrigger** parameter, for example: "maxEventsPerTrigger: 10000"


#### Execution

1. Necessary add this dependency: https://mvnrepository.com/artifact/com.microsoft.azure/azure-eventhubs-spark, choose the current version based on your spark version and scala version
2. In my example i have used spark 2.4.4 and scala 2.11, so the correct package for me was: `com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.21`
3. Running this script `spark-submit --packages com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.21 spark-eventhubs.py`