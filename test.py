from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from cassandra.cluster import Cluster
import os

# Create a Spark Session
spark = SparkSession.builder.appName("StreamingToCassandra").getOrCreate()
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'


# Define the schema for the incoming data
schemaStock = StructType(
  [
    StructField("symbol", StringType()),
    StructField("time", TimestampType()),
    StructField("open", FloatType()),
    StructField("high", FloatType()),
    StructField("low", FloatType()),
    StructField("close", FloatType()),
    StructField("volume", StringType()),
    StructField("previous_close", StringType()),
    StructField("ref", StringType()),
    StructField("ceil", StringType()),
    StructField("floor", StringType()),
          
  ])
# Read data from the Kafka topic
streaming_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "topic_name2") \
  .load()

decoded_df = streaming_df.select(
    from_json(col("value").cast("string"), schema).alias("parsed_data")
)

extracted_df = decoded_df.select("parsed_data.*")

def write_to_cassandra(row):
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    # Insert the row into the Cassandra table
    session.execute(
        "INSERT INTO stocks.tick (time, symbol, open, high, low, close, volume, ref, ceil, floor) "
        "VALUES (%s, %s, %s)",
        (row.date, row.symbol, row.open, row.high, row.low, row.close, row.volume, row.ref, row.ceil, row.floor)
    )

query = extracted_df \
  .writeStream \
  .foreach(write_to_cassandra) \
  .start()

# Wait for the query to finish
query.awaitTermination()