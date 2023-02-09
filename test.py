from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import from_json,col
from pipeline.util.config import config
from pipeline.util.util import computeRSI, computeEMA
from pyspark.sql.types import *
from pipeline.util.util import TIME_ZONE, SYMBOL_LIST
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
def SparkStreamd():
    
  spark = SparkSession \
  .builder \
  .appName("StructuredStock") \
  .getOrCreate()
  
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

  raw_df = spark\
      .read\
      .format("kafka")\
      .option("kafka.bootstrap.servers", config['kafka_broker'])\
      .option("subscribe",  config['topic_fake'])\
      .load().selectExpr("CAST(value AS STRING)")

  stockdf = raw_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),schemaStock).alias("data")).select("data.*")
  
  return stockdf, spark , schemaStock

def writeToCassandra(writeDF, epochId):
  writeDF.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="FAKE", keyspace=config['key_space'])\
    .mode("append") \
    .save()

def savetocsv(df, epichId):
  df.write.format("csv").save('stream')
def topNChange():
  stockdf, spark, sche = SparkStreamd()
  
  stockdf = stockdf.withColumn('perChange',(stockdf['close']-stockdf['previous_close'])/stockdf['previous_close'])

  # sche = StringType([
  #   StructField('STB',FloatType()),
  #   StructField('VIC',FloatType()),
  #   StructField('SSI',FloatType()),
  #   StructField('MSN',FloatType()),
  #   StructField('FPT',FloatType()),
  #   StructField('HAG',FloatType()),
  #   StructField('KDC',FloatType()),
  #   StructField('EIB',FloatType()),
  #   StructField('DPM',FloatType()),
  #   StructField('VNM',FloatType())
  # ])
  # ans = dict(sorted(change, key=change.get, reverse=True)[:5])
  #an = spark.createDataFrame(change)
  #ssi = stockdf.select('change%').where('symbol == "SSI"'
  #.foreachBatch(savetocsv)\
  query =  stockdf.write\
    .format("console")\
    .outputMode("append")\
    .start()
  
  query.awaitTermination()

  # query =  stockdf\
  #   .writeStream\
  #   .outputMode("update") \
  #   .foreachBatch(writeToCassandra) \
  #   .start()
  #query.awaitTermination()

topNChange()


