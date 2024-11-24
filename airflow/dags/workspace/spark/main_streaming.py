import time

from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType,IntegerType,TimestampType
from pyspark.sql.functions import *

from util.config import Config
from util.logger import Log4j
from postgres.operations import PostgresOperate

from batch_prcessing import process_batch
import os

def normalized_df(df):
    schema = StructType([
        StructField("_id", StringType(), True),
        StructField("time_stamp", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("resolution", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("api_version", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("local_time", TimestampType(), True),
        StructField("show_recommendation", StringType(), True),
        StructField("current_url", StringType(), True),
        StructField("referrer_url", StringType(), True),
        StructField("email_address", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("option", ArrayType(StructType([
            StructField("option_label", StringType(), True),
            StructField("option_id", StringType(), True),
            StructField("value_label", StringType(), True),
            StructField("value_id", StringType(), True),
        ])), True),
        StructField("id", StringType(), True)
    ])
    parsed_df = (
        df.withColumn("json_data", from_json(col("value").cast("string"), schema))
        .select("json_data.*")
        .withColumn("time_stamp", col("time_stamp").cast(LongType()))
        .withColumn("product_id", col("product_id").cast(IntegerType()))
        .withColumn("store_id", col("store_id").cast(IntegerType()))
        .withColumn("local_date",to_date(col("local_time"),"yyyy-MM-dd"))
    )
    
    return parsed_df

if __name__ == "__main__":
    
    conf = Config()
    spark_conf = conf.spark_conf
    kaka_conf = conf.kafka_conf
    
    
    postgres_conf =conf.postgres_conf
    db_ops = PostgresOperate(postgres_conf)

    # os.environ['PYSPARK_DRIVER_PYTHON'] = "python"
    # os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"
    print("check_path", os.getcwd())

    KAFKA_PATH_CHECKPOINT = "./spark/kafka_checkpoint"
    try:
        spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

        log = Log4j(spark)

        #Read Kafka Stream
        df = spark.readStream \
        .format("kafka") \
        .option("auto.offset.reset", "earliest") \
        .option("startingOffsets","earliest") \
        .option("maxOffsetsPerTrigger", "100") \
        .options(**kaka_conf) \
        .load()

        #Transform and Load to Postgres
        load = df.transform(lambda df: normalized_df(df)) \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df,batch_id,db_ops)) \
        .option("checkpointLocation", KAFKA_PATH_CHECKPOINT) \
        .start() \
        .awaitTermination()
    except Exception as e:
        raise
    finally:
        print("Stop Streaming")
        spark.stop()