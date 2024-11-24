from pyspark.sql.types import StringType, StructType, StructField,IntegerType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from util.config import Config
from util.logger import Log4j
from postgres.operations import PostgresOperate
import psycopg2

def create_dim_date(spark: SparkSession):    
    # Tạo DataFrame có dãy ngày từ 01-10-2024 đến 10-10-2024
    df_dates = spark.range(1).select(expr("sequence(to_timestamp('2024-01-01 00:00:00'), to_timestamp('2025-01-01 00:00:00'), interval 1 hour) as timestamp_seq"))
    
    # Explode chuỗi ngày để có mỗi ngày một dòng
    df_dates_generate = df_dates.selectExpr("explode(timestamp_seq) as timestamp")
    # Tao cac cot lien quan
    df_dates_generate_column = df_dates_generate \
    .withColumn('datetime_key',date_format("timestamp", "yyyyMMddHH").cast('int')) \
    .withColumn('full_date',col('timestamp').cast('date')) \
    .withColumn("day_of_week", date_format("full_date", "EEEE")) \
    .withColumn('day_of_week_short',dayofweek("full_date")) \
    .withColumn('day_of_month',dayofmonth("full_date")) \
    .withColumn('month',month("full_date")) \
    .withColumn('year',year("full_date")) \
    .withColumn('hour',hour("timestamp")) \
    .drop("timestamp")
    
    return df_dates_generate_column



if __name__ == "__main__":
    
    conf = Config()
    spark_conf = conf.spark_conf
    
    postgres_conf =conf.postgres_conf
    db_ops = PostgresOperate(postgres_conf)


    spark = SparkSession.builder \
    .getOrCreate()

    log = Log4j(spark)

    #Create dimension table
    try:
        df_dim_date = create_dim_date(spark)
        db_ops.save_to_postgres(df_dim_date,"dim_date",mode="append")
    except Exception as e:
        raise
    finally:
        print("Stop Load Date Dimension")
        spark.stop()

    

