from pyspark.sql.types import StringType, StructType, StructField,IntegerType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from util.config import Config
from util.logger import Log4j
from postgres.operations import PostgresOperate

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

def create_dim_location(spark: SparkSession, location_path: str):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("iso", StringType(), True),
        StructField("name", StringType(), True),
        StructField("nicename", StringType(), True),
        StructField("iso3", StringType(), True),
        StructField("numcode", IntegerType(), True),
        StructField("phonecode", IntegerType(), True)
    ])
    df_location = spark.read \
    .format('csv') \
    .option('header',True) \
    .schema(schema) \
    .option('path',location_path) \
    .load()

    undefined_row = spark.sql("""
    SELECT
            0 AS id
            , 'Undefined' AS iso
            , 'Undefined' AS name
            , 'Undefined' AS nicename
            , 'Undefined' AS iso3
            , -1 AS numcode
            , -1 AS phonecode
    """)
    df_location_final = df_location \
    .union(undefined_row) \
    .orderBy('id') \
    .withColumn('location_key',abs(hash('iso'))) \
    .selectExpr("location_key",
               "iso AS country_iso2",
               "iso3 AS country_iso3",
               "name AS country_name",
               "nicename AS country_nicename",
               "numcode AS country_numcode",
               "phonecode AS country_phonecode")
    
    return df_location_final

def create_dim_product(spark:SparkSession, product_path:str):   
    schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
    
    df_product = spark.read \
    .format('csv') \
    .option('header',True) \
    .schema(schema) \
    .option('path',product_path) \
    .load()

    df_product_final = df_product.selectExpr("id AS product_key","name AS product_name")

    return df_product_final


if __name__ == "__main__":
    
    conf = Config()
    spark_conf = conf.spark_conf
    kaka_conf = conf.kafka_conf
    
    
    postgres_conf =conf.postgres_conf
    db_ops = PostgresOperate(postgres_conf)


    KAFKA_PATH_CHECKPOINT = "/data/data_behavior/kafka_checkpoint/"

    spark = SparkSession.builder \
    .config(conf=spark_conf) \
    .config("spark.jars.packages","org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

    log = Log4j(spark)

    #Create dimension table
    df_dim_date = create_dim_date(spark)
    df_dim_location = create_dim_location(spark,"/data/country.csv")
    df_dim_porduct = create_dim_product(spark,"/data/dim_product.csv")

    db_ops.save_to_postgres(df_dim_date,"dim_date")
    db_ops.save_to_postgres(df_dim_location,"dim_location")
    db_ops.save_to_postgres(df_dim_porduct,"dim_product")


    print("Stop Create Dimension")
    spark.stop()

