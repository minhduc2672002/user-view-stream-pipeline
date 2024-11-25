from pyspark.sql.types import StringType, StructType, StructField,IntegerType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from util.config import Config
from util.logger import Log4j
from postgres.operations import PostgresOperate


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
        df_dim_location = create_dim_location(spark,"/data/country.csv")
        db_ops.save_to_postgres(df_dim_location,"dim_location")
    except Exception as e:
        raise
    finally:
        print("Stop Load Location Dimension")
        spark.stop()

    

