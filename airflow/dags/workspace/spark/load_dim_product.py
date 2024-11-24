from pyspark.sql.types import StringType, StructType, StructField,IntegerType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from util.config import Config
from util.logger import Log4j
from postgres.operations import PostgresOperate



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
    
    postgres_conf =conf.postgres_conf
    db_ops = PostgresOperate(postgres_conf)


    spark = SparkSession.builder \
    .getOrCreate()

    log = Log4j(spark)

    #Create dimension table
    try:
        df_dim_porduct = create_dim_product(spark,"/data/dim_product.csv")
        db_ops.save_to_postgres(df_dim_porduct,"dim_product",mode="append")
    except Exception as e:
        raise
    finally:
        print("Stop Load Product Dimension")
        spark.stop()

    

