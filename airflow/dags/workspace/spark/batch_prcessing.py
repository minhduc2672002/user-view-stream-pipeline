from pyspark.sql.types import StringType, StructType, StructField,IntegerType
from pyspark.sql.functions import *
from user_agents import parse
import os

def extract_domain(df: DataFrame):
    extract_current_domain = split(col("current_url"),"/")[2]
    extract_reference_domain = split(col("referrer_url"),"/")[2]

    return (
        df.withColumn('current_domain',extract_current_domain)
        .withColumn('reference_domain',extract_reference_domain)
    )   

def extract_country_code(df : DataFrame):
    num_parts = size(split(col("current_domain"), r"\."))
    extract_coutnry_code = upper(split(col("current_domain"),r"\.")[num_parts -1])
    fix_country_code =  expr("""CASE
                                    WHEN country_code = 'COM' THEN 'US'
                                    WHEN country_code = 'AFRICA'  THEN 'BF'
                                    WHEN country_code = 'MEDIA' THEN 'LY'
                                    WHEN country_code = 'STORE' THEN 'CU'
                                    WHEN country_code = '' THEN 'Undefined'
                                    ELSE country_code
                                END AS country_code
                            """)

    return (
        df.withColumn('country_code',extract_coutnry_code)
        .withColumn('country_code',fix_country_code)
    )

def handle_refernce(df : DataFrame):
    handle_refernce_null = expr("IFNULL(reference_domain,'Undefined') AS reference_domain")
    is_self_reference = expr(""" CASE 
                                    WHEN current_domain = reference_domain THEN True
                                    ELSE False
                                END AS is_self_reference
                            """)
    
    return (
        df.withColumn('reference_domain',handle_refernce_null)
        .withColumn('is_self_reference',is_self_reference)
    )

def extract_browser(df: DataFrame):
    parse_browser_udf = udf(lambda ua: parse(ua).browser.family, StringType())

    return (
        df.withColumn('browser',parse_browser_udf("user_agent"))
    )

def extract_os(df: DataFrame):
    parse_browser_udf = udf(lambda ua: parse(ua).os.family, StringType())

    return (
        df.withColumn('os',parse_browser_udf("user_agent"))
    )

def handle_product_id(df : DataFrame):
    handle_null_product_id = expr("IFNULL(product_id,-1)")

    return (
        df.withColumn("product_id",handle_null_product_id)
    )   


def generate_key(df : DataFrame):
    gen_location_key = abs(hash('country_code'))
    gen_date_key = date_format("local_time", "yyyyMMddHH").cast('int')
    gen_reference_key = abs(hash('reference_domain'))
    gen_browser_key = abs(hash('browser'))
    gen_os_key = abs(hash('os'))

    return (
        df.withColumn('location_key',gen_location_key)
        .withColumn('date_key',gen_date_key)
        .withColumn('reference_key',gen_reference_key)
        .withColumn('browser_key',gen_browser_key)
        .withColumn('os_key',gen_os_key)
        .withColumnRenamed("product_id","product_key")
    )

def handle_fact_view(df : DataFrame):
    gen_fact_key = md5(
            concat(
                coalesce(col("date_key").cast("string"), lit("")),
                coalesce(col("location_key").cast("string"), lit("")),
                coalesce(col("product_key").cast("string"), lit("")),
                coalesce(col("store_id").cast("string"), lit("")),
                coalesce(col("reference_key").cast("string"), lit("")),
                coalesce(col("browser_key").cast("string"), lit("")),
                coalesce(col("os_key").cast("string"), lit(""))
            )
        )
    
    return (
        df.groupBy("date_key",
                    "location_key",
                    "product_key",
                    "store_id",
                    "reference_key",
                    "browser_key",
                    "os_key"
            )
            .agg(expr("count(*) AS total_view"))
            .withColumn("key",gen_fact_key)
     )


def handle_dim_browser(df : DataFrame):


    return (
        df.selectExpr("browser_key",
                "browser AS browser_name")
        .distinct()
    )

def handle_dim_os(df : DataFrame):
    return (
        df.selectExpr("os_key",
                    "os AS os_name")
            .distinct()
    )

def handle_dim_reference(df : DataFrame):
    return (
        df.selectExpr("reference_key",
                "reference_domain",
                "is_self_reference") \
            .distinct()
    )

def data_check(df: DataFrame):
    email_regex = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    ipv4_regex = r"^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
    url_regex = "^(http[s]?:\\/\\/)?([a-zA-Z0-9\\-]+\\.)+[a-zA-Z]{2,6}(\\/[^\\s]*)?$"
    df_check = (df
            .withColumn("is_valid_email",
                        when(col("email_address").rlike(email_regex), True)
                        .when(col("email_address").rlike(""), True)
                        .when(col("email_address").isNull(), True)
                        .otherwise(False))
            .withColumn("is_valid_product",
                        when(col("product_id").isNull(),False)
                        .otherwise(True))
            .withColumn("is_valid_local_time",
                        when(col("local_time").isNull(),False)
                        .otherwise(True))
            .withColumn("is_valid_ip",
                        when(col("ip").rlike(ipv4_regex), True)
                        .otherwise(False))
            .withColumn("is_valid_url",
                        when(col("current_url").rlike(url_regex), True)
                        .otherwise(False))
        )

    true_data = (
        df_check
        .filter(
            (col("is_valid_email") == True) & 
            (col("is_valid_product") == True) & 
            (col("is_valid_ip") == True) & 
            (col("is_valid_url") == True) & 
            (col("is_valid_local_time") == True))
    )

    false_data =(
        df_check
        .filter(
            (col("is_valid_email") == False) |
            (col("is_valid_product") == False) |
            (col("is_valid_ip") == False) | 
            (col("is_valid_url") == False) |
            (col("is_valid_local_time") == False)
        )
    )

    return true_data,false_data


def process_raw_data(df_error : DataFrame,batch_id: int):
    return (
            df_error.withColumn("option", to_json(col("option")))
            .withColumn("batch_id",lit(batch_id))
            .withColumn("insert_dt",current_timestamp())
        )


def process_batch(df_product_view: DataFrame,batch_id: int,db_ops):
    
    df_product_view_correct,df_product_view_incorrect = data_check(df_product_view)


    df_behavior_extract_domain = extract_domain(df_product_view_correct)
    df_extract_country_code = extract_country_code(df_behavior_extract_domain)
    df_handle_refernce = handle_refernce(df_extract_country_code)
    df_extract_browser = extract_browser(df_handle_refernce)
    df_extract_os = extract_os(df_extract_browser)
    df_handle_product_id = handle_product_id(df_extract_os)

    df_generate_key = generate_key(df_handle_product_id)
    df_generate_key_cache = df_generate_key.cache()

    df_fact_view = handle_fact_view(df_generate_key_cache)
    df_dim_browser = handle_dim_browser(df_generate_key_cache)
    df_dim_os = handle_dim_os(df_generate_key_cache)
    df_dim_refer = handle_dim_reference(df_generate_key_cache)

    df_product_view_raw = process_raw_data(df_product_view_correct,batch_id)
    df_product_view_error_raw = process_raw_data(df_product_view_incorrect,batch_id)

    db_ops.save_to_postgres(df_product_view_raw,"product_view",mode="append")
    db_ops.save_to_postgres(df_product_view_error_raw,"product_view_error",mode="append")

    db_ops.upsert_to_fact_view(df_fact_view)
    db_ops.upsert_to_dim_browser(df_dim_browser)
    db_ops.upsert_to_dim_os(df_dim_os)
    db_ops.upsert_to_dim_reference(df_dim_refer)