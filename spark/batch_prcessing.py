from pyspark.sql.types import StringType, StructType, StructField,IntegerType
from pyspark.sql.functions import *
from user_agents import parse

def process_batch(df: DataFrame,db_ops):
    #EXTRACT DOMAIN FROM URL
    df_behavior = df
    extract_current_domain = split(col("current_url"),"/")[2]
    extract_reference_domain = split(col("referrer_url"),"/")[2]
    num_parts = size(split(extract_current_domain, r"\."))
    
    
    df_behavior_extract_domain  = df_behavior \
    .withColumn('current_domain',extract_current_domain) \
    .withColumn('reference_domain',extract_reference_domain)

    extract_coutnry_code = upper(split(extract_current_domain,r"\.")[num_parts -1])
    fix_country_code =  expr("""CASE
                                    WHEN country_code = 'COM' THEN 'US'
                                    WHEN country_code = 'AFRICA'  THEN 'BF'
                                    WHEN country_code = 'MEDIA' THEN 'LY'
                                    WHEN country_code = 'STORE' THEN 'CU'
                                    WHEN country_code = '' THEN 'Undefined'
                                    ELSE country_code
                                END AS country_code
                            """)
    gen_location_key = abs(hash('country_code'))
    
    #GENKEY DATE
    gen_date_key = date_format("local_time", "yyyyMMddHH").cast('int')
    
    #GENKEY REFERENCE
    handle_refernce_null = expr("IFNULL(reference_domain,'Undefined') AS reference_domain")
    is_self_reference = expr(""" CASE 
                                    WHEN current_domain = reference_domain THEN True
                                    ELSE False
                                END AS is_self_reference
                            """)
    gen_reference_key = abs(hash('reference_domain'))
    
    #GENKEY BROWER
    parse_browser_udf = udf(lambda ua: parse(ua).browser.family, StringType())
    gen_browser_key = abs(hash('browser'))
    
    #GENKEY OS
    parse_os_udf = udf(lambda ua: parse(ua).os.family, StringType())
    gen_os_key = abs(hash('os'))
    
    #HANDLE PRODUCT
    handle_null_product_id = expr("IFNULL(product_id,-1)")
    
    
    df_behavior_genkey = df_behavior_extract_domain \
    .withColumn('country_code',extract_coutnry_code) \
    .withColumn('country_code',fix_country_code) \
    .withColumn('location_key',gen_location_key) \
    .withColumn('date_key',gen_date_key) \
    .withColumn('reference_domain',handle_refernce_null) \
    .withColumn('is_self_reference',is_self_reference) \
    .withColumn('reference_key',gen_reference_key) \
    .withColumn('browser',parse_browser_udf("user_agent")) \
    .withColumn('browser_key',gen_browser_key) \
    .withColumn('os',parse_os_udf("user_agent")) \
    .withColumn('os_key',gen_os_key) \
    .withColumn("product_id",handle_null_product_id) \
    .withColumnRenamed("product_id","product_key")

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
    
    fact_view = df_behavior_genkey \
    .groupBy("date_key",
            "location_key",
            "product_key",
            "store_id",
           "reference_key",
           "browser_key",
           "os_key"
           ) \
    .agg(expr("count(*) AS total_view")) \
    .withColumn("key",gen_fact_key)

    ##DIM BROWSER
    df_dim_browser = df_behavior_genkey \
    .selectExpr("browser_key",
                "browser AS browser_name") \
    .distinct()
    
    ##DIM OS
    df_dim_os = df_behavior_genkey \
    .selectExpr("os_key",
                "os AS os_name") \
    .distinct()
    

    ##DIM REFER
    df_dim_refer = df_behavior_genkey \
    .selectExpr("reference_key",
                "reference_domain",
                "is_self_reference") \
    .distinct()



    db_ops.upsert_to_fact_view(fact_view)
    db_ops.upsert_to_dim_browser(df_dim_browser)
    db_ops.upsert_to_dim_os(df_dim_os)
    db_ops.upsert_to_dim_reference(df_dim_refer)