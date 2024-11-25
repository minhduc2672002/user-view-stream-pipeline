from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.configuration import conf

import os


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 25),
    }

with DAG('test_spark',
         default_args = default_args,
         schedule_interval= None,
         catchup=False
        ) as dag:


    spark_stream_to_postgres = SparkSubmitOperator(
        task_id="spark_stream_to_postgres2",
        conn_id="spark-conn",
        application="dags/workspace/spark/main_streaming.py",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3",
        # archives="/opt/airflow/dags/workspace/pyspark_venv.tar.gz#environment",
        py_files="/opt/airflow/dags/workspace/spark/postgres.zip",
        conf={
        'spark.pyspark.driver.python': 'python',
        'spark.pyspark.python': '/data/pyspark_venv/bin/python',
        "spark.sql.shuffle.partitions":20
        }
    )
spark_stream_to_postgres
    
    