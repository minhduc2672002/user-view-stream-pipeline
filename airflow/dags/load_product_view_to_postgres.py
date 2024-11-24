from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.email import EmailOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow.hooks.base import BaseHook
from airflow.configuration import conf

import os


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 24),
    }

with DAG('load_product_view_to_postgres',
         default_args = default_args,
         schedule_interval= None,
         catchup=False
        ) as dag:


    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end",trigger_rule="one_done")

    load_dim_date = SparkSubmitOperator(
        task_id="load_dim_date",
        conn_id="spark-conn",
        application="dags/workspace/spark/load_dim_date.py",
        packages="org.postgresql:postgresql:42.7.3",
        py_files="/opt/airflow/dags/workspace/spark/postgres.zip",
        name="Load Dim Date",
        conf={
        'spark.pyspark.driver.python': 'python',
        'spark.pyspark.python': '/data/pyspark_venv/bin/python',
        }
    )

    load_dim_product = SparkSubmitOperator(
        task_id="load_dim_product",
        conn_id="spark-conn",
        application="dags/workspace/spark/load_dim_product.py",
        packages="org.postgresql:postgresql:42.7.3",
        py_files="/opt/airflow/dags/workspace/spark/postgres.zip",
        name="Load Dim Product",
        conf={
        'spark.pyspark.driver.python': 'python',
        'spark.pyspark.python': '/data/pyspark_venv/bin/python',
        }
    )

    load_dim_location = SparkSubmitOperator(
        task_id="load_dim_location",
        conn_id="spark-conn",
        application="dags/workspace/spark/load_dim_location.py",
        packages="org.postgresql:postgresql:42.7.3",
        py_files="/opt/airflow/dags/workspace/spark/postgres.zip",
        name="Load Dim Location",
        conf={
        'spark.pyspark.driver.python': 'python',
        'spark.pyspark.python': '/data/pyspark_venv/bin/python',
        }
    )

    spark_stream = SparkSubmitOperator(
        task_id="spark_stream",
        conn_id="spark-conn",
        application="dags/workspace/spark/main_streaming.py",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3",
        # archives="/opt/airflow/dags/workspace/pyspark_venv.tar.gz#environment",
        py_files="/opt/airflow/dags/workspace/spark/postgres.zip",
        conf={
        'spark.pyspark.driver.python': 'python',
        'spark.pyspark.python': '/data/pyspark_venv/bin/python',
        }
    )

    email_failure = EmailOperator(
    task_id='email_failure',
    to='minhduc2672002@gmail.com',
    subject='Airflow Alert: Task Failure',
    html_content="""<h3>Task Failure Alert</h3>
                    <p>Task <strong>{{ task.task_id }}</strong> in DAG <strong>{{ dag.dag_id }}</strong> has failed.</p>
                    <p>Please check the Airflow UI for more details.</p>""",
    trigger_rule="one_failed"
    )
    

    email_success = EmailOperator(
        task_id='email_success',
        to='minhduc2672002@gmail.com',
        subject='Airflow Alert: Pipeline Success',
        html_content="""<h3>Pipeline Success Alert</h3>
                        <p>DAG <strong>{{ dag.dag_id }}</strong> has successful.</p>
                        <p>Please check the Airflow UI for more details.</p>""",
        trigger_rule="all_success"
    )
    
            
start >> [load_dim_date, load_dim_location, load_dim_product]
[load_dim_date, load_dim_location, load_dim_product] >> spark_stream
spark_stream >> [email_success, email_failure] 
[email_success, email_failure] >> end
    
    