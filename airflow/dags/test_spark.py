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
    'start_date': datetime(2024, 11, 17),
    }

with DAG('test_spark2',
         default_args = default_args,
         schedule_interval= None,
         catchup=False
        ) as dag:

    create_env = BashOperator(
        task_id = "create_env",
        bash_command="""
        VENV_DIR="pyspark_venv"
        VENV_ARCHIVE="pyspark_venv.tar.gz"
        WORK_DIR="/opt/airflow/dags/workspace"  # Đặt thư mục làm việc bạn muốn

        # Chuyển đến thư mục làm việc mong muốn
        cd $WORK_DIR

        if [ -f "$VENV_ARCHIVE" ]; then
            echo "Tệp tarball '$VENV_ARCHIVE' đã tồn tại. Bỏ qua việc tạo mới."
        else
            # Kiểm tra xem virtual environment đã tồn tại chưa
            if [ -d "$VENV_DIR" ]; then
                echo "Virtual environment '$VENV_DIR' đã tồn tại. Bỏ qua tạo mới."
            else
                echo "Virtual environment '$VENV_DIR' không tồn tại. Tạo mới..."

                # Tạo virtualenv, cài đặt yêu cầu, và đóng gói lại
                python -m venv $VENV_DIR &&
                source $VENV_DIR/bin/activate &&
                pip install -r spark/requirements.txt &&
                venv-pack -o $VENV_ARCHIVE

                echo "Virtual environment được tạo tại đường dẫn:"
                echo "$(pwd)/$VENV_DIR"
            fi
        fi
        """
    )

    # spark_create_dimension = SparkSubmitOperator(
    #     task_id="spark_create_dimension",
    #     conn_id="spark-conn",
    #     application="dags/workspace/spark/create_dimension.py",
    #     packages="org.postgresql:postgresql:42.7.3",
    #     py_files="dags/workspace/spark/postgres.zip"
    # )

    spark_stream = SparkSubmitOperator(
        task_id="spark_stream",
        conn_id="spark-conn",
        application="dags/workspace/spark/main_streaming.py",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3",
        archives="/opt/airflow/dags/workspace/pyspark_venv.tar.gz#environment",
        py_files="/opt/airflow/dags/workspace/spark/postgres.zip"
    )
    
    # delete_env = BashOperator(
    #     task_id = "delete_env",
    #     bash_command="""
    #     VENV_DIR="pyspark_venv"
    #     VENV_ARCHIVE="pyspark_venv.tar.gz"
    #     WORK_DIR="/opt/airflow/dags/workspace"  # Đặt thư mục làm việc bạn muốn

    #     # Chuyển đến thư mục làm việc mong muốn
    #     cd $WORK_DIR

    #     # Kiểm tra nếu tệp tarball tồn tại và xóa nếu có
    #     if [ -f "$VENV_ARCHIVE" ]; then
    #         echo "Tệp tarball '$VENV_ARCHIVE' đã tồn tại. Đang xóa..."
    #         rm -f "$VENV_ARCHIVE"
    #         echo "Đã xóa tệp tarball '$VENV_ARCHIVE'."
    #     else
    #         echo "Tệp tarball '$VENV_ARCHIVE' không tồn tại."
    #     fi

    #     # Kiểm tra nếu thư mục virtual environment tồn tại và xóa nếu có
    #     if [ -d "$VENV_DIR" ]; then
    #         echo "Virtual environment '$VENV_DIR' đã tồn tại. Đang xóa..."
    #         rm -rf "$VENV_DIR"
    #         echo "Đã xóa virtual environment '$VENV_DIR'."
    #     else
    #         echo "Virtual environment '$VENV_DIR' không tồn tại."
    #     fi
    #     """
    # )
            
create_env  >> spark_stream
    
    