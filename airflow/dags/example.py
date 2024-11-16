from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.email import EmailOperator
from airflow.hooks.base import BaseHook
from airflow.configuration import conf

import os
import sys

script_dir = os.path.join(os.path.dirname(__file__), 'src/kafka/')

def print_hello():
    smtp_mail_from = conf.get('smtp', 'smtp_mail_from')  # Thay đổi để lấy thông tin cần thiết

    # In địa chỉ email vào log
    print(f'Sending email from: {smtp_mail_from}')


def print_hello_2(str):
    print(f"{str}")


def my_task_function(**kwargs):
    print("Running my task!")
    raise ValueError("Simulated error!")  # Mô phỏng lỗi

def send_retry_email(context):
    email_task = EmailOperator(
        task_id='send_retry_email',
        to='recipient@example.com',
        subject='Airflow Alert: Task Retrying',
        html_content=f"""<h3>Task Retrying Alert</h3>
                         <p>Task <strong>{context['task_instance'].task_id}</strong> is retrying.</p>
                         <p>Current attempt: {context['task_instance'].try_number}</p>""",
    )
    email_task.execute(context=context)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 14),
    'retries': 1,
    'catchup':False
}

with DAG('example',default_args = default_args) as dag:
    task1 = PythonOperator(
    task_id='task_1',
    python_callable = print_hello
    )

    task2 = PythonOperator(
    task_id='task_2',
    python_callable = print_hello_2,
    op_kwargs={'str':'hello minh duc!'}
    )

    sources = TaskGroup('sources')
    with TaskGroup("data_processing", tooltip="Các task xử lý dữ liệu",parent_group=None) as data_processing:
        
        task3 = EmptyOperator(task_id='task3')
        task4 = EmptyOperator(task_id='task4')
        task5 = EmptyOperator(task_id='task5')
        
        
    with TaskGroup("data_stored", tooltip="Các task xử lý dữ liệu",parent_group=None) as data_stored:
    
        task3 = EmptyOperator(task_id='task3')
        task4 = EmptyOperator(task_id='task4')
        task5 = EmptyOperator(task_id='task5')

    email_failure = EmailOperator(
        task_id='send_email_on_succes',
        to='minhduc2672002@gmail.com',
        subject='Airflow Alert: Pipeline Success',
        html_content="""<h3>Task Failure Alert</h3>
                        <p>Task <strong>{{ task.task_id }}</strong> in DAG <strong>{{ dag.dag_id }}</strong> has failed.</p>
                        <p>Please check the Airflow UI for more details.</p>""",
    )

            
data_processing >> task1 >> data_stored >>  task2 >> email_failure
    
    