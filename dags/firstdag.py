from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
import pandas as pd


default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'my_first_dag2',
    default_args=default_args,
    description='This is my first dag that we write',
    tags=['data engineering', 'sihar'],
    start_date=datetime(2023, 7, 29, 2),
    schedule_interval='@daily',
)

task1 = BashOperator(task_id='first_task', bash_command="echo hello world, this is the first task!", dag=dag)

task2 = BashOperator(task_id='second_task', bash_command="echo hello world, this is the second task!",dag=dag)

task3 = BashOperator(task_id='third_task', bash_command="echo hey, I am task3 and will be running after task1 at the same time as task2!", dag=dag)

task4 = BashOperator(task_id='four_task', bash_command="echo hey, I am task4 and will be running after task1 at the same time as task3!", dag=dag)

task1 >> [task2, task3] >> task4

