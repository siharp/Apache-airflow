from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.decorators.python import PythonOperator
import os

default_args={
    'owner': 'sihar',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

@task(multiple_outputs=True)
def hello():
    return {
        'name': 'sihar',
        'age': 29
    }

@task()
def greetings(name, age):
    print(f'hello {name}, your age is {age}')

@task()
def print_directory():
    print(os.listdir())

with DAG(
    'test_dag',
    default_args=default_args,
    start_date= datetime(2024, 8, 31),
    schedule_interval= '@daily'
):
    name=hello()
    greetings(name['name'], name['age']) >> print_directory()