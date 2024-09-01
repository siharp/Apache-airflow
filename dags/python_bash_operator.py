# import libraries
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

#create defauld argumen
default_args= {
    'owner': 'sihar',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

#create python function
def hello():
    print('hello word')

def great(name, age):
    print(f'hello, myname is {name}, and i am {age} years old')


dag = DAG(
    'python_bash_operator',
    default_args= default_args,
    start_date= datetime(2024, 8, 20),
    schedule_interval= '@daily'
)

task1= PythonOperator(
    task_id= 'hello',
    python_callable= hello,
    dag= dag
)

task2= BashOperator(
    task_id= 'show_date',
    bash_command="echo '{{execution_date.strftime(\'%Y-%m-%d\')}}' ",
    dag=dag
)

task3 = PythonOperator(
    task_id= 'great',
    python_callable= great,
    op_kwargs= {
        'name': 'sihar',
        'age': 28
    }
)

task1 >> task2 >> task3