from airflow import DAG
from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def hello(nama: str, marga: str):
    print(f"hallo ces {nama}, {marga}")

default_args = {
    'owner': 'sihar',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)}

dag = DAG(
    'DAG_sihar_2',
    default_args=default_args,
    schedule_interval='@once',
    start_date= datetime(2023,12,15)
)

task1 = BashOperator(task_id='first_task', bash_command="echo '{{execution_date.strftime(\'%Y-%m\')}}' ", dag=dag)
task2 = PythonOperator(
    task_id = 'testkwargst',
    python_callable=hello,
    op_kwargs={
        "nama" : 'sihar',
        "marga" : 'pangrib'
    },
    dag = dag
)

task1 >> task2