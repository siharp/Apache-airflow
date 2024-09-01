from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'sihar',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'postgres_operator',
    default_args=default_args,
    start_date=datetime(2024, 8, 30),
    schedule_interval='@once',
    template_searchpath= '/opt/airflow/sql',
    catchup=False  # Menonaktifkan catchup jika Anda ingin menjalankan DAG segera
) as dag:
    
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='local_postgres',
        sql='create_table.sql'  # Pastikan path benar
    )

    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='local_postgres',
        sql='insert_data.sql'  # Pastikan path benar
    )

    show_data = PostgresOperator(
        task_id='show_data',
        postgres_conn_id='local_postgres',
        sql='SELECT * FROM karyawan'
    )

    create_table >> insert_data >> show_data
