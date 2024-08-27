# import libaries
import pandas as pd 
import os
import json
from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


def json_to_parquet():
    '''
    python sript to transform json to parquet
    
    '''
    # list json file
    json_files = [file for file in os.listdir('/opt/airflow/dags/source/') if file.endswith(".json")]

    # fungsion to get channel name 
    def get_channel_name(channel_id, channels):
        channel = next((c for c in channels if c['channel_id'] == channel_id), None)
        return channel['channel_name'] if channel else 'Unknown Channel'


    for json_file in json_files:

        name = json_file.split('.')[0]

        result = []

        with open(os.path.join('/opt/airflow/dags/source/', json_file), 'r') as f:
            json_sample = json.load(f)

            for item in json_sample['product_skus']:
                result_item = {
                    'barcode': item['item_code'],
                    'size': item['variation_values'][0]['value'],
                    'SKU': json_sample['item_group_name'],
                }

                for price in item['prices']:
                    result_item[get_channel_name(price['channel_id'], json_sample['channels'])] = price['sell_price']

                result.append(result_item)

        df = pd.DataFrame(result)
        df.to_parquet(f'/opt/airflow/dags/output/{name}.parquet')



default_args = {
    'owner': 'sihar',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)}

dag = DAG(
    'DAG_sihar',
    default_args=default_args,
    description='DAG Creation json to parquet',
    tags=['DE', 'TechnicalTest','sihar'],
    start_date=datetime(2024, 2, 1),
    schedule_interval='0 11 * * *',
)

start = DummyOperator(
    task_id ='start',
    dag = dag
)

json_to_parquet = PythonOperator(
        task_id='json_to_parquet',
        python_callable=json_to_parquet
    )

end = DummyOperator(
    task_id ='end',
    dag = dag
)


start >> json_to_parquet >> end