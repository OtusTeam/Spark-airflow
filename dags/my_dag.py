from airflow import DAG

from airflow.operators.python import PythonOperator

import datetime

default_args = {
    'start_date': datetime.datetime(2021, 8, 10)
}

def _my_function(**context):
    print(context)

with DAG('dag_my',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    my_hive = PythonOperator(task_id='hive',
                             python_callable=_my_function,
                             provide_context=True)

