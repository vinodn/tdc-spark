from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

with DAG(
    dag_id='tdc-spark',
    schedule_interval='@daily',
    start_date=datetime(2022, 5, 16)
) as dag:
    task_1 = DummyOperator(task_id='task_1')
    task_2 = DummyOperator(task_id='task_2')
    task_3 = DummyOperator(task_id='task_3')
    task_4 = DummyOperator(task_id='task_4')
    task_5 = DummyOperator(task_id='task_5')
    task_6 = DummyOperator(task_id='task_6')
    task_1 >> [task_2, task_3] >> [task_4, task_5, task_6]