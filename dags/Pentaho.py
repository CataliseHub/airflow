from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow_pentaho.operators.KitchenOperator import KitchenOperator
from airflow_pentaho.operators.PanOperator import PanOperator
from airflow_pentaho.operators.CarteJobOperator import CarteJobOperator
from airflow_pentaho.operators.CarteTransOperator import CarteTransOperator

"""
# For versions before 2.0
from airflow.operators.airflow_pentaho import KitchenOperator
from airflow.operators.airflow_pentaho import PanOperator
from airflow.operators.airflow_pentaho import CarteJobOperator
from airflow.operators.airflow_pentaho import CarteTransOperator
"""

DAG_NAME = "pdi_example_flow"
DEFAULT_ARGS = {
    'owner': 'Examples',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(dag_id=DAG_NAME,
         default_args=DEFAULT_ARGS,
         dagrun_timeout=timedelta(hours=2),
         schedule_interval='30 0 * * *') as dag:

    trans1 = CarteTransOperator(
        dag=dag,
        task_id="trans1",
        trans="/home/bi/test_trans_1",
        params={"date": "{{ ds }}"})

    trans2 = CarteTransOperator(
        dag=dag,
        task_id="trans2",
        trans="/home/bi/test_trans_2",
        params={"date": "{{ ds }}"})

    job1 = CarteJobOperator(
        dag=dag,
        task_id="job1",
        job="/home/bi/test_job_1",
        params={"date": "{{ ds }}"})

    trans3 = CarteTransOperator(
        dag=dag,
        task_id="trans3",
        trans="/home/bi/test_trans_3",
        params={"date": "{{ ds }}"})

    trans4 = CarteTransOperator(
        dag=dag,
        task_id="trans4",
        trans="/home/bi/test_trans_4",
        params={"date": "{{ ds }}"})

    trans1 >> trans2 >> job1
    job1 >> trans3
    job1 >> trans4