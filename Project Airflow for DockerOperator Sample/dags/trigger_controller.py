from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
'owner'                 : 'airflow',
'description'           : 'Use of the DockerOperator',
'depend_on_past'        : False,
'start_date'            : datetime(2021, 5, 1),
'email_on_failure'      : False,
'email_on_retry'        : False,
'retries'               : 1,
'retry_delay'           : timedelta(minutes=5)
}

with DAG('trigger_controller', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    start_dag = DummyOperator(
        task_id='start_dag'
        )

    end_dag = DummyOperator(
        task_id='end_dag'
        )       

    t3 = TriggerDagRunOperator(
        task_id="trigger_test",
        trigger_dag_id="trigger_target_dag",
        conf={
            "month" : 1,
            "yaer" : 2 ,
            "billing_alias" : "yes",
            "billing_id" : "banana"
            }
        )

    start_dag >> t3 >> end_dag
