from datetime import datetime, timedelta
from airflow import DAG
from pprint import pprint
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import task

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



with DAG('trigger_target_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    @task(task_id='target_dagger')
    def target_dagger(**kwargs):
        print ("aaaaaaaaaaaaaaaaaaaaaaaa")
        valor = kwargs['dag_run'].conf['billing_alias']
        print (f"valor =  {valor}")
        task_instance = kwargs['task_instance']
        task_instance.xcom_push(key='banana', value = valor)
        return
    run_this = target_dagger()
    
    @task(task_id='set_taskid')
    def set_taskid(task_name):  
        valor = task_name
        print(valor)
        return valor
    set_this = set_taskid("{{task_instance.xcom_pull('target_dagger', key='banana')}}")

    start_dag = DummyOperator(
        task_id='start_dag'
        )

    end_dag = DummyOperator(
        task_id='end_dag'
        )

    t3 = BashOperator(
        task_id=f"{{{{task_instance.xcom_pull('target_dagger', key='banana')[0]}}}}",
        bash_command = "echo 'here is the billing allias: {{task_instance.xcom_pull('target_dagger', key='banana')}}'",
        )

    start_dag >> run_this >> set_this >> t3 >> end_dag
