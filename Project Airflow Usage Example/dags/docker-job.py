from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import BranchPythonOperator
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

with DAG('docker_operator_dag_funcionava', default_args=default_args, schedule_interval="30 * * * *", catchup=False) as dag:
    start_dag = DummyOperator(
        task_id='start_dag'
        )

    end_dag = DummyOperator(
        task_id='end_dag'
        )       

    t3 = DockerOperator(
        task_id='emergencialvestoqueconsulta',
        image='emergencialvestoqueconsulta',
        container_name='temp_emergencialvestoqueconsulta',
        api_version='auto',
        auto_remove=True,
        command="python wms_function_vEstoqueConsulta.py",
        docker_url="TCP://docker-socket-proxy:2375",
        network_mode="bridge"
        )

    t5= DockerOperator(
        task_id='emergencialvrecebimento',
        image='emergencialvrecebimento',
        container_name='temp_emergencialvrecebimento',
        api_version='auto',
        auto_remove=True,
        command="python wms_function_vRecebimento.py",
        docker_url="TCP://docker-socket-proxy:2375",
        network_mode="bridge"
        )

    start_dag >> t3 >> t5 >> end_dag
