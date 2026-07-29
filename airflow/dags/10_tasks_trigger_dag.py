"""DAG de exemplo que dispara outra DAG (8_tasks_trigger_params) com parâmetros.

Cada task usa o TriggerDagRunOperator para criar uma run da DAG 8 passando o
tipo no conf (o mesmo JSON do Trigger DAG w/ config), uma task por tipo.

Obs.: a DAG 8 precisa estar despausada na UI para as runs disparadas executarem.
"""
from airflow import DAG
from airflow.operators.dummy import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'herculanocm.consult@srmasset.com',
    'on_failure_callback': None,  # callback function to be called on failure
    'on_success_callback': None,  # callback function to be called on success
    'retries': 0, # number of retries in case of failure
    'retry_delay': timedelta(minutes=5), # delay between retries
    'execution_timeout': timedelta(minutes=180), # maximum time allowed for the task to run
}

with DAG(
        dag_id='10_tasks_trigger_dag', # unique identifier for the DAG
        schedule_interval=None, # somente via trigger manual
        start_date=datetime(2024, 6, 1), # start date for the DAG (June 1, 2024)
        description='Dispara a DAG 8 com o conf de cada tipo via TriggerDagRunOperator.', # description of the DAG
        default_args=default_args, # default arguments for the tasks in the DAG
        catchup=False, # whether to catch up on missed runs or not
        max_active_runs=1, # maximum number of active runs for the DAG
        concurrency=3, # maximum number of concurrent tasks for the DAG
        tags=['hello_world', 'training'], # tags for categorizing the DAG
) as dag:

    task_start = EmptyOperator(
        task_id='task_start', # unique identifier for the task
        dag=dag # reference to the DAG that the task belongs to
    )

    task_trigger_dag8_pagamento = TriggerDagRunOperator(
        task_id='task_trigger_dag8_pagamento', # unique identifier for the task
        trigger_dag_id='8_tasks_trigger_params', # DAG alvo que será disparada
        conf={'tipo': 'pagamento'}, # conf enviado para a run (dag_run.conf da DAG 8)
        wait_for_completion=True, # não espera a run da DAG 8 terminar
        dag=dag # reference to the DAG that the task belongs to
    )

    task_trigger_dag8_taxa = TriggerDagRunOperator(
        task_id='task_trigger_dag8_taxa', # unique identifier for the task
        trigger_dag_id='8_tasks_trigger_params', # DAG alvo que será disparada
        conf={'tipo': 'taxa'}, # conf enviado para a run (dag_run.conf da DAG 8)
        wait_for_completion=True, # não espera a run da DAG 8 terminar
        dag=dag # reference to the DAG that the task belongs to
    )

    task_trigger_dag8_transferencia = TriggerDagRunOperator(
        task_id='task_trigger_dag8_transferencia', # unique identifier for the task
        trigger_dag_id='8_tasks_trigger_params', # DAG alvo que será disparada
        conf={'tipo': 'transferencia'}, # conf enviado para a run (dag_run.conf da DAG 8)
        wait_for_completion=True, # não espera a run da DAG 8 terminar
        dag=dag # reference to the DAG that the task belongs to
    )

    task_end = EmptyOperator(
        task_id='task_end', # unique identifier for the task
        dag=dag # reference to the DAG that the task belongs to
    )

task_start >> [task_trigger_dag8_pagamento, task_trigger_dag8_taxa, task_trigger_dag8_transferencia] >> task_end
