"""DAG de exemplo para validar o ambiente do treinamento."""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def exec_python_1(**kwargs):
    print("exec_python_1")

def exec_python_2(**kwargs):
    print("exec_python_2")


default_args = {
    'owner': 'herculanocm.consult@srmasset.com',
    'on_failure_callback': None,  # callback function to be called on failure
    'on_success_callback': None,  # callback function to be called on success
    'retries': 0, # number of retries in case of failure
    'retry_delay': timedelta(minutes=5), # delay between retries
    'execution_timeout': timedelta(minutes=180), # maximum time allowed for the task to run
}

with DAG(
        dag_id='2_tasks', # unique identifier for the DAG
        schedule_interval=None, # somente via trigger manual
        start_date=datetime(2024, 6, 1), # start date for the DAG (June 1, 2024)
        description='Pipe de Tasks para validar o ambiente do treinamento.', # description of the DAG
        default_args=default_args, # default arguments for the tasks in the DAG
        catchup=False, # whether to catch up on missed runs or not
        params={"custom_param": "default_value"}, # custom parameters for the DAG
        max_active_runs=1, # maximum number of active runs for the DAG
        concurrency=1, # maximum number of concurrent tasks for the DAG
        tags=['hello_world', 'training'], # tags for categorizing the DAG
) as dag:

    task_exec_python_1 = PythonOperator(
        task_id='task_exec_python_1', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=exec_python_1, # callable function to be executed by the task
        dag=dag # reference to the DAG that the task belongs to
    )

    task_exec_python_2 = PythonOperator(
        task_id='task_exec_python_2', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=exec_python_2, # callable function to be executed by the task
        dag=dag # reference to the DAG that the task belongs to
    )

# O operador ">>" define a dependência (ordem de execução) entre as tasks:
# task_exec_python_1 executa primeiro e, somente se ela for concluída com sucesso,
# task_exec_python_2 é executada. É o operador de bitshift do Python, que o Airflow
# sobrescreve como atalho para task_exec_python_1.set_downstream(task_exec_python_2).
# O inverso "<<" (set_upstream) também existe: a >> b equivale a b << a.
task_exec_python_1 >> task_exec_python_2