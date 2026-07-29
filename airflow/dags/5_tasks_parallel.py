"""DAG de exemplo para validar o ambiente do treinamento."""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

def exec_python(**kwargs):
    print(f"exec_python with param={kwargs.get('param')}")



default_args = {
    'owner': 'herculanocm.consult@srmasset.com',
    'on_failure_callback': None,  # callback function to be called on failure
    'on_success_callback': None,  # callback function to be called on success
    'retries': 0, # number of retries in case of failure
    'retry_delay': timedelta(minutes=5), # delay between retries
    'execution_timeout': timedelta(minutes=180), # maximum time allowed for the task to run
}

with DAG(
        dag_id='5_tasks_parallel', # unique identifier for the DAG
        schedule_interval=None, # somente via trigger manual
        start_date=datetime(2024, 6, 1), # start date for the DAG (June 1, 2024)
        description='Pipe de Tasks Paralelas', # description of the DAG
        default_args=default_args, # default arguments for the tasks in the DAG
        catchup=False, # whether to catch up on missed runs or not
        params={"custom_param": "default_value"}, # custom parameters for the DAG
        max_active_runs=1, # maximum number of active runs for the DAG
        concurrency=1, # maximum number of concurrent tasks for the DAG
        tags=['hello_world', 'training'], # tags for categorizing the DAG
) as dag:

    task_start = EmptyOperator(
        task_id='task_start', # unique identifier for the task
        dag=dag # reference to the DAG that the task belongs to
    )

    task_exec_python_1 = PythonOperator(
        task_id='task_exec_python_1', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=exec_python, # callable function to be executed by the task
        op_kwargs={'param': 'value1'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_exec_python_2 = PythonOperator(
        task_id='task_exec_python_2', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=exec_python, # callable function to be executed by the task
        op_kwargs={'param': 'value2'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_exec_python_3 = PythonOperator(
        task_id='task_exec_python_3', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=exec_python, # callable function to be executed by the task
        op_kwargs={'param': 'value3'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_end = EmptyOperator(
        task_id='task_end', # unique identifier for the task
        dag=dag # reference to the DAG that the task belongs to
    )

task_start >> [task_exec_python_1, task_exec_python_2, task_exec_python_3] >> task_end