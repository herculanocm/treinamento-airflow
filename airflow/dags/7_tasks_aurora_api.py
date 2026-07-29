"""DAG de exemplo para validar o ambiente do treinamento."""
import json
import urllib3
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

def exec_python(**kwargs):
    tipo = kwargs.get('tipo')
    ts = kwargs.get('ts')
    run_id = kwargs.get('run_id')
    tipo = kwargs.get('tipo')
    print(f"exec_python with tipo={tipo}, ts={ts}, run_id={run_id}")

    datetime_ts = datetime.strptime(ts[:19], '%Y-%m-%dT%H:%M:%S')

    sql_query = f"""
    select
    sum(valor) as total_valor
    from public.transacao t 
    where
    t.tipo = '{tipo}' and date_trunc('day', t.dt_transacao) = '{datetime_ts.strftime("%Y-%m-%d")}'
    limit 1
    """
    print(f"Executing SQL query: {sql_query}")
    hook = PostgresHook(postgres_conn_id='aurora_postgres_banco_master')

    # Execute the SQL query and fetch results
    results = hook.get_first(sql_query)
    print(f"Results {type(results)}: {results}")

    if results and results[0] is not None and results[0] > 0:
        print(f"Results from Aurora: {results[0]} for tipo={tipo} on {datetime_ts.strftime('%Y-%m-%d')}")
        # Salvando no xcom
        kwargs['ti'].xcom_push(key=f'total_valor_{tipo}', value={ 'tipo': tipo, 'valor': results[0], 'data': datetime_ts.strftime('%Y-%m-%d') })
    else:
        print(f"No results from Aurora for tipo={tipo} on {datetime_ts.strftime('%Y-%m-%d')}")


def post_api(**kwargs):
    tipo = kwargs.get('tipo')
    ti = kwargs['ti']

    # Recupera do XCom o total calculado pela task de consulta do mesmo tipo
    payload = ti.xcom_pull(task_ids=f'task_query_aurora_{tipo}', key=f'total_valor_{tipo}')
    print(f"post_api with tipo={tipo}, payload={payload}")

    if not payload:
        print(f"No XCom value for tipo={tipo}, nothing to post")
        return

    # valor vem como Decimal do Postgres — converte para float para o JSON
    body = json.dumps({'tipo': payload['tipo'], 'valor': float(payload['valor']), 'data': payload['data']})

    http = urllib3.PoolManager()
    response = http.request(
        'POST',
        'http://api-treinamento:3000/api/v1/total-tipo',
        body=body,
        headers={'Content-Type': 'application/json'},
        timeout=10.0,
    )
    response_body = response.data.decode('utf-8')
    print(f"API response status={response.status} body={response_body}")

    if response.status != 201:
        raise Exception(f"API returned status {response.status}: {response_body}")


def branch_status(**kwargs):
    tipo = kwargs.get('tipo')
    ts = kwargs.get('ts')
    data_referencia = datetime.strptime(ts[:19], '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d')

    # Se já existe linha para (data de referência, tipo) vai para o update,
    # senão vai para o insert
    sql_query = f"""
    select 1
    from public.transacao_parceiro_status
    where dt_referencia = '{data_referencia}' and tipo = '{tipo}'
    limit 1
    """
    print(f"Executing SQL query: {sql_query}")
    hook = PostgresHook(postgres_conn_id='aurora_postgres_banco_master')
    exists = hook.get_first(sql_query)

    proxima_task = f'task_update_status_{tipo}' if exists else f'task_insert_status_{tipo}'
    print(f"branch_status tipo={tipo} data={data_referencia} exists={bool(exists)} -> {proxima_task}")
    return proxima_task


def upsert_status(**kwargs):
    tipo = kwargs.get('tipo')
    operacao = kwargs.get('operacao')
    ts = kwargs.get('ts')
    data_referencia = datetime.strptime(ts[:19], '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d')

    # valor apurado pela task de consulta (None se não houve resultado)
    payload = kwargs['ti'].xcom_pull(task_ids=f'task_query_aurora_{tipo}', key=f'total_valor_{tipo}')
    valor_sql = str(float(payload['valor'])) if payload else 'NULL'

    # status = resultado do envio para a API: true se concluiu, false se falhou
    post_ti = kwargs['dag_run'].get_task_instance(f'task_post_api_{tipo}')
    status = post_ti is not None and post_ti.state == 'success'
    print(f"upsert_status tipo={tipo} operacao={operacao} data={data_referencia} valor={valor_sql} status={status}")

    if operacao == 'insert':
        sql_query = f"""
        insert into public.transacao_parceiro_status (dt_referencia, tipo, valor, status)
        values ('{data_referencia}', '{tipo}', {valor_sql}, {status})
        """
    else:
        sql_query = f"""
        update public.transacao_parceiro_status
        set valor = {valor_sql}, status = {status}, atualizado_em = now()
        where dt_referencia = '{data_referencia}' and tipo = '{tipo}'
        """
    print(f"Executing SQL query: {sql_query}")
    hook = PostgresHook(postgres_conn_id='aurora_postgres_banco_master')
    hook.run(sql_query)


default_args = {
    'owner': 'herculanocm.consult@srmasset.com',
    'on_failure_callback': None,  # callback function to be called on failure
    'on_success_callback': None,  # callback function to be called on success
    'retries': 0, # number of retries in case of failure
    'retry_delay': timedelta(minutes=5), # delay between retries
    'execution_timeout': timedelta(minutes=180), # maximum time allowed for the task to run
}

with DAG(
        dag_id='7_tasks_aurora_api', # unique identifier for the DAG
        schedule_interval=None, # somente via trigger manual
        start_date=datetime(2024, 6, 1), # start date for the DAG (June 1, 2024)
        description='Pipe de Tasks para Aurora 1.', # description of the DAG
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

    task_query_aurora_pagamento = PythonOperator(
        task_id='task_query_aurora_pagamento', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=exec_python, # callable function to be executed by the task
        op_kwargs={'tipo': 'pagamento'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_query_aurora_taxa = PythonOperator(
        task_id='task_query_aurora_taxa', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=exec_python, # callable function to be executed by the task
        op_kwargs={'tipo': 'taxa'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_query_aurora_transferencia = PythonOperator(
        task_id='task_query_aurora_transferencia', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=exec_python, # callable function to be executed by the task
        op_kwargs={'tipo': 'transferencia'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_post_api_pagamento = PythonOperator(
        task_id='task_post_api_pagamento', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=post_api, # callable function to be executed by the task
        op_kwargs={'tipo': 'pagamento'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_post_api_taxa = PythonOperator(
        task_id='task_post_api_taxa', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=post_api, # callable function to be executed by the task
        op_kwargs={'tipo': 'taxa'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_post_api_transferencia = PythonOperator(
        task_id='task_post_api_transferencia', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=post_api, # callable function to be executed by the task
        op_kwargs={'tipo': 'transferencia'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_branch_status_pagamento = BranchPythonOperator(
        task_id='task_branch_status_pagamento', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=branch_status, # callable function that returns the task_id to follow
        op_kwargs={'tipo': 'pagamento'}, # optional keyword arguments to pass to the callable function
        trigger_rule='all_done', # runs even if the post task failed, to record status=false
        dag=dag # reference to the DAG that the task belongs to
    )

    task_branch_status_taxa = BranchPythonOperator(
        task_id='task_branch_status_taxa', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=branch_status, # callable function that returns the task_id to follow
        op_kwargs={'tipo': 'taxa'}, # optional keyword arguments to pass to the callable function
        trigger_rule='all_done', # runs even if the post task failed, to record status=false
        dag=dag # reference to the DAG that the task belongs to
    )

    task_branch_status_transferencia = BranchPythonOperator(
        task_id='task_branch_status_transferencia', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=branch_status, # callable function that returns the task_id to follow
        op_kwargs={'tipo': 'transferencia'}, # optional keyword arguments to pass to the callable function
        trigger_rule='all_done', # runs even if the post task failed, to record status=false
        dag=dag # reference to the DAG that the task belongs to
    )

    task_insert_status_pagamento = PythonOperator(
        task_id='task_insert_status_pagamento', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=upsert_status, # callable function to be executed by the task
        op_kwargs={'tipo': 'pagamento', 'operacao': 'insert'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_update_status_pagamento = PythonOperator(
        task_id='task_update_status_pagamento', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=upsert_status, # callable function to be executed by the task
        op_kwargs={'tipo': 'pagamento', 'operacao': 'update'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_insert_status_taxa = PythonOperator(
        task_id='task_insert_status_taxa', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=upsert_status, # callable function to be executed by the task
        op_kwargs={'tipo': 'taxa', 'operacao': 'insert'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_update_status_taxa = PythonOperator(
        task_id='task_update_status_taxa', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=upsert_status, # callable function to be executed by the task
        op_kwargs={'tipo': 'taxa', 'operacao': 'update'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_insert_status_transferencia = PythonOperator(
        task_id='task_insert_status_transferencia', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=upsert_status, # callable function to be executed by the task
        op_kwargs={'tipo': 'transferencia', 'operacao': 'insert'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_update_status_transferencia = PythonOperator(
        task_id='task_update_status_transferencia', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=upsert_status, # callable function to be executed by the task
        op_kwargs={'tipo': 'transferencia', 'operacao': 'update'}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to
    )

    task_end = EmptyOperator(
        task_id='task_end', # unique identifier for the task
        trigger_rule='none_failed', # one branch of each pair is skipped, so all_success would skip it
        dag=dag # reference to the DAG that the task belongs to
    )

task_start >> [task_query_aurora_pagamento, task_query_aurora_taxa, task_query_aurora_transferencia]
task_query_aurora_pagamento >> task_post_api_pagamento
task_query_aurora_taxa >> task_post_api_taxa
task_query_aurora_transferencia >> task_post_api_transferencia
task_post_api_pagamento >> task_branch_status_pagamento
task_post_api_taxa >> task_branch_status_taxa
task_post_api_transferencia >> task_branch_status_transferencia
task_branch_status_pagamento >> [task_insert_status_pagamento, task_update_status_pagamento]
task_branch_status_taxa >> [task_insert_status_taxa, task_update_status_taxa]
task_branch_status_transferencia >> [task_insert_status_transferencia, task_update_status_transferencia]
[task_insert_status_pagamento, task_update_status_pagamento,
 task_insert_status_taxa, task_update_status_taxa,
 task_insert_status_transferencia, task_update_status_transferencia] >> task_end