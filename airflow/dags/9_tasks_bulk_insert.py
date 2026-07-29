"""DAG de exemplo de carga em lote (bulk insert) a partir de arquivo no MinIO.

Trigger manual passando a chave do arquivo nas opções da run:
    {"pathkey": "entrada/transacoes_novas.csv"}

Baixa o CSV do bucket "treinamento" do MinIO, valida com pandas e faz o bulk
insert na tabela transacao via COPY. Ao final:
- se concluir com sucesso: posta um aviso positivo na API Node
- se falhar: posta um aviso negativo (trigger_rule='one_failed')
"""
import json
import os
import urllib3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET = 'treinamento'
COLUNAS_ESPERADAS = ['id', 'dt_transacao', 'tipo', 'valor']


def get_pathkey(kwargs):
    """Lê o pathkey das opções do trigger (dag_run.conf)."""
    conf = (kwargs['dag_run'].conf or {})
    pathkey = conf.get('pathkey', kwargs['params'].get('pathkey'))

    if not pathkey:
        raise ValueError('pathkey não informado nas opções do trigger (dag_run.conf)')

    return pathkey


def processa_bulk_insert(**kwargs):
    pathkey = get_pathkey(kwargs)
    print(f"processa_bulk_insert with pathkey={pathkey}")

    s3_hook = S3Hook(aws_conn_id='minio_s3')
    if not s3_hook.check_for_key(pathkey, bucket_name=BUCKET):
        raise FileNotFoundError(f'Arquivo s3://{BUCKET}/{pathkey} não encontrado')

    arquivo_local = f'/tmp/bulk_{os.path.basename(pathkey)}'
    s3_hook.get_key(pathkey, bucket_name=BUCKET).download_file(arquivo_local)
    print(f"Arquivo baixado em {arquivo_local} ({os.path.getsize(arquivo_local)} bytes)")

    # Processamento/validação antes da carga
    df = pd.read_csv(arquivo_local)
    if list(df.columns) != COLUNAS_ESPERADAS:
        raise ValueError(f'Colunas inválidas: {list(df.columns)}. Esperado: {COLUNAS_ESPERADAS}')
    if df[COLUNAS_ESPERADAS].isnull().any().any():
        raise ValueError('Arquivo contém valores nulos')
    print(f"Arquivo validado: {len(df)} transacoes, tipos={df['tipo'].unique().tolist()}")

    # Bulk insert via COPY (muito mais rápido que INSERT linha a linha)
    sql_copy = f"COPY public.transacao ({', '.join(COLUNAS_ESPERADAS)}) FROM STDIN WITH (FORMAT csv, HEADER true)"
    print(f"Executing SQL: {sql_copy}")
    hook = PostgresHook(postgres_conn_id='aurora_postgres_banco_master')
    hook.copy_expert(sql_copy, arquivo_local)
    print(f"Bulk insert concluido: {len(df)} transacoes inseridas")

    os.remove(arquivo_local)
    return len(df)


def post_status(**kwargs):
    sucesso = kwargs.get('sucesso')
    pathkey = get_pathkey(kwargs)

    if sucesso:
        qtd = kwargs['ti'].xcom_pull(task_ids='task_processa_bulk_insert')
        mensagem = f'Bulk insert concluido com sucesso: {qtd} transacoes do arquivo {pathkey}'
    else:
        mensagem = f'Falha no bulk insert do arquivo {pathkey}'

    body = json.dumps({'tipo': None, 'data': None, 'mensagem': mensagem})
    print(f"post_status sucesso={sucesso} body={body}")

    http = urllib3.PoolManager()
    response = http.request(
        'POST',
        'http://api-treinamento:3000/api/v1/aviso',
        body=body,
        headers={'Content-Type': 'application/json'},
        timeout=10.0,
    )
    response_body = response.data.decode('utf-8')
    print(f"API response status={response.status} body={response_body}")

    if response.status != 201:
        raise Exception(f"API returned status {response.status}: {response_body}")


default_args = {
    'owner': 'herculanocm.consult@srmasset.com',
    'on_failure_callback': None,  # callback function to be called on failure
    'on_success_callback': None,  # callback function to be called on success
    'retries': 0, # number of retries in case of failure
    'retry_delay': timedelta(minutes=5), # delay between retries
    'execution_timeout': timedelta(minutes=180), # maximum time allowed for the task to run
}

with DAG(
        dag_id='9_tasks_bulk_insert', # unique identifier for the DAG
        schedule_interval=None, # somente via trigger manual (com config)
        start_date=datetime(2024, 6, 1), # start date for the DAG (June 1, 2024)
        description='Bulk insert na transacao a partir de CSV no MinIO, com post positivo/negativo na API.', # description of the DAG
        default_args=default_args, # default arguments for the tasks in the DAG
        catchup=False, # whether to catch up on missed runs or not
        params={"pathkey": ""}, # default sobrescrito pelo dag_run.conf
        max_active_runs=1, # maximum number of active runs for the DAG
        concurrency=1, # maximum number of concurrent tasks for the DAG
        tags=['hello_world', 'training'], # tags for categorizing the DAG
) as dag:

    task_start = EmptyOperator(
        task_id='task_start', # unique identifier for the task
        dag=dag # reference to the DAG that the task belongs to
    )

    task_processa_bulk_insert = PythonOperator(
        task_id='task_processa_bulk_insert', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=processa_bulk_insert, # callable function to be executed by the task
        dag=dag # reference to the DAG that the task belongs to
    )

    task_post_positivo = PythonOperator(
        task_id='task_post_positivo', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=post_status, # callable function to be executed by the task
        op_kwargs={'sucesso': True}, # optional keyword arguments to pass to the callable function
        dag=dag # reference to the DAG that the task belongs to (roda no sucesso: all_success default)
    )

    task_post_negativo = PythonOperator(
        task_id='task_post_negativo', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=post_status, # callable function to be executed by the task
        op_kwargs={'sucesso': False}, # optional keyword arguments to pass to the callable function
        trigger_rule='one_failed', # roda somente se o bulk insert falhar
        dag=dag # reference to the DAG that the task belongs to
    )

    task_end = EmptyOperator(
        task_id='task_end', # unique identifier for the task
        trigger_rule='one_success', # basta um dos posts concluir (o outro nunca roda)
        dag=dag # reference to the DAG that the task belongs to
    )

task_start >> task_processa_bulk_insert >> [task_post_positivo, task_post_negativo] >> task_end
