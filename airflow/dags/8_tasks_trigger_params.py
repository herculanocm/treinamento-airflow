"""DAG de exemplo com parâmetros via Trigger DAG w/ config (dag_run.conf).

Trigger manual passando o tipo nas opções da run:
    {"tipo": "pagamento"}

A data de referência é o ts da própria execução (data lógica da DAG run).
A primeira task (branch) verifica se existem transações para o tipo/data:
- se NÃO existir: posta um aviso na API Node
- se existir: gera um CSV analítico (todas as transações) e envia para o
  MinIO como se fosse S3 (bucket "treinamento")
"""
import json
import os
import urllib3
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

TIPOS_VALIDOS = ['pagamento', 'transferencia', 'taxa']
BUCKET = 'treinamento'


def get_parametros(kwargs):
    """Lê o tipo das opções do trigger (dag_run.conf); a data vem do ts da execução."""
    conf = (kwargs['dag_run'].conf or {})
    tipo = conf.get('tipo', kwargs['params'].get('tipo'))
    data_referencia = datetime.strptime(kwargs['ts'][:19], '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d')

    if tipo not in TIPOS_VALIDOS:
        raise ValueError(f"tipo inválido: {tipo}. Use um de: {TIPOS_VALIDOS}")

    return tipo, data_referencia


def branch_verifica_dados(**kwargs):
    tipo, data_referencia = get_parametros(kwargs)
    print(f"branch_verifica_dados with tipo={tipo}, data={data_referencia}, conf={kwargs['dag_run'].conf}")

    sql_query = f"""
    select count(1) as qtd
    from public.transacao t
    where
    t.tipo = '{tipo}' and date_trunc('day', t.dt_transacao) = '{data_referencia}'
    """
    print(f"Executing SQL query: {sql_query}")
    hook = PostgresHook(postgres_conn_id='aurora_postgres_banco_master')
    results = hook.get_first(sql_query)
    qtd = results[0] if results else 0

    proxima_task = 'task_gera_csv_minio' if qtd > 0 else 'task_post_aviso'
    print(f"branch_verifica_dados tipo={tipo} data={data_referencia} qtd={qtd} -> {proxima_task}")
    return proxima_task


def post_aviso(**kwargs):
    tipo, data_referencia = get_parametros(kwargs)

    body = json.dumps({
        'tipo': tipo,
        'data': data_referencia,
        'mensagem': f'Sem transacoes para tipo={tipo} na data={data_referencia}',
    })
    print(f"post_aviso body={body}")

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


def gera_csv_minio(**kwargs):
    tipo, data_referencia = get_parametros(kwargs)

    # Extração analítica: todas as transações do tipo/data, linha a linha
    sql_query = f"""
    select id, dt_transacao, tipo, valor
    from public.transacao t
    where
    t.tipo = '{tipo}' and date_trunc('day', t.dt_transacao) = '{data_referencia}'
    order by t.dt_transacao
    """
    print(f"Executing SQL query: {sql_query}")
    hook = PostgresHook(postgres_conn_id='aurora_postgres_banco_master')
    df = hook.get_pandas_df(sql_query)
    print(f"Total de transacoes extraidas: {len(df)}")

    arquivo_local = f'/tmp/transacoes_{tipo}_{data_referencia}.csv'
    df.to_csv(arquivo_local, index=False)
    print(f"CSV gerado em {arquivo_local} ({os.path.getsize(arquivo_local)} bytes)")

    # Envia para o MinIO como se fosse S3 (conexão minio_s3 -> http://minio:9000)
    s3_hook = S3Hook(aws_conn_id='minio_s3')
    if not s3_hook.check_for_bucket(BUCKET):
        print(f"Bucket {BUCKET} nao existe, criando...")
        s3_hook.create_bucket(bucket_name=BUCKET)

    chave = f'transacoes/{data_referencia}/transacoes_{tipo}_{data_referencia}.csv'
    s3_hook.load_file(filename=arquivo_local, key=chave, bucket_name=BUCKET, replace=True)
    print(f"Arquivo enviado para s3://{BUCKET}/{chave}")

    os.remove(arquivo_local)


default_args = {
    'owner': 'herculanocm.consult@srmasset.com',
    'on_failure_callback': None,  # callback function to be called on failure
    'on_success_callback': None,  # callback function to be called on success
    'retries': 0, # number of retries in case of failure
    'retry_delay': timedelta(minutes=5), # delay between retries
    'execution_timeout': timedelta(minutes=180), # maximum time allowed for the task to run
}

with DAG(
        dag_id='8_tasks_trigger_params', # unique identifier for the DAG
        schedule_interval=None, # somente via trigger manual (com config)
        start_date=datetime(2024, 6, 1), # start date for the DAG (June 1, 2024)
        description='Pipe com parametros via Trigger DAG w/ config: branch -> aviso na API ou CSV no MinIO.', # description of the DAG
        default_args=default_args, # default arguments for the tasks in the DAG
        catchup=False, # whether to catch up on missed runs or not
        params={"tipo": "pagamento"}, # default sobrescrito pelo dag_run.conf
        max_active_runs=1, # maximum number of active runs for the DAG
        concurrency=1, # maximum number of concurrent tasks for the DAG
        tags=['hello_world', 'training'], # tags for categorizing the DAG
) as dag:

    task_start = EmptyOperator(
        task_id='task_start', # unique identifier for the task
        dag=dag # reference to the DAG that the task belongs to
    )

    task_branch_verifica_dados = BranchPythonOperator(
        task_id='task_branch_verifica_dados', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=branch_verifica_dados, # callable function that returns the task_id to follow
        dag=dag # reference to the DAG that the task belongs to
    )

    task_post_aviso = PythonOperator(
        task_id='task_post_aviso', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=post_aviso, # callable function to be executed by the task
        dag=dag # reference to the DAG that the task belongs to
    )

    task_gera_csv_minio = PythonOperator(
        task_id='task_gera_csv_minio', # unique identifier for the task
        provide_context=True, # whether to provide context variables to the callable function
        python_callable=gera_csv_minio, # callable function to be executed by the task
        dag=dag # reference to the DAG that the task belongs to
    )

    task_end = EmptyOperator(
        task_id='task_end', # unique identifier for the task
        trigger_rule='none_failed', # um dos ramos do branch é sempre pulado
        dag=dag # reference to the DAG that the task belongs to
    )

task_start >> task_branch_verifica_dados >> [task_post_aviso, task_gera_csv_minio] >> task_end
