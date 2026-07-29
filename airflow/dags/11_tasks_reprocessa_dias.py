"""DAG de exemplo de reprocessamento: dispara a DAG 8 para os últimos 5 dias.

Cada task usa o TriggerDagRunOperator para criar uma run da DAG 8 com o
logical_date deslocado (ts - 1 dia até ts - 5 dias). Como a DAG 8 usa o ts da
própria run como data de referência, cada chamada reprocessa um dia.

O tipo pode ser passado nas opções do trigger ({"tipo": "taxa"}), com default
nos params. As chamadas são sequenciais e cada uma espera a run da DAG 8
terminar (wait_for_completion=True); reset_dag_run=True permite reprocessar
um dia que já tem run (a run existente é limpa e re-executada).
"""
import json
import urllib3
from airflow import DAG
from airflow.operators.dummy import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


def post_falha_callback(context):
    """on_failure_callback: recebe o context da task que falhou e notifica a
    API Node no endpoint /api/v1/falha. Callbacks não afetam o estado da task,
    então aqui só logamos a resposta, sem raise."""
    ti = context['task_instance']
    body = json.dumps({
        'dag_id': ti.dag_id,
        'task_id': ti.task_id,
        'run_id': context.get('run_id'),
        'logical_date': str(context.get('logical_date')),
        'erro': str(context.get('exception')),
    })
    print(f"post_falha_callback body={body}")

    http = urllib3.PoolManager()
    response = http.request(
        'POST',
        'http://api-treinamento:3000/api/v1/falha',
        body=body,
        headers={'Content-Type': 'application/json'},
        timeout=10.0,
    )
    print(f"API response status={response.status} body={response.data.decode('utf-8')}")


default_args = {
    'owner': 'herculanocm.consult@srmasset.com',
    'on_failure_callback': None,  # callback function to be called on failure
    'on_success_callback': None,  # callback function to be called on success
    'retries': 0, # number of retries in case of failure
    'retry_delay': timedelta(minutes=5), # delay between retries
    'execution_timeout': timedelta(minutes=180), # maximum time allowed for the task to run
}

with DAG(
        dag_id='11_tasks_reprocessa_dias', # unique identifier for the DAG
        schedule_interval=None, # somente via trigger manual
        start_date=datetime(2024, 6, 1), # start date for the DAG (June 1, 2024)
        description='Reprocessa os ultimos 5 dias disparando a DAG 8 com logical_date = ts - N dias.', # description of the DAG
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

    task_end = EmptyOperator(
        task_id='task_end', # unique identifier for the task
        dag=dag # reference to the DAG that the task belongs to
    )

    # Cria uma task de trigger por dia (ts - 1 até ts - 5), encadeadas em
    # sequência: cada uma só dispara depois que a run do dia anterior termina
    task_anterior = task_start
    for dias in range(1, 6):
        task_trigger = TriggerDagRunOperator(
            task_id=f'task_trigger_dag8_menos_{dias}d', # unique identifier for the task
            trigger_dag_id='8_tasks_trigger_params', # DAG alvo que será disparada
            logical_date=f"{{{{ macros.ds_add(ds, -{dias}) }}}}", # ts da run alvo = ts desta run - N dias
            conf={'tipo': "{{ dag_run.conf.get('tipo', params.tipo) }}"}, # repassa o tipo para a DAG 8
            reset_dag_run=True, # se já existe run nesse logical_date, limpa e reprocessa
            wait_for_completion=True, # espera a run da DAG 8 terminar antes de seguir
            poke_interval=10, # intervalo (s) entre checagens do estado da run disparada
            #on_failure_callback=post_falha_callback, # notifica a API Node se a task falhar
            dag=dag # reference to the DAG that the task belongs to
        )
        task_anterior >> task_trigger
        task_anterior = task_trigger

task_anterior >> task_end
