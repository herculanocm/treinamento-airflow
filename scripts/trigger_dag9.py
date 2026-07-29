#!/usr/bin/env python3
"""Aciona remotamente a DAG 9 (9_tasks_bulk_insert) via REST API do Airflow.

Cria uma DAG run passando o pathkey no conf, igual ao Trigger DAG w/ config
da UI. A autenticação é basic auth (padrão do treinamento: airflow/airflow).

Uso:
    python scripts/trigger_dag9.py entrada/transacoes_novas.csv
    python scripts/trigger_dag9.py entrada/arquivo.csv --host http://localhost:8080

Obs.: a DAG 9 precisa estar despausada na UI para a run criada executar.
"""
import argparse
import json
import sys
import urllib3

DAG_ID = '9_tasks_bulk_insert'


def main():
    parser = argparse.ArgumentParser(description=f'Dispara a DAG {DAG_ID} passando o pathkey no conf')
    parser.add_argument('pathkey', help='Chave do arquivo no bucket treinamento (ex.: entrada/transacoes_novas.csv)')
    parser.add_argument('--host', default='http://localhost:8080', help='URL base do Airflow (default: %(default)s)')
    parser.add_argument('--usuario', default='airflow', help='Usuário da API (default: %(default)s)')
    parser.add_argument('--senha', default='airflow', help='Senha da API (default: %(default)s)')
    args = parser.parse_args()

    url = f'{args.host}/api/v1/dags/{DAG_ID}/dagRuns'
    body = json.dumps({'conf': {'pathkey': args.pathkey}})

    # Basic auth + content-type nos headers
    headers = urllib3.util.make_headers(basic_auth=f'{args.usuario}:{args.senha}')
    headers['Content-Type'] = 'application/json'

    print(f'POST {url} body={body}')
    http = urllib3.PoolManager()
    response = http.request('POST', url, body=body, headers=headers, timeout=10.0)
    payload = json.loads(response.data.decode('utf-8'))
    print(json.dumps(payload, indent=2, ensure_ascii=False))

    if response.status != 200:
        print(f'ERRO: API retornou status {response.status}', file=sys.stderr)
        sys.exit(1)

    print(f"Run criada com sucesso: run_id={payload['dag_run_id']} state={payload['state']}")


if __name__ == '__main__':
    main()
