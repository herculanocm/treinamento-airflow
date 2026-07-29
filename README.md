# Treinamento Airflow

Ambiente local de treinamento do Apache Airflow **2.10.5** rodando em WSL + Docker,
com CeleryExecutor (Postgres + Redis) e MinIO como storage compatível com S3.

## Estrutura

```
.
├── docker-compose.yaml
├── start.sh              # inicia todo o ambiente (Linux-WSL)
├── stop.sh               # força a parada de todo o ambiente (Linux-WSL)
├── .env                  # AIRFLOW_UID e imagem do Airflow
├── postgres-init/        # scripts SQL do banco de exemplo (idempotente)
├── api/                  # API Node/Express dos exercícios (totais, avisos e falhas)
├── scripts/              # scripts auxiliares (ex.: trigger remoto de DAGs via REST API)
├── venv/                 # ambiente Python local (opcional, ver seção abaixo)
└── airflow/
    ├── dags/             # coloque suas DAGs aqui
    ├── logs/
    ├── config/
    └── plugins/
```

## Subindo o ambiente

```bash
./start.sh
```

Na primeira subida o serviço `airflow-init` roda automaticamente (migração do
banco + criação do usuário admin) antes dos demais serviços. Além de subir os
containers, o start aguarda o Postgres ficar pronto e **reaplica o script
idempotente do banco de exemplo**: cria o database `banco` e as tabelas que
faltarem, e só popula a `transacao` se estiver vazia.

Também funciona subir diretamente com `docker compose up -d` — nesse caso o
banco de exemplo é criado apenas na primeira inicialização do volume (via
`docker-entrypoint-initdb.d`).

## Parando o ambiente

```bash
./stop.sh
```

O script força a parada de todos os containers (timeout de 5s), preservando
os dados nos volumes.

```bash
# Acompanhar os logs
docker compose logs -f
```

## Acessos

| Serviço          | URL                    | Usuário    | Senha      |
|------------------|------------------------|------------|------------|
| Airflow UI       | http://localhost:8080  | `airflow`  | `airflow`  |
| MinIO Console    | http://localhost:9001  | `admin`    | `password` |
| MinIO API (S3)   | http://localhost:9000  | `admin`    | `password` |
| Postgres         | localhost:5433 (db `airflow`) | `postgres` | `postgres` |
| API treinamento  | http://localhost:3000  | —          | —          |

O Postgres escuta na **5433** (dentro e fora do compose) para não conflitar com
um Postgres local que você já tenha na 5432.

## Banco de dados de exemplo

Na primeira subida do volume, o script [postgres-init/init-banco.sql](postgres-init/init-banco.sql)
cria automaticamente o database **`banco`** com a tabela **`transacao`**:

| Coluna         | Tipo          | Descrição                                                  |
|----------------|---------------|------------------------------------------------------------|
| `id`           | UUID          | id aleatório (chave primária)                              |
| `dt_transacao` | TIMESTAMP     | data/hora ao longo de todo o 2025                          |
| `tipo`         | TEXT          | sorteado entre `pagamento`, `transferencia` e `taxa`       |
| `valor`        | NUMERIC(12,2) | valor entre 0.01 e 10000.00                                |

São gerados **~200 mil registros aleatórios** distribuídos por todo o ano de
2025. Dentro do compose, acesse em `postgres:5433`; da máquina host, em
`localhost:5433` (`postgres`/`postgres`).

O script também cria a tabela **`transacao_parceiro_status`** (PK composta
`dt_referencia` + `tipo`), alimentada pela DAG 7: guarda o valor apurado no
dia e se o envio para a API concluiu com sucesso (`status` true/false).

## DAGs do treinamento

Todas as DAGs estão com `schedule_interval=None` — nada roda sozinho. Despause
a DAG na UI e dispare manualmente (Trigger DAG), pela CLI ou pela REST API.

| DAG                                   | O que demonstra                                                             |
|---------------------------------------|------------------------------------------------------------------------------|
| `1_hello_airflow` … `6_tasks_aurora1` | Básicos: hello world, encadeamento e reuso de tasks, paralelismo, Postgres   |
| `7_tasks_aurora_api`                  | Totais por tipo → POST na API + upsert de status com `BranchPythonOperator`  |
| `8_tasks_trigger_params`              | Parâmetros via Trigger w/ config: branch → aviso na API ou CSV no MinIO      |
| `9_tasks_bulk_insert`                 | Bulk insert (COPY) de CSV do MinIO + post positivo/negativo via `trigger_rule` |
| `10_tasks_trigger_dag`                | `TriggerDagRunOperator`: dispara a DAG 8 uma vez por tipo                    |
| `11_tasks_reprocessa_dias`            | Reprocessa os últimos 5 dias (logical_date deslocado) + `on_failure_callback` |

## Conexões do Airflow

As DAGs 6 a 11 dependem de duas conexões, criadas via CLI. Elas ficam no banco
de metadados do Airflow — um `docker compose down -v` as apaga; recrie com:

```bash
# Postgres do banco de exemplo
docker compose exec airflow-webserver airflow connections add aurora_postgres_banco_master \
  --conn-type postgres --conn-host postgres --conn-port 5433 \
  --conn-schema banco --conn-login postgres --conn-password postgres

# MinIO como S3
docker compose exec airflow-webserver airflow connections add minio_s3 \
  --conn-type aws --conn-login admin --conn-password password \
  --conn-extra '{"endpoint_url": "http://minio:9000", "region_name": "us-east-1"}'
```

## API do treinamento (Node/Express)

O serviço `api-treinamento` ([api/](api/)) é uma API Node.js/Express usada nos
exercícios: recebe das DAGs os totais por `tipo` da tabela `transacao`, avisos
(ex.: dia sem transações) e notificações de falha de tasks
(`on_failure_callback`). Os dados ficam **em memória** (são perdidos ao
reiniciar o container).

Dentro do compose as DAGs acessam em `http://api-treinamento:3000`; da máquina
host, em `http://localhost:3000`.

| Método | Rota                 | Descrição                                        |
|--------|----------------------|--------------------------------------------------|
| POST   | `/api/v1/total-tipo` | Recebe `{ "tipo": "...", "valor": 123.45, "data": "2025-06-15" }` |
| GET    | `/api/v1/total-tipo` | Lista os totais recebidos (filtro `?tipo=taxa`)  |
| POST   | `/api/v1/aviso`      | Recebe avisos das DAGs: `{ "tipo": "...", "data": "...", "mensagem": "..." }` |
| GET    | `/api/v1/aviso`      | Lista os avisos recebidos                        |
| POST   | `/api/v1/falha`      | Recebe falhas de tasks (`on_failure_callback`): `{ "dag_id": "...", "task_id": "...", "erro": "..." }` |
| GET    | `/api/v1/falha`      | Lista as falhas recebidas                        |
| GET    | `/health`            | Healthcheck usado pelo compose                   |

O campo `tipo` aceita as mesmas categorias da tabela: `pagamento`,
`transferencia` ou `taxa`. Exemplo:

```bash
curl -X POST http://localhost:3000/api/v1/total-tipo \
  -H 'Content-Type: application/json' \
  -d '{"tipo": "pagamento", "valor": 1234.56, "data": "2025-06-15"}'
```

## Acionando DAGs remotamente

O script [scripts/trigger_dag9.py](scripts/trigger_dag9.py) dispara a DAG 9
pela REST API do Airflow (basic auth `airflow`/`airflow`), passando o
`pathkey` no conf — o mesmo endpoint que o botão Trigger da UI usa:

```bash
venv/bin/python scripts/trigger_dag9.py entrada/transacoes_novas.csv
```

## Ambiente de desenvolvimento (Python local)

As DAGs **executam** dentro dos containers, mas para desenvolver com conforto —
autocomplete, checagem de imports e linting na IDE — vale instalar o Airflow e
os providers num ambiente virtual Python local, com as mesmas versões dos
containers.

### 1. Instalar o Python 3.12

```bash
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install python3.12 python3.12-venv python3.12-dev
```

- `add-apt-repository ppa:deadsnakes/ppa` — adiciona o repositório *deadsnakes*,
  um PPA da comunidade que empacota versões novas do Python para Ubuntu (que só
  traz uma versão padrão de `python3` por release).
- `apt-get update` — recarrega o índice de pacotes, agora enxergando o PPA
  recém-adicionado.
- `apt-get install` — instala o interpretador (`python3.12`), o módulo de
  criação de ambientes virtuais (`python3.12-venv`) e os headers de compilação
  (`python3.12-dev`), necessários para dependências do Airflow que compilam
  código nativo durante o `pip install`.

> Nota: `python3.12-distutils` não existe — o módulo `distutils` foi removido
> do Python 3.12; os pacotes atuais usam `setuptools` no lugar.

### 2. Criar e ativar o ambiente virtual

```bash
python3.12 -m venv ./venv
source ./venv/bin/activate
```

- `python3.12 -m venv ./venv` — cria um ambiente virtual na pasta `venv/`: uma
  instalação Python isolada do sistema, onde as dependências do projeto não
  conflitam com as de outros projetos.
- `source ./venv/bin/activate` — ativa o ambiente no shell atual: `python` e
  `pip` passam a apontar para o venv. Para sair, use `deactivate`. (A pasta
  `venv/` já está no `.gitignore` do projeto.)

### 3. Instalar o Airflow e os providers

```bash
pip install "apache-airflow==2.10.5" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"

pip install apache-airflow-providers-amazon --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"
pip install apache-airflow-providers-postgres --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"
```

O Airflow tem centenas de dependências transitivas, e o pip sozinho pode
resolver combinações de versões nunca testadas juntas. O `--constraint` aponta
para o **arquivo de constraints oficial** do Airflow, que congela a versão
exata de cada dependência testada para aquela release — o padrão da URL é
`constraints-<versão do Airflow>/constraints-<versão do Python>.txt`. Por isso
todos os comandos usam `constraints-2.10.5` + `constraints-3.12.txt`: as mesmas
versões do Airflow e do Python que rodam nos containers.

Os providers são pacotes de integração que adicionam hooks, operators e sensors:

| Provider   | O que adiciona                                                        |
|------------|-----------------------------------------------------------------------|
| `amazon`   | Integração AWS (S3, etc.) — é com ele que as DAGs falam com o MinIO   |
| `postgres` | `PostgresHook`/operators SQL — consultas no Postgres (ex.: `transacao`) |

Na IDE, selecione `./venv/bin/python` como interpretador do projeto para o
autocomplete passar a enxergar esses pacotes.

## Comandos úteis

```bash
# Parar tudo (mantém os dados)
docker compose down

# Parar tudo e apagar volumes (reset completo: banco, MinIO e metadados do
# Airflow — inclusive as conexões, que precisam ser recriadas; ver seção acima)
docker compose down -v

# CLI do Airflow (profile debug)
docker compose run --rm airflow-cli airflow dags list

# Flower (monitoramento do Celery) em http://localhost:5555
docker compose --profile flower up -d
```

## Dependências extras

Para testes rápidos, adicione pacotes pip no `.env`:

```bash
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-amazon
```

Isso instala os pacotes a **cada** start dos containers. Para algo permanente,
estenda a imagem oficial com um `Dockerfile` (descomente a linha `build: .` no
`docker-compose.yaml`).

