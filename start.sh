#!/usr/bin/env bash
# Inicia todo o ambiente do treinamento
set -e
cd "$(dirname "$0")"

echo "=== Subindo o ambiente do treinamento..."
echo "=== (a primeira subida pode levar alguns minutos: download das imagens + airflow-init)"
docker compose up -d

echo "=== Aguardando o Postgres ficar pronto..."
for i in $(seq 1 60); do
    if docker compose exec -T postgres pg_isready -U postgres -p 5433 -q 2>/dev/null; then
        break
    fi
    sleep 2
done

# Garante o banco de exemplo: se o database "banco" ou a tabela "transacao"
# não existirem, o script idempotente cria e popula (~200 mil registros)
tabela_ok=$(docker compose exec -T postgres psql -U postgres -p 5433 -d banco -tAc \
    "SELECT to_regclass('public.transacao') IS NOT NULL" 2>/dev/null || echo f)
if [[ "$tabela_ok" == "t" ]]; then
    echo "=== Banco de exemplo OK (database 'banco' e tabela 'transacao' ja existem)"
else
    echo "=== Criando database 'banco' e tabela 'transacao' (~200 mil registros)..."
    docker compose exec -T postgres psql -U postgres -p 5433 -v ON_ERROR_STOP=1 \
        -f /docker-entrypoint-initdb.d/init-banco.sql
fi

echo
echo "=== Ambiente no ar!"
echo "    Airflow UI:    http://localhost:8080  (airflow / airflow)"
echo "    MinIO Console: http://localhost:9001  (admin / password)"
echo "    Postgres:      localhost:5433        (postgres / postgres)"
