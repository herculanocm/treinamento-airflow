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

# Garante o banco de exemplo reaplicando o script idempotente: cria o database
# "banco" e as tabelas que faltarem; só popula a transacao se estiver vazia
echo "=== Garantindo o banco de exemplo (script idempotente)..."
docker compose exec -T postgres psql -U postgres -p 5433 -v ON_ERROR_STOP=1 \
    -f /docker-entrypoint-initdb.d/init-banco.sql

echo
echo "=== Ambiente no ar!"
echo "    Airflow UI:    http://localhost:8080  (airflow / airflow)"
echo "    MinIO Console: http://localhost:9001  (admin / password)"
echo "    Postgres:      localhost:5433        (postgres / postgres)"
