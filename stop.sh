#!/usr/bin/env bash
# Força a parada de todo o ambiente do treinamento
set -e
cd "$(dirname "$0")"

echo "=== Parando o ambiente do treinamento..."
# -t 5: espera até 5s pelo shutdown gracioso e depois força (kill)
docker compose down --remove-orphans -t 5

echo
echo "=== Ambiente parado. Os dados foram preservados."
echo "    Para apagar tambem os dados (reset completo): docker compose down -v"
