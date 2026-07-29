@echo off
rem Inicia todo o ambiente do treinamento
cd /d "%~dp0"

echo === Subindo o ambiente do treinamento...
echo === (a primeira subida pode levar alguns minutos: download das imagens + airflow-init)
docker compose up -d
if errorlevel 1 (
    echo === ERRO ao subir o ambiente. Verifique se o Docker Desktop esta rodando.
    exit /b 1
)

echo === Aguardando o Postgres ficar pronto...
set tentativas=0
:wait_pg
docker compose exec -T postgres pg_isready -U postgres -p 5433 -q >nul 2>&1
if errorlevel 1 (
    set /a tentativas+=1
    if %tentativas% geq 60 goto pg_timeout
    ping -n 3 127.0.0.1 >nul
    goto wait_pg
)

rem Garante o banco de exemplo: se o database "banco" ou a tabela "transacao"
rem nao existirem, o script idempotente cria e popula (~200 mil registros)
set tabela_ok=f
for /f %%i in ('docker compose exec -T postgres psql -U postgres -p 5433 -d banco -tAc "SELECT to_regclass('public.transacao') IS NOT NULL" 2^>nul') do set tabela_ok=%%i
if "%tabela_ok%"=="t" (
    echo === Banco de exemplo OK ^(database 'banco' e tabela 'transacao' ja existem^)
) else (
    echo === Criando database 'banco' e tabela 'transacao' ^(~200 mil registros^)...
    docker compose exec -T postgres psql -U postgres -p 5433 -v ON_ERROR_STOP=1 -f /docker-entrypoint-initdb.d/init-banco.sql
)
goto fim

:pg_timeout
echo === ERRO: Postgres nao respondeu a tempo. Verifique: docker compose logs postgres
exit /b 1

:fim
echo.
echo === Ambiente no ar!
echo     Airflow UI:    http://localhost:8080  (airflow / airflow)
echo     MinIO Console: http://localhost:9001  (admin / password)
echo     Postgres:      localhost:5433        (postgres / postgres)
