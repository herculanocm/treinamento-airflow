-- Script de configuração do banco de exemplo do treinamento.
-- Executado automaticamente pelo Postgres na PRIMEIRA subida do volume
-- (via /docker-entrypoint-initdb.d) e reexecutado pelo start.sh/start.cmd
-- a cada subida. É idempotente: só cria/popula o que ainda não existe.

SELECT 'CREATE DATABASE banco'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'banco')\gexec

\connect banco

CREATE TABLE IF NOT EXISTS transacao (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- id aleatório
    dt_transacao TIMESTAMP NOT NULL,                         -- data/hora da transação
    tipo         TEXT NOT NULL                               -- categoria da transação
                 CHECK (tipo IN ('pagamento', 'transferencia', 'taxa')),
    valor        NUMERIC(12,2) NOT NULL                      -- valor da transação
);

-- ~200 mil registros aleatórios distribuídos ao longo de todo o ano de 2025,
-- com valores entre 0.01 e 10000.00 e tipo sorteado uniformemente entre as
-- 3 categorias. Só insere se a tabela estiver vazia.
INSERT INTO transacao (dt_transacao, tipo, valor)
SELECT
    timestamp '2025-01-01' + random() * (timestamp '2026-01-01' - timestamp '2025-01-01'),
    (ARRAY['pagamento', 'transferencia', 'taxa'])[1 + floor(random() * 3)::int],
    round((random() * 9999.99 + 0.01)::numeric, 2)
FROM generate_series(1, (195000 + floor(random() * 10001))::int)
WHERE NOT EXISTS (SELECT 1 FROM transacao);

CREATE INDEX IF NOT EXISTS idx_transacao_dt ON transacao (dt_transacao);

-- Status de processamento por data de referência e tipo, alimentada pela DAG
-- 7_tasks_aurora_api: guarda o valor apurado e se o envio para a API concluiu
-- com sucesso (true) ou falhou (false). Uma linha por (dt_referencia, tipo).
CREATE TABLE IF NOT EXISTS transacao_parceiro_status (
    dt_referencia DATE NOT NULL,                    -- data de referência do total
    tipo          TEXT NOT NULL                     -- categoria da transação
                  CHECK (tipo IN ('pagamento', 'transferencia', 'taxa')),
    valor         NUMERIC(12,2),                    -- total apurado (null se não houve)
    status        BOOLEAN NOT NULL,                 -- true = envio ok, false = falha
    atualizado_em TIMESTAMP NOT NULL DEFAULT now(), -- última atualização da linha
    PRIMARY KEY (dt_referencia, tipo)
);
