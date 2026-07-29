// API do treinamento: recebe via POST os totais por tipo calculados pelas
// DAGs do Airflow (ex.: 7_tasks_aurora_api) e guarda em memória para
// consulta. Não use em produção — os dados são perdidos ao reiniciar.
const express = require('express');

const app = express();
const PORT = process.env.PORT || 3000;

// Mesmas categorias do CHECK da tabela transacao (postgres-init/init-banco.sql)
const TIPOS_VALIDOS = ['pagamento', 'transferencia', 'taxa'];

// Armazenamento em memória dos totais, avisos e falhas recebidos
const totais = [];
const avisos = [];
const falhas = [];

app.use(express.json());

// Loga cada requisição para facilitar o acompanhamento nos logs do container
app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} ${req.method} ${req.originalUrl}`);
  next();
});

// Healthcheck usado pelo docker compose
app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

// Recebe um total por tipo: { "tipo": "pagamento", "valor": 123.45, "data": "2025-06-15" }
app.post('/api/v1/total-tipo', (req, res) => {
  const { tipo, valor, data } = req.body || {};

  const erros = [];
  if (!TIPOS_VALIDOS.includes(tipo)) {
    erros.push(`campo "tipo" é obrigatório e deve ser um de: ${TIPOS_VALIDOS.join(', ')}`);
  }
  const valorNumerico = Number(valor);
  if (valor === undefined || valor === null || Number.isNaN(valorNumerico)) {
    erros.push('campo "valor" é obrigatório e deve ser numérico');
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(data))) {
    erros.push('campo "data" é obrigatório no formato YYYY-MM-DD');
  }

  if (erros.length > 0) {
    return res.status(400).json({ erros });
  }

  const registro = {
    id: totais.length + 1,
    tipo,
    valor: valorNumerico,
    data,
    recebidoEm: new Date().toISOString(),
  };
  totais.push(registro);

  console.log(`Total recebido: tipo=${registro.tipo} valor=${registro.valor} data=${registro.data}`);
  return res.status(201).json(registro);
});

// Lista os totais recebidos, com filtro opcional: /api/v1/total-tipo?tipo=taxa
app.get('/api/v1/total-tipo', (req, res) => {
  const { tipo } = req.query;
  const resultado = tipo ? totais.filter((t) => t.tipo === tipo) : totais;
  res.json({ quantidade: resultado.length, totais: resultado });
});

// Recebe avisos das DAGs (ex.: dia sem transações para um tipo):
// { "tipo": "pagamento", "data": "2025-06-15", "mensagem": "..." }
app.post('/api/v1/aviso', (req, res) => {
  const { tipo, data, mensagem } = req.body || {};

  if (!mensagem) {
    return res.status(400).json({ erros: ['campo "mensagem" é obrigatório'] });
  }

  const registro = {
    id: avisos.length + 1,
    tipo: tipo ?? null,
    data: data ?? null,
    mensagem,
    recebidoEm: new Date().toISOString(),
  };
  avisos.push(registro);

  console.log(`Aviso recebido: tipo=${registro.tipo} data=${registro.data} mensagem=${registro.mensagem}`);
  return res.status(201).json(registro);
});

// Lista os avisos recebidos
app.get('/api/v1/aviso', (req, res) => {
  res.json({ quantidade: avisos.length, avisos });
});

// Recebe falhas de tasks das DAGs (on_failure_callback):
// { "dag_id": "...", "task_id": "...", "run_id": "...", "logical_date": "...", "erro": "..." }
app.post('/api/v1/falha', (req, res) => {
  const { dag_id: dagId, task_id: taskId, run_id: runId, logical_date: logicalDate, erro } = req.body || {};

  if (!dagId || !taskId) {
    return res.status(400).json({ erros: ['campos "dag_id" e "task_id" são obrigatórios'] });
  }

  const registro = {
    id: falhas.length + 1,
    dagId,
    taskId,
    runId: runId ?? null,
    logicalDate: logicalDate ?? null,
    erro: erro ?? null,
    recebidoEm: new Date().toISOString(),
  };
  falhas.push(registro);

  console.error(`FALHA recebida: dag=${registro.dagId} task=${registro.taskId} run=${registro.runId} erro=${registro.erro}`);
  return res.status(201).json(registro);
});

// Lista as falhas recebidas
app.get('/api/v1/falha', (req, res) => {
  res.json({ quantidade: falhas.length, falhas });
});

app.listen(PORT, () => {
  console.log(`api-treinamento escutando na porta ${PORT}`);
});
