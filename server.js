import express from 'express';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = process.env.PORT || 3000;

const QUERY_SERVICE_URL = process.env.QUERY_SERVICE_URL || 'https://query.keboola.com';
const KBC_TOKEN = process.env.KBC_TOKEN || '';
const WORKSPACE_ID = process.env.WORKSPACE_ID || '';
const BRANCH_ID = process.env.BRANCH_ID || 'default';
const SQL = process.env.KBC_QUERY || 'SELECT * FROM "in.c-andy-test"."sample_orders"';

const headers = {
  'X-StorageAPI-Token': KBC_TOKEN,
  'Content-Type': 'application/json',
};

async function executeSQL(sql) {
  const submitRes = await fetch(
    `${QUERY_SERVICE_URL}/api/v1/branches/${BRANCH_ID}/workspaces/${WORKSPACE_ID}/queries`,
    { method: 'POST', headers, body: JSON.stringify({ statements: [sql], transactional: false }) }
  );
  if (!submitRes.ok) throw new Error(`Submit failed: ${submitRes.status} ${await submitRes.text()}`);
  const { queryJobId } = await submitRes.json();

  for (let i = 0; i < 60; i++) {
    const statusRes = await fetch(`${QUERY_SERVICE_URL}/api/v1/queries/${queryJobId}`, { headers });
    if (!statusRes.ok) throw new Error(`Status failed: ${statusRes.status}`);
    const job = await statusRes.json();
    const state = (job.state || job.status || '').toLowerCase();
    if (['success', 'completed', 'finished'].includes(state)) return;
    if (['error', 'failed', 'cancelled'].includes(state)) throw new Error(`Query ${state}: ${JSON.stringify(job)}`);
    await new Promise(r => setTimeout(r, 1000));
  }
  throw new Error('Query timed out');
}

async function runQuery(sql) {
  // Submit query job
  const submitRes = await fetch(
    `${QUERY_SERVICE_URL}/api/v1/branches/${BRANCH_ID}/workspaces/${WORKSPACE_ID}/queries`,
    { method: 'POST', headers, body: JSON.stringify({ statements: [sql], transactional: false }) }
  );
  if (!submitRes.ok) throw new Error(`Submit failed: ${submitRes.status} ${await submitRes.text()}`);
  const { queryJobId } = await submitRes.json();

  // Poll for completion
  let job;
  for (let i = 0; i < 60; i++) {
    const statusRes = await fetch(`${QUERY_SERVICE_URL}/api/v1/queries/${queryJobId}`, { headers });
    if (!statusRes.ok) throw new Error(`Status failed: ${statusRes.status}`);
    job = await statusRes.json();
    const state = (job.state || job.status || '').toLowerCase();
    if (['success', 'completed', 'finished'].includes(state)) break;
    if (['error', 'failed', 'cancelled'].includes(state)) throw new Error(`Query ${state}: ${JSON.stringify(job)}`);
    await new Promise(r => setTimeout(r, 1000));
  }

  // Fetch results
  const statementId = job?.statements?.[0]?.id;
  if (!statementId) throw new Error('Query Service returned no statement ID.');
  const resultsRes = await fetch(
    `${QUERY_SERVICE_URL}/api/v1/queries/${queryJobId}/${statementId}/results?pageSize=1000`,
    { headers }
  );
  if (!resultsRes.ok) throw new Error(`Results failed: ${resultsRes.status} ${await resultsRes.text()}`);
  const data = await resultsRes.json();

  const columns = data.columns.map(c => c.name);
  const rows = (data.rows || data.data || []).map(row =>
    Object.fromEntries(columns.map((col, i) => [col, row[i]]))
  );
  return { columns, rows };
}

app.all('/', (req, res) => res.sendFile(join(__dirname, 'index.html')));

app.get('/api/orders', async (req, res) => {
  try {
    if (!KBC_TOKEN) return res.status(500).json({ error: 'KBC_TOKEN not set' });
    if (!WORKSPACE_ID) return res.status(500).json({ error: 'WORKSPACE_ID not set' });
    const result = await runQuery(req.query.sql || SQL);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/save', express.json(), async (req, res) => {
  try {
    if (!KBC_TOKEN) return res.status(500).json({ error: 'KBC_TOKEN not set' });
    if (!WORKSPACE_ID) return res.status(500).json({ error: 'WORKSPACE_ID not set' });
    const { statements } = req.body;
    if (!Array.isArray(statements) || statements.length === 0)
      return res.status(400).json({ error: 'No statements provided' });
    for (const sql of statements) await executeSQL(sql);
    res.json({ ok: true, count: statements.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Listening on port ${PORT}`);
  console.log('ENV:', {
    QUERY_SERVICE_URL,
    KBC_TOKEN: KBC_TOKEN ? `${KBC_TOKEN.slice(0, 10)}…` : '(not set)',
    WORKSPACE_ID: WORKSPACE_ID || '(not set)',
    BRANCH_ID,
    SQL,
  });
});
