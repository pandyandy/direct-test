import express from 'express';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { StorageClient } from '@keboola/storage-api-js-client-v2';

const __dirname = dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = process.env.PORT || 3000;

// StorageClient reads KBC_URL and KBC_TOKEN from env automatically
const client = new StorageClient();

function tableIdFromSql(sql) {
  const match = sql.match(/FROM\s+"([^"]+)"\."([^"]+)"/i);
  if (match) return `${match[1]}.${match[2]}`;
  return sql.trim();
}

app.all('/', (req, res) => res.sendFile(join(__dirname, 'index.html')));

app.get('/api/orders', async (req, res) => {
  try {
    const tableId = tableIdFromSql(req.query.sql || 'in.c-andy-test.sample_orders');
    const rows = await client.tables.export(tableId);
    const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
    res.json({ columns, rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/save', express.json(), async (req, res) => {
  try {
    const { tableId, rows } = req.body;
    if (!tableId) return res.status(400).json({ error: 'tableId required' });
    if (!Array.isArray(rows) || rows.length === 0)
      return res.status(400).json({ error: 'No rows provided' });
    await client.tables.load(tableId, rows, { incremental: true });
    res.json({ ok: true, count: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Listening on port ${PORT}`);
  console.log('ENV:', {
    KBC_URL: process.env.KBC_URL || '(not set)',
    KBC_TOKEN: process.env.KBC_TOKEN ? `${process.env.KBC_TOKEN.slice(0, 10)}…` : '(not set)',
  });
});
