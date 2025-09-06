const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Allow all origins in local dev, including file:// (Origin null)
app.use(cors());
app.options('*', cors());
app.use(express.json({ limit: '10mb' }));

// Serve static files from the current directory
app.use(express.static(__dirname));

// Root route - serve index.html
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Auth route - serve auth.html
app.get('/auth.html', (req, res) => {
  res.sendFile(path.join(__dirname, 'auth.html'));
});

// JavaScript files routes
app.get('/app.js', (req, res) => {
  res.setHeader('Content-Type', 'application/javascript');
  res.sendFile(path.join(__dirname, 'app.js'));
});

app.get('/x10-browser.js', (req, res) => {
  res.setHeader('Content-Type', 'application/javascript');
  res.sendFile(path.join(__dirname, 'x10-browser.js'));
});

app.get('/data.js', (req, res) => {
  res.setHeader('Content-Type', 'application/javascript');
  res.sendFile(path.join(__dirname, 'data.js'));
});

app.get('/db.js', (req, res) => {
  res.setHeader('Content-Type', 'application/javascript');
  res.sendFile(path.join(__dirname, 'db.js'));
});

app.get('/excel_export.js', (req, res) => {
  res.setHeader('Content-Type', 'application/javascript');
  res.sendFile(path.join(__dirname, 'excel_export.js'));
});

// Health check
app.get('/health', (req, res) => {
  res.json({ ok: true, service: 'local-writer', cwd: process.cwd() });
});

// Write updated dataset into data.js
app.post('/update-data', async (req, res) => {
  try {
    const body = req.body;
    if (!body || !Array.isArray(body.items)) {
      return res.status(400).json({ ok: false, error: 'Invalid payload: { items: [...] } required' });
    }

    const content = 'window.OP_MASTER = ' + JSON.stringify(body.items, null, 2) + '\n';
    const targetPath = path.join(__dirname, 'data.js');

    await fs.promises.writeFile(targetPath, content, 'utf8');
    return res.json({ ok: true, message: 'data.js updated', path: targetPath, count: body.items.length });
  } catch (err) {
    console.error('Failed to write data.js:', err);
    return res.status(500).json({ ok: false, error: String(err) });
  }
});

app.listen(PORT, () => {
  console.log(`Local writer listening on http://localhost:${PORT}`);
});
