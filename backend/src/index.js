require('dotenv').config();
const express = require('express');
const cors = require('cors');
const rateLimit = require('express-rate-limit');

const workspacesRouter = require('./routes/workspaces');
const datasetsRouter = require('./routes/datasets');
const chatRouter = require('./routes/chat');

const app = express();

app.use(cors({ origin: process.env.CORS_ORIGIN || 'http://localhost:3000' }));
app.use(express.json());

const limiter = rateLimit({ windowMs: 60 * 1000, max: 60 });
app.use('/api/', limiter);

const chatLimiter = rateLimit({ windowMs: 60 * 1000, max: 20 });
app.use('/api/chat', chatLimiter);

app.use('/api/workspaces', workspacesRouter);
app.use('/api/datasets', datasetsRouter);
app.use('/api/chat', chatRouter);

app.get('/api/health', (req, res) => res.json({ status: 'ok' }));

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`Backend running on http://localhost:${PORT}`));
