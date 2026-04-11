const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { Readable } = require('stream');
const morgan = require('morgan');

const app = express();
const PORT = Number(process.env.PORT || 3217);
const HOST = process.env.HOST || '127.0.0.1';
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const STORE_FILE = path.join(DATA_DIR, 'jobs.json');
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || '').replace(/\/$/, '');
const GITHUB_OWNER = process.env.GITHUB_OWNER || 'GratifluxTools';
const GITHUB_REPO = process.env.GITHUB_REPO || 'vendor-bin-win-x64';
const GITHUB_REF = process.env.GITHUB_REF || 'main';
const GITHUB_TOKEN = process.env.GITHUB_TOKEN || '';
const WORKFLOW_FILE = process.env.GITHUB_WORKFLOW_FILE || 'gha-ephemeral-ytdlp-portal.yml';
const CALLBACK_SECRET = process.env.CALLBACK_SECRET || '';
const APP_USER = process.env.APP_USER || '';
const APP_PASS = process.env.APP_PASS || '';

fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(STORE_FILE)) {
  fs.writeFileSync(STORE_FILE, JSON.stringify({ jobs: {} }, null, 2));
}

function readStore() {
  return JSON.parse(fs.readFileSync(STORE_FILE, 'utf8'));
}

function writeStore(store) {
  fs.writeFileSync(STORE_FILE, JSON.stringify(store, null, 2));
}

function withStore(mutator) {
  const store = readStore();
  const result = mutator(store);
  writeStore(store);
  return result;
}

function getJobById(jobId) {
  const store = readStore();
  return store.jobs[jobId] || null;
}

function findJobByToken(token) {
  const store = readStore();
  return Object.values(store.jobs).find((job) => job.downloadToken === token) || null;
}

function makeId(prefix = 'job') {
  return `${prefix}_${Date.now()}_${crypto.randomBytes(5).toString('hex')}`;
}

function boolString(value) {
  return value ? 'true' : 'false';
}

function sanitizeFilenamePart(value) {
  return String(value || '')
    .replace(/[\\/:*?"<>|\u0000-\u001f]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function buildDownloadName(job) {
  const fallbackBase = sanitizeFilenamePart(job.originalTitle || 'download') || 'download';
  const requestedRaw = sanitizeFilenamePart(job.requestedFilename || '');
  const ext = sanitizeFilenamePart(job.outputExt || '') || '';
  if (requestedRaw) {
    if (ext && !requestedRaw.toLowerCase().endsWith(`.${ext.toLowerCase()}`)) {
      return `${requestedRaw}.${ext}`;
    }
    return requestedRaw;
  }
  return ext ? `${fallbackBase}.${ext}` : fallbackBase;
}

function buildPublicDownloadUrl(job) {
  const rel = `/download/${job.downloadToken}`;
  return PUBLIC_BASE_URL ? `${PUBLIC_BASE_URL}${rel}` : rel;
}

function ensureAuth(req, res, next) {
  if (req.path.startsWith('/api/callback/')) return next();
  if (!APP_USER || !APP_PASS) return next();
  const header = req.headers.authorization || '';
  if (!header.startsWith('Basic ')) {
    res.setHeader('WWW-Authenticate', 'Basic realm="Ephemeral Portal"');
    return res.status(401).send('Authentication required');
  }
  const decoded = Buffer.from(header.slice(6), 'base64').toString('utf8');
  const idx = decoded.indexOf(':');
  const user = idx >= 0 ? decoded.slice(0, idx) : '';
  const pass = idx >= 0 ? decoded.slice(idx + 1) : '';
  if (user !== APP_USER || pass !== APP_PASS) {
    res.setHeader('WWW-Authenticate', 'Basic realm="Ephemeral Portal"');
    return res.status(401).send('Invalid credentials');
  }
  next();
}

function ensureCallbackSecret(req, res, next) {
  const provided = req.headers['x-callback-secret'];
  if (!CALLBACK_SECRET || provided !== CALLBACK_SECRET) {
    return res.status(403).json({ error: 'Invalid callback secret' });
  }
  next();
}

app.use(morgan('combined'));
app.use(express.json({ limit: '1mb' }));
app.use(ensureAuth);

app.get('/healthz', (req, res) => {
  res.json({ ok: true, service: 'gha-ytdlp-portal' });
});

app.get('/api/config', (req, res) => {
  res.json({
    publicBaseUrl: PUBLIC_BASE_URL || '',
    workflowFile: WORKFLOW_FILE,
    repo: `${GITHUB_OWNER}/${GITHUB_REPO}`
  });
});

app.post('/api/jobs', async (req, res) => {
  if (!GITHUB_TOKEN) {
    return res.status(500).json({ error: 'GITHUB_TOKEN is not configured on the VPS.' });
  }

  const sourceUrl = String(req.body.sourceUrl || '').trim();
  const requestFormat = String(req.body.requestFormat || 'auto').trim();
  const requestedFilename = String(req.body.requestedFilename || '').trim();
  const startTime = String(req.body.startTime || '').trim();
  const endTime = String(req.body.endTime || '').trim();
  const embedThumbnail = !!req.body.embedThumbnail;
  const embedMetadata = !!req.body.embedMetadata;
  const detailedLog = !!req.body.detailedLog;
  const preferModernCodecs = !!req.body.preferModernCodecs;

  if (!sourceUrl) {
    return res.status(400).json({ error: 'URL を入力してください。' });
  }
  if (!/^https?:\/\//i.test(sourceUrl)) {
    return res.status(400).json({ error: 'http または https の URL を指定してください。' });
  }
  if (startTime && endTime && startTime === endTime) {
    return res.status(400).json({ error: '開始時間と終了時間が同じです。' });
  }

  const job = {
    id: makeId('job'),
    sourceUrl,
    requestFormat,
    requestedFilename,
    startTime,
    endTime,
    embedThumbnail,
    embedMetadata,
    detailedLog,
    preferModernCodecs,
    downloadToken: crypto.randomBytes(24).toString('hex'),
    status: 'queued',
    latestLine: 'GitHub Actions へジョブを送信しています。',
    detailLog: '',
    originalTitle: '',
    outputExt: '',
    runnerUrl: '',
    expiresAt: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    errorMessage: ''
  };

  withStore((store) => {
    store.jobs[job.id] = job;
  });

  const callbackBase = PUBLIC_BASE_URL || `http://127.0.0.1:${PORT}`;
  const dispatchBody = {
    ref: GITHUB_REF,
    inputs: {
      job_id: job.id,
      source_url: sourceUrl,
      request_format: requestFormat,
      requested_filename: requestedFilename,
      embed_thumbnail: boolString(embedThumbnail),
      embed_metadata: boolString(embedMetadata),
      start_time: startTime,
      end_time: endTime,
      detailed_log: boolString(detailedLog),
      prefer_modern_codecs: boolString(preferModernCodecs),
      callback_base: callbackBase,
      callback_secret: CALLBACK_SECRET
    }
  };

  try {
    const response = await fetch(`https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/actions/workflows/${WORKFLOW_FILE}/dispatches`, {
      method: 'POST',
      headers: {
        'Accept': 'application/vnd.github+json',
        'Authorization': `Bearer ${GITHUB_TOKEN}`,
        'X-GitHub-Api-Version': '2022-11-28',
        'Content-Type': 'application/json',
        'User-Agent': 'gha-ytdlp-portal'
      },
      body: JSON.stringify(dispatchBody)
    });

    if (!response.ok) {
      const message = await response.text();
      withStore((store) => {
        store.jobs[job.id].status = 'failed';
        store.jobs[job.id].errorMessage = message;
        store.jobs[job.id].latestLine = 'workflow_dispatch の開始に失敗しました。';
        store.jobs[job.id].updatedAt = new Date().toISOString();
      });
      return res.status(502).json({ error: 'GitHub Actions の起動に失敗しました。', details: message });
    }

    withStore((store) => {
      store.jobs[job.id].status = 'queued';
      store.jobs[job.id].latestLine = 'GitHub Actions にジョブを送信しました。runner の起動を待っています。';
      store.jobs[job.id].updatedAt = new Date().toISOString();
    });

    const fresh = getJobById(job.id);
    res.json({ job: serializeJob(fresh) });
  } catch (error) {
    withStore((store) => {
      store.jobs[job.id].status = 'failed';
      store.jobs[job.id].errorMessage = error.message;
      store.jobs[job.id].latestLine = 'GitHub API への接続に失敗しました。';
      store.jobs[job.id].updatedAt = new Date().toISOString();
    });
    res.status(500).json({ error: error.message });
  }
});

function serializeJob(job) {
  if (!job) return null;
  return {
    id: job.id,
    sourceUrl: job.sourceUrl,
    requestFormat: job.requestFormat,
    requestedFilename: job.requestedFilename,
    embedThumbnail: job.embedThumbnail,
    embedMetadata: job.embedMetadata,
    startTime: job.startTime,
    endTime: job.endTime,
    detailedLog: job.detailedLog,
    preferModernCodecs: job.preferModernCodecs,
    status: job.status,
    latestLine: job.latestLine,
    detailLog: job.detailLog,
    originalTitle: job.originalTitle,
    outputExt: job.outputExt,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    expiresAt: job.expiresAt,
    errorMessage: job.errorMessage,
    downloadUrl: job.runnerUrl && job.status === 'ready' ? buildPublicDownloadUrl(job) : null,
    suggestedFilename: buildDownloadName(job)
  };
}

app.get('/api/jobs/:id', (req, res) => {
  const job = getJobById(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json({ job: serializeJob(job) });
});

app.post('/api/callback/:jobId/progress', ensureCallbackSecret, (req, res) => {
  const { jobId } = req.params;
  const latestLine = String(req.body.latestLine || '').slice(0, 5000);
  const status = String(req.body.status || '').trim() || 'running';
  const appendLog = String(req.body.appendLog || '').slice(0, 10000);
  const originalTitle = String(req.body.originalTitle || '').trim();

  const updated = withStore((store) => {
    const job = store.jobs[jobId];
    if (!job) return null;
    job.status = status;
    if (latestLine) job.latestLine = latestLine;
    if (appendLog) {
      const next = job.detailLog ? `${job.detailLog}\n${appendLog}` : appendLog;
      job.detailLog = next.slice(-250000);
    }
    if (originalTitle && !job.originalTitle) job.originalTitle = originalTitle;
    job.updatedAt = new Date().toISOString();
    return job;
  });

  if (!updated) return res.status(404).json({ error: 'Job not found' });
  res.json({ ok: true });
});

app.post('/api/callback/:jobId/ready', ensureCallbackSecret, (req, res) => {
  const { jobId } = req.params;
  const updated = withStore((store) => {
    const job = store.jobs[jobId];
    if (!job) return null;
    job.status = 'ready';
    job.latestLine = String(req.body.latestLine || 'ダウンロードの準備が完了しました。');
    job.runnerUrl = String(req.body.runnerUrl || '').trim();
    job.originalTitle = String(req.body.originalTitle || job.originalTitle || '').trim();
    job.outputExt = String(req.body.outputExt || job.outputExt || '').trim();
    job.expiresAt = String(req.body.expiresAt || '').trim() || new Date(Date.now() + 30 * 60 * 1000).toISOString();
    job.updatedAt = new Date().toISOString();
    if (req.body.appendLog) {
      const appended = String(req.body.appendLog).slice(0, 10000);
      job.detailLog = job.detailLog ? `${job.detailLog}\n${appended}` : appended;
    }
    return job;
  });

  if (!updated) return res.status(404).json({ error: 'Job not found' });
  res.json({ ok: true });
});

app.post('/api/callback/:jobId/finished', ensureCallbackSecret, (req, res) => {
  const { jobId } = req.params;
  const updated = withStore((store) => {
    const job = store.jobs[jobId];
    if (!job) return null;
    const status = String(req.body.status || '').trim() || 'expired';
    job.status = status;
    job.latestLine = String(req.body.latestLine || (status === 'failed' ? 'ダウンロード処理が失敗しました。' : 'ファイルの保持期間が終了しました。'));
    job.errorMessage = String(req.body.errorMessage || job.errorMessage || '').slice(0, 10000);
    if (req.body.appendLog) {
      const appended = String(req.body.appendLog).slice(0, 10000);
      job.detailLog = job.detailLog ? `${job.detailLog}\n${appended}` : appended;
    }
    job.updatedAt = new Date().toISOString();
    if (status !== 'ready') {
      job.runnerUrl = '';
    }
    return job;
  });

  if (!updated) return res.status(404).json({ error: 'Job not found' });
  res.json({ ok: true });
});

app.get('/download/:token', async (req, res) => {
  const job = findJobByToken(req.params.token);
  if (!job) {
    return res.status(404).send('Download token not found');
  }
  if (job.status !== 'ready' || !job.runnerUrl) {
    return res.status(410).send('This file is no longer available');
  }
  if (job.expiresAt && new Date(job.expiresAt).getTime() <= Date.now()) {
    return res.status(410).send('This file has expired');
  }

  try {
    const headers = {};
    if (req.headers.range) headers.Range = req.headers.range;
    const upstream = await fetch(job.runnerUrl, { headers });
    if (!upstream.ok && upstream.status !== 206) {
      const text = await upstream.text();
      return res.status(upstream.status).send(text || 'Upstream download failed');
    }

    const forwardHeaders = [
      'content-type',
      'content-length',
      'content-range',
      'accept-ranges',
      'last-modified',
      'etag'
    ];
    for (const name of forwardHeaders) {
      const value = upstream.headers.get(name);
      if (value) res.setHeader(name, value);
    }
    const downloadName = buildDownloadName(job);
    const encoded = encodeURIComponent(downloadName).replace(/['()]/g, escape).replace(/\*/g, '%2A');
    res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encoded}`);
    res.status(upstream.status);
    if (!upstream.body) return res.end();
    Readable.fromWeb(upstream.body).pipe(res);
  } catch (error) {
    res.status(502).send(`Proxy error: ${error.message}`);
  }
});

app.use(express.static(path.join(__dirname, 'public')));

app.listen(PORT, HOST, () => {
  console.log(`gha-ytdlp-portal listening on http://${HOST}:${PORT}`);
});
