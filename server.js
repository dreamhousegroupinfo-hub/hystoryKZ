import express from 'express';
import { spawn } from 'node:child_process';
import { promises as fs, createReadStream, createWriteStream } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { randomUUID } from 'node:crypto';
import os from 'node:os';
import path from 'node:path';

const PORT = process.env.PORT || 8080;
const RENDER_TOKEN = process.env.RENDER_TOKEN || '';
const JOB_TTL_MS = 60 * 60 * 1000;
const DATA_DIR = process.env.DATA_DIR || path.join(os.tmpdir(), 'render-data');
const WORK_ROOT = path.join(DATA_DIR, 'work');
const META_DIR = path.join(DATA_DIR, 'meta');
await fs.mkdir(WORK_ROOT, { recursive: true });
await fs.mkdir(META_DIR, { recursive: true });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchWithRetry(url) {
  let lastErr;
  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      const resp = await fetch(url, { redirect: 'follow' });
      if (resp.status >= 500) {
        lastErr = new Error(`download failed (${resp.status}) for ${url}`);
        await resp.body?.cancel().catch(() => {});
      } else {
        return resp;
      }
    } catch (e) {
      lastErr = e;
    }
    await sleep(attempt * 3000);
  }
  throw lastErr;
}

async function downloadUrl(url, destPath) {
  const resp = await fetchWithRetry(url);
  if (!resp.ok || !resp.body) throw new Error(`download failed (${resp.status}) for ${url}`);
  const ctype = resp.headers.get('content-type') || '';
  if (ctype.includes('text/html')) {
    const html = await resp.text();
    const formAction = html.match(/action="(https:\/\/drive\.usercontent\.google\.com\/download[^"]*)"/);
    if (formAction) {
      const params = {};
      const re = /name="([^"]+)"\s+value="([^"]*)"/g;
      let mm;
      while ((mm = re.exec(html)) !== null) params[mm[1]] = mm[2];
      const usp = new URLSearchParams(params);
      const retryUrl = formAction[1].replace(/&amp;/g, '&') + (formAction[1].includes('?') ? '&' : '?') + usp.toString();
      const r2 = await fetchWithRetry(retryUrl);
      if (!r2.ok || !r2.body) throw new Error(`download retry failed (${r2.status}) for ${url}`);
      await pipeline(Readable.fromWeb(r2.body), createWriteStream(destPath));
      return;
    }
    const m = html.match(/href="(\/uc\?export=download[^"]+)"/);
    if (m) {
      const retryUrl = 'https://drive.google.com' + m[1].replace(/&amp;/g, '&');
      const r2 = await fetchWithRetry(retryUrl);
      if (!r2.ok || !r2.body) throw new Error(`download retry failed (${r2.status}) for ${url}`);
      await pipeline(Readable.fromWeb(r2.body), createWriteStream(destPath));
      return;
    }
    throw new Error(`got HTML instead of file for ${url} (is it shared "anyone with link"?)`);
  }
  await pipeline(Readable.fromWeb(resp.body), createWriteStream(destPath));
}

function runScript(scriptPath, cwd) {
  return new Promise((resolve, reject) => {
    const child = spawn('bash', [scriptPath], { cwd, env: process.env });
    let stderr = '';
    child.stdout.on('data', (d) => process.stdout.write(d));
    child.stderr.on('data', (d) => { stderr += d.toString(); process.stderr.write(d); });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error('render script exited with code ' + code + '\n' + stderr.slice(-4000)));
    });
  });
}

async function executeRender(body, work) {
  const { topic_title, files, script, script_name, out } = body || {};
  const outName = (out && /^[\w.\-]+$/.test(out)) ? out : 'final.mp4';
  console.log(`[render] topic="${topic_title || ''}" files=${files.length} out=${outName} work=${work}`);
  for (const f of files) {
    if (!f || !f.name || !f.url) continue;
    await downloadUrl(f.url, path.join(work, f.name));
    console.log(`[render] downloaded ${f.name}`);
  }
  let scriptToRun;
  if (typeof script === 'string' && script.trim()) {
    scriptToRun = 'run.sh';
    await fs.writeFile(path.join(work, scriptToRun), script, 'utf8');
  } else if (script_name) {
    scriptToRun = script_name;
  } else {
    const local = await fs.readdir(work);
    const shFiles = local.filter((n) => /\.sh$/i.test(n)).sort();
    scriptToRun = shFiles[shFiles.length - 1];
  }
  if (!scriptToRun) throw new Error('no script to run (provide script, script_name, or a .sh file)');
  console.log(`[render] running ${scriptToRun}`);
  await runScript(path.join(work, scriptToRun), work);
  const finalPath = path.join(work, outName);
  try { await fs.access(finalPath); } catch { throw new Error(`render finished but ${outName} was not produced`); }
  return { finalPath, outName };
}

const app = express();
app.use(express.json({ limit: '10mb' }));

function checkAuth(req, res) {
  if (!RENDER_TOKEN) return true;
  const hdr = req.get('authorization') || '';
  if (hdr === `Bearer ${RENDER_TOKEN}`) return true;
  res.status(401).json({ error: 'unauthorized' });
  return false;
}

// ---- jobs: in-memory map + metadata on disk + single-slot queue ----
const jobs = new Map();
const queue = [];
let active = false;

const isPending = (job) => job.status === 'running';

async function persist(job) {
  const meta = {
    id: job.id, status: job.status, error: job.error, work: job.work,
    finalPath: job.finalPath, outName: job.outName, created: job.created,
  };
  try { await fs.writeFile(path.join(META_DIR, job.id + '.json'), JSON.stringify(meta)); } catch (e) { console.error('[persist]', e.message); }
}

function dropJob(job) {
  jobs.delete(job.id);
  const qi = queue.indexOf(job);
  if (qi >= 0) queue.splice(qi, 1);
  if (job.work) fs.rm(job.work, { recursive: true, force: true }).catch(() => {});
  fs.rm(path.join(META_DIR, job.id + '.json'), { force: true }).catch(() => {});
}

function scheduleDrop(job, ms) {
  setTimeout(() => dropJob(job), Math.max(1000, ms)).unref();
}

function pump() {
  if (active) return;
  const job = queue.shift();
  if (!job) return;
  active = true;
  console.log(`[queue] start ${job.id} (waiting: ${queue.length})`);
  executeRender(job.body, job.work)
    .then(({ finalPath, outName }) => { job.finalPath = finalPath; job.outName = outName; job.status = 'done'; })
    .catch((err) => { console.error('[render] error:', err); job.status = 'error'; job.error = err.message; })
    .finally(async () => {
      job.body = null;
      await persist(job);
      job.waiters.splice(0).forEach((fn) => fn());
      active = false;
      pump();
    });
}

// restore jobs after a restart; anything that was unfinished is reported as an error, not a 404
async function restoreJobs() {
  let names = [];
  try { names = await fs.readdir(META_DIR); } catch { return; }
  const known = new Set();
  for (const n of names) {
    if (!n.endsWith('.json')) continue;
    try {
      const meta = JSON.parse(await fs.readFile(path.join(META_DIR, n), 'utf8'));
      const job = { ...meta, waiters: [], body: null };
      if (job.status === 'running') {
        job.status = 'error';
        job.error = 'interrupted by service restart';
        await persist(job);
      }
      jobs.set(job.id, job);
      known.add(path.basename(job.work || ''));
      scheduleDrop(job, job.created + JOB_TTL_MS - Date.now());
    } catch (e) { console.error('[restore] bad meta', n, e.message); }
  }
  // remove work dirs that have no metadata
  try {
    for (const d of await fs.readdir(WORK_ROOT)) {
      if (!known.has(d)) fs.rm(path.join(WORK_ROOT, d), { recursive: true, force: true }).catch(() => {});
    }
  } catch {}
  console.log(`[restore] ${jobs.size} job(s) restored from ${META_DIR}`);
}
await restoreJobs();

app.get('/', (_req, res) => res.json({ ok: true, service: 'railway-github-ffmpeg', async_jobs: true, queue: queue.length, active }));
app.get('/health', (_req, res) => res.json({ ok: true }));

app.get('/jobs', (req, res) => {
  if (!checkAuth(req, res)) return;
  res.json({
    active, queued: queue.length,
    jobs: [...jobs.values()].map((j) => ({ job_id: j.id, status: j.status, error: j.error, created: j.created })),
  });
});

app.post('/jobs', async (req, res) => {
  if (!checkAuth(req, res)) return;
  const body = req.body || {};
  if (!Array.isArray(body.files) || body.files.length === 0) {
    return res.status(400).json({ error: 'files[] is required (array of { name, url })' });
  }
  const id = randomUUID();
  const job = {
    id, status: 'running', error: null,
    work: await fs.mkdtemp(path.join(WORK_ROOT, 'render-')),
    finalPath: null, outName: null, created: Date.now(), waiters: [], body,
  };
  jobs.set(job.id, job);
  scheduleDrop(job, JOB_TTL_MS);
  await persist(job);
  queue.push(job);
  const position = queue.length + (active ? 1 : 0);
  pump();
  res.status(202).json({ job_id: job.id, status: 'running', queue_position: position });
});

app.get('/jobs/:id', (req, res) => {
  if (!checkAuth(req, res)) return;
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'job not found (expired or server restarted)' });
  const reply = () => res.json({ job_id: job.id, status: job.status, error: job.error });
  const waitSec = Math.min(Math.max(Number(req.query.wait) || 0, 0), 100);
  if (!isPending(job) || waitSec === 0) return reply();
  let finished = false;
  const done = () => { if (finished) return; finished = true; clearTimeout(timer); reply(); };
  const timer = setTimeout(() => { job.waiters = job.waiters.filter((fn) => fn !== done); done(); }, waitSec * 1000);
  job.waiters.push(done);
  req.on('close', () => { finished = true; clearTimeout(timer); job.waiters = job.waiters.filter((fn) => fn !== done); });
});

app.get('/jobs/:id/file', async (req, res) => {
  if (!checkAuth(req, res)) return;
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'job not found' });
  if (job.status !== 'done') return res.status(409).json({ error: 'job is ' + job.status, detail: job.error });
  res.setHeader('Content-Type', 'video/mp4');
  res.setHeader('Content-Disposition', `attachment; filename="${job.outName}"`);
  try {
    await pipeline(createReadStream(job.finalPath), res);
    setTimeout(() => dropJob(job), 10 * 60 * 1000).unref();
  } catch (err) {
    console.error('[render] stream error:', err);
    if (!res.headersSent) res.status(500).json({ error: err.message }); else res.end();
  }
});

// legacy synchronous endpoint (not queued)
app.post('/render', async (req, res) => {
  if (!checkAuth(req, res)) return;
  const body = req.body || {};
  if (!Array.isArray(body.files) || body.files.length === 0) {
    return res.status(400).json({ error: 'files[] is required (array of { name, url })' });
  }
  const work = await fs.mkdtemp(path.join(WORK_ROOT, 'render-'));
  try {
    const { finalPath, outName } = await executeRender(body, work);
    res.setHeader('Content-Type', 'video/mp4');
    res.setHeader('Content-Disposition', `attachment; filename="${outName}"`);
    await pipeline(createReadStream(finalPath), res);
  } catch (err) {
    console.error('[render] error:', err);
    if (!res.headersSent) res.status(500).json({ error: err.message }); else res.end();
  } finally {
    fs.rm(work, { recursive: true, force: true }).catch(() => {});
  }
});

app.listen(PORT, () => console.log(`ffmpeg render service listening on :${PORT}`));
