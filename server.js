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

async function downloadUrl(url, destPath) {
  const resp = await fetch(url, { redirect: 'follow' });
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
      const r2 = await fetch(retryUrl, { redirect: 'follow' });
      if (!r2.ok || !r2.body) throw new Error(`download retry failed (${r2.status}) for ${url}`);
      await pipeline(Readable.fromWeb(r2.body), createWriteStream(destPath));
      return;
    }
    const m = html.match(/href="(\/uc\?export=download[^"]+)"/);
    if (m) {
      const retryUrl = 'https://drive.google.com' + m[1].replace(/&amp;/g, '&');
      const r2 = await fetch(retryUrl, { redirect: 'follow' });
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

app.get('/', (_req, res) => res.json({ ok: true, service: 'railway-github-ffmpeg', async_jobs: true }));
app.get('/health', (_req, res) => res.json({ ok: true }));

const jobs = new Map();
function dropJob(job) {
  jobs.delete(job.id);
  if (job.work) fs.rm(job.work, { recursive: true, force: true }).catch(() => {});
}

app.post('/jobs', async (req, res) => {
  if (!checkAuth(req, res)) return;
  const body = req.body || {};
  if (!Array.isArray(body.files) || body.files.length === 0) {
    return res.status(400).json({ error: 'files[] is required (array of { name, url })' });
  }
  const job = {
    id: randomUUID(), status: 'running', error: null,
    work: await fs.mkdtemp(path.join(os.tmpdir(), 'render-')),
    finalPath: null, outName: null, created: Date.now(), waiters: [],
  };
  jobs.set(job.id, job);
  setTimeout(() => dropJob(job), JOB_TTL_MS).unref();

  executeRender(body, job.work)
    .then(({ finalPath, outName }) => { job.finalPath = finalPath; job.outName = outName; job.status = 'done'; })
    .catch((err) => { console.error('[render] error:', err); job.status = 'error'; job.error = err.message; })
    .finally(() => { job.waiters.splice(0).forEach((fn) => fn()); });

  res.status(202).json({ job_id: job.id, status: 'running' });
});

app.get('/jobs/:id', (req, res) => {
  if (!checkAuth(req, res)) return;
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'job not found (expired or server restarted)' });
  const reply = () => res.json({ job_id: job.id, status: job.status, error: job.error });
  const waitSec = Math.min(Math.max(Number(req.query.wait) || 0, 0), 100);
  if (job.status !== 'running' || waitSec === 0) return reply();
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

// legacy synchronous endpoint
app.post('/render', async (req, res) => {
  if (!checkAuth(req, res)) return;
  const body = req.body || {};
  if (!Array.isArray(body.files) || body.files.length === 0) {
    return res.status(400).json({ error: 'files[] is required (array of { name, url })' });
  }
  const work = await fs.mkdtemp(path.join(os.tmpdir(), 'render-'));
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
