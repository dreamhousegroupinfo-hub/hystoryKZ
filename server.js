// ─────────────────────────────────────────────────────────────────────────────
// railway-github-ffmpeg
//
// Generic HTTP ffmpeg render service for the n8n "Scheduled and Manual Start"
// block-based pipeline.
//
// NO Google authentication here. n8n lists the Drive folder itself and passes
// PUBLIC download links; this service downloads each URL, runs a bash script,
// and streams the produced output file straight back. n8n uploads it to Drive.
//
// Contract (node "Call Railway FFmpeg" for a block, and the assemble trigger):
//   POST /render
//   body: {
//     topic_title?: string,                 // logging only
//     files: [ { name, url }, ... ],        // downloaded into the work dir under `name`
//     script?: string,                      // inline bash to write as run.sh and execute
//     script_name?: string,                 // OR: name of a downloaded .sh file to run
//     out?: string                          // output filename to stream back (default final.mp4)
//   }
//   -> response: the produced output file (Content-Type: video/mp4) as a binary body.
//
// If neither `script` nor `script_name` is given, it runs the newest downloaded *.sh.
//
// Auth (inbound, optional): set RENDER_TOKEN, send Authorization: Bearer <RENDER_TOKEN>.
// ─────────────────────────────────────────────────────────────────────────────

import express from 'express';
import { spawn } from 'node:child_process';
import { promises as fs, createReadStream, createWriteStream } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import os from 'node:os';
import path from 'node:path';

const PORT = process.env.PORT || 8080;
const RENDER_TOKEN = process.env.RENDER_TOKEN || '';

// ── download a single public URL to a local path ─────────────────────────────
async function downloadUrl(url, destPath) {
  const resp = await fetch(url, { redirect: 'follow' });
  if (!resp.ok || !resp.body) {
    throw new Error(`download failed (${resp.status}) for ${url}`);
  }
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

// ── run a bash script ────────────────────────────────────────────────────────
function runScript(scriptPath, cwd) {
  return new Promise((resolve, reject) => {
    const child = spawn('bash', [scriptPath], { cwd, env: process.env });
    let stderr = '';
    child.stdout.on('data', (d) => process.stdout.write(d));
    child.stderr.on('data', (d) => {
      stderr += d.toString();
      process.stderr.write(d);
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error('render script exited with code ' + code + '\n' + stderr.slice(-4000)));
    });
  });
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

app.get('/', (_req, res) => res.json({ ok: true, service: 'railway-github-ffmpeg' }));
app.get('/health', (_req, res) => res.json({ ok: true }));

app.post('/render', async (req, res) => {
  if (!checkAuth(req, res)) return;

  const { topic_title, files, script, script_name, out } = req.body || {};
  if (!Array.isArray(files) || files.length === 0) {
    return res.status(400).json({ error: 'files[] is required (array of { name, url })' });
  }
  const outName = (out && /^[\w.\-]+$/.test(out)) ? out : 'final.mp4';

  const work = await fs.mkdtemp(path.join(os.tmpdir(), 'render-'));
  console.log(`[render] topic="${topic_title || ''}" files=${files.length} out=${outName} work=${work}`);

  try {
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
    if (!scriptToRun) {
      return res.status(422).json({ error: 'no script to run (provide script, script_name, or a .sh file)' });
    }
    console.log(`[render] running ${scriptToRun}`);
    await runScript(path.join(work, scriptToRun), work);

    const finalPath = path.join(work, outName);
    try {
      await fs.access(finalPath);
    } catch {
      return res.status(500).json({ error: `render finished but ${outName} was not produced` });
    }

    res.setHeader('Content-Type', 'video/mp4');
    res.setHeader('Content-Disposition', `attachment; filename="${outName}"`);
    console.log(`[render] done -> streaming ${outName}`);
    await pipeline(createReadStream(finalPath), res);
  } catch (err) {
    console.error('[render] error:', err);
    if (!res.headersSent) res.status(500).json({ error: err.message });
    else res.end();
  } finally {
    fs.rm(work, { recursive: true, force: true }).catch(() => {});
  }
});

app.listen(PORT, () => console.log(`railway-github-ffmpeg listening on :${PORT}`));
