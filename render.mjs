import { promises as fs, createWriteStream } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { spawn } from 'node:child_process';
import path from 'node:path';

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
      const retryUrl = formAction[1].replace(/&amp;/g, '&') + (formAction[1].includes('?') ? '&' : '?') + new URLSearchParams(params).toString();
      const r2 = await fetch(retryUrl, { redirect: 'follow' });
      if (!r2.ok || !r2.body) throw new Error(`download retry failed (${r2.status}) for ${url}`);
      await pipeline(Readable.fromWeb(r2.body), createWriteStream(destPath));
      return;
    }
    const m = html.match(/href="(\/uc\?export=download[^"]+)"/);
    if (m) {
      const r2 = await fetch('https://drive.google.com' + m[1].replace(/&amp;/g, '&'), { redirect: 'follow' });
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
    const child = spawn('bash', [scriptPath], { cwd, env: process.env, stdio: 'inherit' });
    child.on('error', reject);
    child.on('close', (code) => (code === 0 ? resolve() : reject(new Error('render script exited with code ' + code))));
  });
}

const body = JSON.parse(process.env.PAYLOAD || '{}');
const { topic_title, files = [], script, script_name, out } = body;
const outName = out && /^[\w.\-]+$/.test(out) ? out : 'final.mp4';
const work = path.resolve('work');
const outDir = path.resolve('output');
await fs.mkdir(work, { recursive: true });
await fs.mkdir(outDir, { recursive: true });
console.log(`[render] topic="${topic_title || ''}" files=${files.length} out=${outName}`);

for (const f of files) {
  if (!f || !f.name || !f.url) continue;
  if (!/^[\w.\-]+$/.test(f.name)) throw new Error('bad file name: ' + f.name);
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
  const sh = (await fs.readdir(work)).filter((n) => /\.sh$/i.test(n)).sort();
  scriptToRun = sh[sh.length - 1];
}
if (!scriptToRun) throw new Error('no script to run (provide script, script_name, or a .sh file)');
console.log(`[render] running ${scriptToRun}`);
await runScript(path.join(work, scriptToRun), work);

const produced = path.join(work, outName);
try { await fs.access(produced); } catch { throw new Error(`render finished but ${outName} was not produced`); }
await fs.rename(produced, path.join(outDir, outName));
console.log(`[render] done: ${outName}`);
