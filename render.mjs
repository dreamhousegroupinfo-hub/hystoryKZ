import { mkdirSync, writeFileSync, existsSync } from 'node:fs';
import { execSync } from 'node:child_process';
import path from 'node:path';

const p = JSON.parse(process.env.PAYLOAD || '{}');
const work = path.resolve('work');
const outDir = path.join(work, 'out');
mkdirSync(outDir, { recursive: true });

// Входные файлы: [{ name, url }] или [{ name, content_base64 }]
for (const f of p.files || []) {
  const target = path.join(work, path.basename(f.name));
  if (f.content_base64) {
    writeFileSync(target, Buffer.from(f.content_base64, 'base64'));
  } else if (f.url) {
    const r = await fetch(f.url);
    if (!r.ok) throw new Error('Download failed: ' + f.url + ' ' + r.status);
    writeFileSync(target, Buffer.from(await r.arrayBuffer()));
  }
}

// Скрипт: либо готовая команда, либо файл script_name
let cmd = p.script || '';
if (p.script_name) {
  const sp = path.join(work, path.basename(p.script_name));
  writeFileSync(sp, p.script);
  cmd = 'bash ' + sp;
}
if (!cmd) throw new Error('Empty script');

execSync(cmd, { cwd: work, stdio: 'inherit', shell: '/bin/bash' });

const out = path.join(work, path.basename(p.out || 'final.mp4'));
if (!existsSync(out)) throw new Error('Output not found: ' + out);
execSync('mv "' + out + '" "' + outDir + '/"');
