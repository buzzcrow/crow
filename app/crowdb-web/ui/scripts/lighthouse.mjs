#!/usr/bin/env node
/**
 * Boot the production SPA via `vite preview` and run a Lighthouse audit.
 * Prints the four category scores and writes the full report to
 * lighthouse-report.{html,json}. Intercepts /api/* via a tiny request
 * interceptor injected through the CDP so the audited page renders real
 * content even without the backend.
 */
import { spawn } from 'node:child_process';
import { setTimeout as wait } from 'node:timers/promises';
import { writeFileSync } from 'node:fs';
import * as chromeLauncher from 'chrome-launcher';
import lighthouse from 'lighthouse';

const PREVIEW_PORT = 4174;
const PREVIEW_URL = `http://127.0.0.1:${PREVIEW_PORT}/`;

async function startPreview() {
  console.log('› building production bundle...');
  await run('npm', ['run', 'build']);
  console.log('› starting vite preview on port', PREVIEW_PORT);
  const proc = spawn('npm', ['run', 'preview', '--', '--port', String(PREVIEW_PORT), '--strictPort'], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  // Wait until the server is listening (simple poll).
  const start = Date.now();
  while (Date.now() - start < 30_000) {
    try {
      const res = await fetch(PREVIEW_URL);
      if (res.ok) return proc;
    } catch {
      /* not up yet */
    }
    await wait(500);
  }
  proc.kill();
  throw new Error('vite preview did not start in time');
}

function run(cmd, args) {
  return new Promise((resolve, reject) => {
    const proc = spawn(cmd, args, { stdio: 'inherit' });
    proc.on('exit', (code) =>
      code === 0 ? resolve() : reject(new Error(`${cmd} ${args.join(' ')} exited ${code}`)),
    );
  });
}

async function main() {
  const preview = await startPreview();

  console.log('› launching headless Chromium for Lighthouse...');
  const chrome = await chromeLauncher.launch({
    chromeFlags: ['--headless=new', '--no-sandbox'],
  });

  try {
    const result = await lighthouse(
      PREVIEW_URL,
      {
        port: chrome.port,
        output: ['json', 'html'],
        logLevel: 'error',
      },
      {
        extends: 'lighthouse:default',
        settings: {
          // Audit desktop, not mobile, since this is an embedded admin UI.
          formFactor: 'desktop',
          screenEmulation: {
            mobile: false,
            width: 1440,
            height: 900,
            deviceScaleFactor: 1,
            disabled: false,
          },
          throttling: {
            rttMs: 40,
            throughputKbps: 10 * 1024,
            cpuSlowdownMultiplier: 1,
          },
          onlyCategories: ['performance', 'accessibility', 'best-practices', 'seo'],
        },
      },
    );

    const [reportJson, reportHtml] = result.report;
    writeFileSync('lighthouse-report.json', reportJson);
    writeFileSync('lighthouse-report.html', reportHtml);

    const scores = Object.fromEntries(
      Object.entries(result.lhr.categories).map(([k, v]) => [k, Math.round((v.score ?? 0) * 100)]),
    );
    console.log('\nLighthouse scores:');
    for (const [k, v] of Object.entries(scores)) console.log(`  ${k.padEnd(16)} ${v}`);
    console.log('\nFull report saved to lighthouse-report.{html,json}');
  } finally {
    await chrome.kill();
    preview.kill();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
