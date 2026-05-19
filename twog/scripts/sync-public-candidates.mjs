import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import process from 'node:process';

const source = process.env.TWOG_COMMAND_CENTER_URL ?? 'http://127.0.0.1:8792';
const output = process.env.TWOG_PUBLIC_CANDIDATES_OUT ?? join(process.cwd(), 'data', 'public-candidates.json');

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.status} ${response.statusText}`);
  }
  return response.json();
}

const listing = await fetchJson(`${source}/api/public-candidates?limit=100`);
const rows = Array.isArray(listing.candidates) ? listing.candidates : [];
const candidates = [];

for (const row of rows) {
  if (!row?.candidate_id) continue;
  const detail = await fetchJson(`${source}/api/public-candidates/${row.candidate_id}`);
  candidates.push(detail);
}

const payload = {
  generatedAt: new Date().toISOString(),
  source,
  candidates,
};

await mkdir(dirname(output), { recursive: true });
await writeFile(output, `${JSON.stringify(payload, null, 2)}\n`);
console.log(`Wrote ${candidates.length} public candidate(s) to ${output}`);
