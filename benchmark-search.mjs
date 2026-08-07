// Search benchmark: latency (cold vs warm) + result quality.
// Usage: bun benchmark-search.mjs [baseUrl] [token]
// Cold = fresh cache key (limit variation), warm = repeat of same key (should hit
// in-memory cache). Each request uses its own random 28-hex token so the per-token
// 20/min search rate limit never interferes.
const BASE = process.argv[2] || 'https://monochrome1.gourabmahalikadarsh.workers.dev';
const TOKEN = process.argv[3] || 'c08cd03f71d0396064226a28dda4';

const QUERIES = [
  'pink floyd',
  'thousand knives',
  'what makes you beautiful',
  'senorita',
  'midnight rain',
  'touhou',
  'bohemian rhapsody',
  'abunaikioku',
  'yousuke yasui',
  'let it be',
];

function hex28() {
  const b = new Uint8Array(14);
  crypto.getRandomValues(b);
  return [...b].map(x => x.toString(16).padStart(2, '0')).join('');
}

async function search(q, limit) {
  const t0 = performance.now();
  let status = 0, body = null, err = null;
  try {
    const r = await fetch(`${BASE}/u/${hex28()}/search?q=${encodeURIComponent(q)}&limit=${limit}`);
    status = r.status;
    body = await r.json();
  } catch (e) { err = e.message; }
  return { ms: Math.round(performance.now() - t0), status, body, err };
}

function analyze(body) {
  if (!body || !body.tracks) return null;
  const tracks = body.tracks;
  const providers = {}, tiers = {};
  for (const t of tracks) {
    providers[t.provider || '?'] = (providers[t.provider || '?'] || 0) + 1;
    tiers[t.audioQuality || '?'] = (tiers[t.audioQuality || '?'] || 0) + 1;
  }
  const keys = new Map();
  let dupes = 0;
  for (const t of tracks) {
    const k = (t.title || '').toLowerCase().replace(/[^a-z0-9]/g, '') + '|' + (t.artist || '').toLowerCase().replace(/[^a-z0-9]/g, '');
    keys.set(k, (keys.get(k) || 0) + 1);
  }
  for (const n of keys.values()) if (n > 1) dupes += n - 1;
  return {
    tracks: tracks.length,
    albums: (body.albums || []).length,
    artists: (body.artists || []).length,
    playlists: (body.playlists || []).length,
    isrc: tracks.filter(t => t.isrc).length,
    dupes,
    atmos: tracks.filter(t => /[\u25d7\u25d6]/.test(t.title || '')).length,
    providers,
    tiers,
  };
}

const rows = [];
for (const q of QUERIES) {
  const cold = await search(q, 18);   // fresh key -> real network path
  const warm = await search(q, 18);   // same key -> memory/Redis fast path
  const a = analyze(cold.body);
  rows.push({
    query: q,
    coldMs: cold.status === 200 ? cold.ms : `${cold.status}${cold.err ? ':' + cold.err.slice(0, 30) : ''}`,
    warmMs: warm.status === 200 ? warm.ms : `${warm.status}`,
    tracks: a ? a.tracks : '-',
    albums: a ? a.albums : '-',
    artists: a ? a.artists : '-',
    playlists: a ? a.playlists : '-',
    isrc: a ? a.isrc : '-',
    dupes: a ? a.dupes : '-',
    tidal: a ? (a.providers.Tidal || 0) : '-',
    qobuz: a ? (a.providers.Qobuz || 0) : '-',
    tier: a ? Object.entries(a.tiers).map(([k, v]) => `${k}:${v}`).join(' ') : '-',
  });
  await new Promise(r => setTimeout(r, 300));
}

console.log('\n=== SEARCH BENCHMARK ===');
console.log(`base: ${BASE}`);
console.table(rows);

const ok = rows.filter(r => typeof r.coldMs === 'number' && typeof r.warmMs === 'number');
if (ok.length) {
  const avg = (k) => Math.round(ok.reduce((s, r) => s + r[k], 0) / ok.length);
  const max = (k) => Math.max(...ok.map(r => r[k]));
  const min = (k) => Math.min(...ok.map(r => r[k]));
  console.log(`cold:  avg ${avg('coldMs')}ms  min ${min('coldMs')}ms  max ${max('coldMs')}ms`);
  console.log(`warm:  avg ${avg('warmMs')}ms  min ${min('warmMs')}ms  max ${max('warmMs')}ms`);
  const tr = rows.reduce((s, r) => s + (typeof r.tracks === 'number' ? r.tracks : 0), 0);
  const qu = rows.reduce((s, r) => s + (typeof r.qobuz === 'number' ? r.qobuz : 0), 0);
  const du = rows.reduce((s, r) => s + (typeof r.dupes === 'number' ? r.dupes : 0), 0);
  const is = rows.reduce((s, r) => s + (typeof r.isrc === 'number' ? r.isrc : 0), 0);
  const at = rows.reduce((s, r) => s + (typeof r.tracks === 'number' ? r.tracks : 0), 0) || 1;
  console.log(`tracks total: ${tr} | qobuz hits: ${qu} | duplicate pairs: ${du} | isrc coverage: ${is}/${at} (${Math.round(is / at * 100)}%)`);
}
