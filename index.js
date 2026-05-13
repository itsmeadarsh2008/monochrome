// Qobuz + Tidal — Eclipse Music Addon
// Cloudflare Workers (Hono) — ISRC Scoring Engine v1.4 + Upstash Redis + Full Catalog
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { Redis } from '@upstash/redis/cloudflare';

const app = new Hono();
app.use('*', cors());

// ─── Constants ────────────────────────────────────────────────────────────────
const UA           = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36';
const TIMEOUT_MS   = 12000;
const STREAM_TTL   = 200;
const INSTANCE_TTL = 300;
const DEEZER_API   = 'https://api.deezer.com';

// Qobuz credentials
const APP_ID     = '312369995';
const USER_TOKEN = 'XX7seyZt4OaHGPgksFUldL2Ig0cH6jqcKSAfOAiAGBzw1HosDl9vfQTGRQEo2zkkcwP9ADc3L20nYNaI0l7E4g';
const SECRET     = 'e79f8b9be485692b0e5f9dd895826368';

// ─── Qobuz Instance Pool ──────────────────────────────────────────────────────
const QOBUZ_INSTANCES = [
  'https://qobuz-api1.onrender.com',
  'https://trypt-hifi-dl-456461932686.us-west1.run.app',
  'https://qobuz-api.stremio123.duckdns.org',
  'https://qobuz.kennyy.com.br/api',
];

// ─── HiFi (Tidal) Instance Pool ───────────────────────────────────────────────
const DEFAULT_HIFI_INSTANCES = [
  'https://hifi-api-pj08.onrender.com',
  'https://mono.kennyy.com.br/hifi-api',
  'https://tidal-api.binimum.org',
  'https://triton.squid.wtf',
  'https://ohio-1.monochrome.tf',
  'https://frankfurt-1.monochrome.tf',
  'https://vogel.qqdl.site',
  'https://eu-central.monochrome.tf',
  'https://us-west.monochrome.tf',
  'https://hifi.geeked.wtf',
  'https://monochrome-api.samidy.com',
  'https://hifi-two.spotisaver.net',
  'https://wolf.qqdl.site',
  'https://katze.qqdl.site',
  'https://hund.qqdl.site',
  'https://api.monochrome.tf',
];

// ─── In-memory track meta cache ───────────────────────────────────────────────
const TRACK_META_CACHE = new Map();
function cacheTrackMeta(id, title, artist, isrc) {
  if (!id || !title) return;
  TRACK_META_CACHE.set(String(id), { title, artist: artist || 'Unknown', isrc: isrc || null });
  if (TRACK_META_CACHE.size > 5000) TRACK_META_CACHE.delete(TRACK_META_CACHE.keys().next().value);
}

// ─── Upstash Redis + in-memory fallback ───────────────────────────────────────
const memCache = new Map();
function getRedis(env) {
  if (env?.UPSTASH_REDIS_REST_URL && env?.UPSTASH_REDIS_REST_TOKEN) return Redis.fromEnv(env);
  return null;
}
async function rGet(redis, key) {
  if (redis) { try { return await redis.get(key); } catch {} }
  const e = memCache.get(key);
  if (!e) return null;
  if (Date.now() > e.exp) { memCache.delete(key); return null; }
  return e.val;
}
async function rSet(redis, key, value, ttl) {
  if (redis) { try { await redis.set(key, value, { ex: ttl }); return; } catch {} }
  memCache.set(key, { val: value, exp: Date.now() + ttl * 1000 });
  if (memCache.size > 500) memCache.delete(memCache.keys().next().value);
}

// ─── HTTP helper ──────────────────────────────────────────────────────────────
async function httpGet(url, params, timeout) {
  const u = new URL(url);
  if (params) Object.entries(params).forEach(([k, v]) => u.searchParams.set(k, String(v)));
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeout || 10000);
  try {
    const r = await fetch(u.toString(), { headers: { 'User-Agent': UA, 'Accept': 'application/json' }, signal: ctrl.signal });
    clearTimeout(timer);
    if (!r.ok) { try { await r.arrayBuffer(); } catch {} throw new Error('HTTP ' + r.status); }
    return r.json();
  } catch (e) { clearTimeout(timer); throw e; }
}

// ─── Compact MD5 ──────────────────────────────────────────────────────────────
function md5(str) {
  function RL(v,n){return(v<<n)|(v>>>(32-n));}
  function AU(x,y){const x8=(x&0x80000000),y8=(y&0x80000000),x4=(x&0x40000000),y4=(y&0x40000000),r=(x&0x3FFFFFFF)+(y&0x3FFFFFFF);if(x4&y4)return(r^0x80000000^x8^y8);if(x4|y4){if(r&0x40000000)return(r^0xC0000000^x8^y8);return(r^0x40000000^x8^y8);}return(r^x8^y8);}
  function F(x,y,z){return(x&y)|((~x)&z);}function G(x,y,z){return(x&z)|(y&(~z));}function H(x,y,z){return x^y^z;}function I(x,y,z){return y^(x|(~z));}
  function FF(a,b,c,d,x,s,ac){a=AU(a,AU(AU(F(b,c,d),x),ac));return AU(RL(a,s),b);}
  function GG(a,b,c,d,x,s,ac){a=AU(a,AU(AU(G(b,c,d),x),ac));return AU(RL(a,s),b);}
  function HH(a,b,c,d,x,s,ac){a=AU(a,AU(AU(H(b,c,d),x),ac));return AU(RL(a,s),b);}
  function II(a,b,c,d,x,s,ac){a=AU(a,AU(AU(I(b,c,d),x),ac));return AU(RL(a,s),b);}
  function CW(s){const ml=s.length,nw_t1=ml+8,nw_t2=(nw_t1-(nw_t1%64))/64,nw=(nw_t2+1)*16,wa=Array(nw-1);let bp=0,bc=0;while(bc<ml){const wc=(bc-(bc%4))/4,pos=(bc%4)*8;wa[wc]=(wa[wc]|(s.charCodeAt(bc)<<pos));bc++;}const wc2=(bc-(bc%4))/4;wa[wc2]=(wa[wc2]|(0x80<<((bc%4)*8)));wa[nw-2]=ml<<3;wa[nw-1]=ml>>>29;return wa;}
  function WH(v){let r='',t='',byte,c;for(c=0;c<=3;c++){byte=(v>>>(c*8))&255;t='0'+byte.toString(16);r+=t.substr(t.length-2,2);}return r;}
  const x=CW(str);let k,a=0x67452301,b=0xEFCDAB89,c2=0x98BADCFE,d=0x10325476,AA,BB,CC,DD;
  const S11=7,S12=12,S13=17,S14=22,S21=5,S22=9,S23=14,S24=20,S31=4,S32=11,S33=16,S34=23,S41=6,S42=10,S43=15,S44=21;
  for(k=0;k<x.length;k+=16){AA=a;BB=b;CC=c2;DD=d;a=FF(a,b,c2,d,x[k],S11,0xD76AA478);d=FF(d,a,b,c2,x[k+1],S12,0xE8C7B756);c2=FF(c2,d,a,b,x[k+2],S13,0x242070DB);b=FF(b,c2,d,a,x[k+3],S14,0xC1BDCEEE);a=FF(a,b,c2,d,x[k+4],S11,0xF57C0FAF);d=FF(d,a,b,c2,x[k+5],S12,0x4787C62A);c2=FF(c2,d,a,b,x[k+6],S13,0xA8304613);b=FF(b,c2,d,a,x[k+7],S14,0xFD469501);a=FF(a,b,c2,d,x[k+8],S11,0x698098D8);d=FF(d,a,b,c2,x[k+9],S12,0x8B44F7AF);c2=FF(c2,d,a,b,x[k+10],S13,0xFFFF5BB1);b=FF(b,c2,d,a,x[k+11],S14,0x895CD7BE);a=FF(a,b,c2,d,x[k+12],S11,0x6B901122);d=FF(d,a,b,c2,x[k+13],S12,0xFD987193);c2=FF(c2,d,a,b,x[k+14],S13,0xA679438E);b=FF(b,c2,d,a,x[k+15],S14,0x49B40821);a=GG(a,b,c2,d,x[k+1],S21,0xF61E2562);d=GG(d,a,b,c2,x[k+6],S22,0xC040B340);c2=GG(c2,d,a,b,x[k+11],S23,0x265E5A51);b=GG(b,c2,d,a,x[k],S24,0xE9B6C7AA);a=GG(a,b,c2,d,x[k+5],S21,0xD62F105D);d=GG(d,a,b,c2,x[k+10],S22,0x02441453);c2=GG(c2,d,a,b,x[k+15],S23,0xD8A1E681);b=GG(b,c2,d,a,x[k+4],S24,0xE7D3FBC8);a=GG(a,b,c2,d,x[k+9],S21,0x21E1CDE6);d=GG(d,a,b,c2,x[k+14],S22,0xC33707D6);c2=GG(c2,d,a,b,x[k+3],S23,0xF4D50D87);b=GG(b,c2,d,a,x[k+8],S24,0x455A14ED);a=GG(a,b,c2,d,x[k+13],S21,0xA9E3E905);d=GG(d,a,b,c2,x[k+2],S22,0xFCEFA3F8);c2=GG(c2,d,a,b,x[k+7],S23,0x676F02D9);b=GG(b,c2,d,a,x[k+12],S24,0x8D2A4C8A);a=HH(a,b,c2,d,x[k+5],S31,0xFFFA3942);d=HH(d,a,b,c2,x[k+8],S32,0x8771F681);c2=HH(c2,d,a,b,x[k+11],S33,0x6D9D6122);b=HH(b,c2,d,a,x[k+14],S34,0xFDE5380C);a=HH(a,b,c2,d,x[k+1],S31,0xA4BEEA44);d=HH(d,a,b,c2,x[k+4],S32,0x4BDECFA9);c2=HH(c2,d,a,b,x[k+7],S33,0xF6BB4B60);b=HH(b,c2,d,a,x[k+10],S34,0xBEBFBC70);a=HH(a,b,c2,d,x[k+13],S31,0x289B7EC6);d=HH(d,a,b,c2,x[k],S32,0xEAA127FA);c2=HH(c2,d,a,b,x[k+3],S33,0xD4EF3085);b=HH(b,c2,d,a,x[k+6],S34,0x04881D05);a=HH(a,b,c2,d,x[k+9],S31,0xD9D4D039);d=HH(d,a,b,c2,x[k+12],S32,0xE6DB99E5);c2=HH(c2,d,a,b,x[k+15],S33,0x1FA27CF8);b=HH(b,c2,d,a,x[k+2],S34,0xC4AC5665);a=II(a,b,c2,d,x[k],S41,0xF4292244);d=II(d,a,b,c2,x[k+7],S42,0x432AFF97);c2=II(c2,d,a,b,x[k+14],S43,0xAB9423A7);b=II(b,c2,d,a,x[k+5],S44,0xFC93A039);a=II(a,b,c2,d,x[k+12],S41,0x655B59C3);d=II(d,a,b,c2,x[k+3],S42,0x8F0CCC92);c2=II(c2,d,a,b,x[k+10],S43,0xFFEFF47D);b=II(b,c2,d,a,x[k+1],S44,0x85845DD1);a=II(a,b,c2,d,x[k+8],S41,0x6FA87E4F);d=II(d,a,b,c2,x[k+15],S42,0xFE2CE6E0);c2=II(c2,d,a,b,x[k+6],S43,0xA3014314);b=II(b,c2,d,a,x[k+13],S44,0x4E0811A1);a=II(a,b,c2,d,x[k+4],S41,0xF7537E82);d=II(d,a,b,c2,x[k+11],S42,0xBD3AF235);c2=II(c2,d,a,b,x[k+2],S43,0x2AD7D2BB);b=II(b,c2,d,a,x[k+9],S44,0xEB86D391);a=AU(a,AA);b=AU(b,BB);c2=AU(c2,CC);d=AU(d,DD);}
  return (WH(a)+WH(b)+WH(c2)+WH(d)).toLowerCase();
}

// ─── Token store ──────────────────────────────────────────────────────────────
const TOKEN_STORE = new Map();
function generateToken() {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let t = '';
  const arr = new Uint8Array(28);
  crypto.getRandomValues(arr);
  for (const b of arr) t += chars[b % chars.length];
  return t;
}
async function saveToken(redis, token, data) {
  TOKEN_STORE.set(token, data);
  if (redis) { try { await redis.set('qt:token:' + token, JSON.stringify(data), { ex: 2592000 }); } catch {} }
}
async function loadToken(redis, token) {
  if (TOKEN_STORE.has(token)) return TOKEN_STORE.get(token);
  if (redis) {
    try {
      const raw = await redis.get('qt:token:' + token);
      if (raw) { const d = typeof raw === 'string' ? JSON.parse(raw) : raw; TOKEN_STORE.set(token, d); return d; }
    } catch {}
  }
  return null;
}

// ─── Instance health — race to first winner ────────────────────────────────
async function getWorkingHiFiInstance(instances, redis) {
  const list     = (instances && instances.length) ? instances : DEFAULT_HIFI_INSTANCES;
  const cacheKey = 'qt:hifi:working:v4';
  const cached   = await rGet(redis, cacheKey);
  if (cached) return cached;
  for (const inst of list) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 2500);
    try {
      const r = await fetch(`${inst}/search/?s=test&limit=1`, { headers: { 'User-Agent': UA }, signal: ctrl.signal });
      clearTimeout(timer);
      try { await r.arrayBuffer(); } catch {}
      if (r.ok) {
        await rSet(redis, cacheKey, inst, INSTANCE_TTL);
        return inst;
      }
    } catch { clearTimeout(timer); }
  }
  return DEFAULT_HIFI_INSTANCES[4];
}

async function getWorkingQobuzInstance(redis) {
  const cacheKey = 'qt:qobuz:working:v4';
  const cached   = await rGet(redis, cacheKey);
  if (cached) return cached;
  for (const inst of QOBUZ_INSTANCES) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 2500);
    try {
      const r = await fetch(`${inst}/search?q=test&limit=1`, { headers: { 'User-Agent': UA }, signal: ctrl.signal });
      clearTimeout(timer);
      try { await r.arrayBuffer(); } catch {}
      if (r.ok) {
        await rSet(redis, cacheKey, inst, INSTANCE_TTL);
        return inst;
      }
    } catch { clearTimeout(timer); }
  }
  return QOBUZ_INSTANCES[0];
}

// ─── Qobuz proxy API (uses instances, not qobuz.com directly) ─────────────
async function qobuzProxyGet(endpoint, params, redis) {
  const inst = await getWorkingQobuzInstance(redis);
  return httpGet((inst || QOBUZ_INSTANCES[0]) + endpoint, params || {}, TIMEOUT_MS);
}
async function qobuzProxyAny(endpoint, params, redis) {
  for (const inst of QOBUZ_INSTANCES) {
    const ctrl  = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 7000);
    try {
      const u = new URL(inst + endpoint);
      if (params) Object.entries(params).forEach(([k, v]) => u.searchParams.set(k, String(v)));
      const r = await fetch(u.toString(), { headers: { 'User-Agent': UA, 'Accept': 'application/json' }, signal: ctrl.signal });
      clearTimeout(timer);
      if (!r.ok) { try { await r.arrayBuffer(); } catch {} throw new Error('HTTP ' + r.status); }
      return await r.json();
    } catch(e) { clearTimeout(timer); continue; }
  }
  throw new Error('All Qobuz instances failed for ' + endpoint);
}

// ─── HiFi helpers ─────────────────────────────────────────────────────────────
async function fetchHifi(path, instances, redis) {
  const inst = await getWorkingHiFiInstance(instances, redis);
  if (!inst) throw new Error('No working HiFi instance found');
  return httpGet(inst + path, null, TIMEOUT_MS);
}
async function fetchHifiAny(path, instances, redis) {
  const list = (instances && instances.length) ? instances : DEFAULT_HIFI_INSTANCES;
  for (const inst of list) {
    try { return await httpGet(inst + path, null, 6000); } catch {}
  }
  throw new Error('All HiFi instances failed for: ' + path);
}

// ─── Text normalization ────────────────────────────────────────────────────────
function normalizeStr(s) {
  return String(s || '').toLowerCase()
    .replace(/[àáâãäå]/g,'a').replace(/[èéêë]/g,'e').replace(/[ìíîï]/g,'i')
    .replace(/[òóôõö]/g,'o').replace(/[ùúûü]/g,'u').replace(/[ý]/g,'y')
    .replace(/[ñ]/g,'n').replace(/[ç]/g,'c')
    .replace(/[^a-z0-9\s]/g,' ').replace(/\s+/g,' ').trim();
}
const FEAT_RE = /\s*(\(|\[)?\s*(feat\.?|ft\.?|featuring)\s+[^\)\]]*\s*(\)|\])?/gi;
function removeFeat(s) { return String(s||'').replace(FEAT_RE,'').trim(); }
function cleanTitle(t) { return t ? removeFeat(t) : 'Unknown'; }
function formatQuery(q) {
  q = q.replace(/['\u2018\u2019\u0060\u00B4]/g,"'").replace(/[\u201C\u201D\u00AB\u00BB]/g,'"');
  q = removeFeat(q);
  const parts = q.split('-');
  if (parts.length > 1) return parts.map(p => removeFeat(p.trim())).join(' - ');
  return removeFeat(q);
}

// ─── Scoring engine ────────────────────────────────────────────────────────────
function findBestMatch(items, query) {
  let bestItem = null, bestScore = -1;
  const qNorm = normalizeStr(query);
  const hasHyphen = qNorm.includes('-');
  let qTitleOnly = hasHyphen ? qNorm.split('-')[1].trim() : qNorm;
  if (hasHyphen && qNorm.split('-')[0].trim() === '') qTitleOnly = qNorm;
  const qWords = qNorm.replace(/[^a-z0-9\s]/gi,' ').split(/\s+/).filter(w => w.length > 1);
  for (let i = 0; i < Math.min(items.length, 50); i++) {
    const t = items[i];
    const tTitle  = normalizeStr(cleanTitle(t.title || ''));
    const tArtist = normalizeStr(t.performer?.name || t.artist?.name || t.artists?.[0]?.name || '');
    let score = 0;
    score += qWords.filter(w => (tTitle+' '+tArtist).includes(w)).length * 10;
    let titleMatch = false, artistMatch = false;
    if (hasHyphen) {
      const [p1,p2] = qNorm.split('-').map(p=>p.trim());
      if (p1&&(tTitle===p1||tTitle.includes(p1)||p1.includes(tTitle))) titleMatch=true;
      if (p2&&(tTitle===p2||tTitle.includes(p2)||p2.includes(tTitle))) titleMatch=true;
      if (p1&&(tArtist===p1||tArtist.includes(p1)||p1.includes(tArtist))) artistMatch=true;
      if (p2&&(tArtist===p2||tArtist.includes(p2)||p2.includes(tArtist))) artistMatch=true;
    } else {
      if (tTitle&&(qNorm===tTitle||qNorm.includes(tTitle)||tTitle.includes(qNorm))) titleMatch=true;
      if (tArtist&&(qNorm===tArtist||qNorm.includes(tArtist)||tArtist.includes(qNorm))) artistMatch=true;
    }
    if (titleMatch)  score += 40;
    if (artistMatch) score += 40;
    if (titleMatch && artistMatch) score += 100;
    if (tTitle===qTitleOnly||tTitle===qNorm) score += 60;
    const twm = qWords.filter(w=>tTitle.includes(w)).length;
    if (twm===0&&tTitle!==qTitleOnly&&!qNorm.includes(tTitle)) {
      if (qNorm!==tArtist&&!tArtist.includes(qNorm)) score -= 100;
    }
    if (!/\b(cover|karaoke|tribute|instrumental|8-bit)\b/i.test(qNorm) &&
        /\b(cover|karaoke|tribute|instrumental|8-bit)\b/i.test(t.title||'')) score -= 500;
    if (score > bestScore) { bestScore = score; bestItem = t; }
  }
  return { item: bestItem, score: bestScore };
}

// ─── ISRC resolution ──────────────────────────────────────────────────────────
async function getIsrcFromTidal(query, hifiInstances, redis) {
  try {
    const data  = await fetchHifi('/search/?s=' + encodeURIComponent(query) + '&limit=30', hifiInstances, redis);
    const items = data?.data?.items || data?.data?.tracks?.items || data?.tracks?.items || data?.data || [];
    const arr   = Array.isArray(items) ? items : (Array.isArray(data) ? data : []);
    const match = findBestMatch(arr, query);
    if (match.item && match.score >= 50) {
      let isrc = match.item.isrc;
      if (!isrc) { try { const info = await fetchHifi('/info/?id='+encodeURIComponent(match.item.id), hifiInstances, redis); isrc = (info?.data||info||{}).isrc; } catch {} }
      return { isrc, track: match.item, source: 'tidal', score: match.score };
    }
    return null;
  } catch { return null; }
}
async function getIsrcFromDeezer(query) {
  try {
    const data = await httpGet(DEEZER_API + '/search/track', { q: query, limit: 25 }, TIMEOUT_MS);
    if (!data?.data?.length) return null;
    const match = findBestMatch(data.data, query);
    if (match.item && match.score >= 50) {
      let isrc = match.item.isrc;
      if (!isrc) { try { isrc = (await httpGet(DEEZER_API+'/track/'+match.item.id,null,5000)).isrc; } catch {} }
      return { isrc, track: match.item, source: 'deezer', score: match.score };
    }
    return null;
  } catch { return null; }
}

// ─── Quality map ──────────────────────────────────────────────────────────────
const QUALITY_FORMAT_MAP = { MP3: 5, CD: 6, HIRES_96: 7, HIRES: 27 };
function qobuzQualityLabel(formatId, item) {
  if (formatId === 5)  return '320kbps MP3';
  if (formatId === 6)  return '16-bit / 44.1 kHz FLAC';
  if (formatId === 7)  return '24-bit / 96 kHz FLAC';
  if (formatId === 27) {
    const bits = item?.bit_depth || item?.maximum_bit_depth || 24;
    const rate = item?.sampling_rate || item?.maximum_sampling_rate || 192;
    return bits + '-bit / ' + rate + ' kHz FLAC';
  }
  return '16-bit / 44.1 kHz FLAC';
}
function tidalQualityLabel(q) {
  if (q==='HIGH') return '320kbps AAC (Tidal)';
  if (q==='LOW')  return '96kbps AAC (Tidal)';
  return q + ' (Tidal)';
}

// ─── Mappers ──────────────────────────────────────────────────────────────────
function mapQobuzTrack(t, userQuality) {
  if (!t?.id) return null;
  const formatId = QUALITY_FORMAT_MAP[String(userQuality).toUpperCase()] || 27;
  return {
    id:           String(t.id),
    title:        cleanTitle(t.title),
    artist:       t.performer?.name || t.artist?.name || t.album?.artist?.name || 'Unknown Artist',
    album:        t.album?.title || '',
    albumId:      t.album?.id ? String(t.album.id) : null,
    artworkURL:   t.album?.image?.large || t.album?.image?.thumbnail || t.album?.cover || null,
    duration:     t.duration || 0,
    trackNumber:  t.track_number || 0,
    discNumber:   t.media_number || t.volume_number || 1,
    audioQuality: qobuzQualityLabel(formatId, t),
    isrc:         t.isrc || undefined,
    format:       formatId === 5 ? 'mp3' : 'flac',
  };
}
function coverUrl(uuid, size) {
  if (!uuid) return null;
  const s = String(uuid);
  if (s.startsWith('http')) return s;
  return 'https://resources.tidal.com/images/' + s.replace(/-/g,'/') + '/' + (size||320) + 'x' + (size||320) + '.jpg';
}
function trackArtist(t) {
  if (!t) return 'Unknown';
  if (t.artists?.length) return t.artists.map(a=>a.name).join(', ');
  return t.artist?.name || t.performer?.name || 'Unknown';
}
function mapHifiTrack(t, tidalQuality) {
  if (!t?.id) return null;
  return {
    id:           'hifi:' + t.id,
    title:        cleanTitle(t.title),
    artist:       trackArtist(t),
    album:        t.album?.title || '',
    albumId:      t.album?.id ? 'hifi:' + t.album.id : null,
    artworkURL:   coverUrl(t.album?.cover, 1280),
    duration:     t.duration || 0,
    trackNumber:  t.trackNumber || 0,
    discNumber:   t.volumeNumber || 1,
    audioQuality: tidalQualityLabel(tidalQuality),
    format:       'aac',
  };
}
function mapQobuzAlbum(a) {
  return {
    id:         String(a.id),
    title:      a.title || 'Unknown',
    artist:     a.artist?.name || 'Unknown Artist',
    artworkURL: a.image?.large || a.image?.thumbnail || null,
    year:       a.release_date_original?.substring(0,4) || a.release_date?.substring(0,4) || null,
    trackCount: a.tracks_count || 0,
  };
}

// ─── Search Tracks — returns full list (up to limit) ─────────────────────────
async function searchTracks(query, limit, context) {
  const cleanedQuery  = formatQuery(query);
  const userQuality   = context?.settings?.quality?.value      || 'HIRES';
  const tidalQuality  = context?.settings?.tidalQuality?.value || 'HIGH';
  const hifiInstances = context?.settings?.hifiInstances?.value || DEFAULT_HIFI_INSTANCES;
  const redis         = context?.redis;
  const maxResults    = limit || 20;

  // Phase 1 — broad Qobuz search (fastest, most results)
  let qobuzTracks = [];
  try {
    const res = await qobuzProxyGet('/search', { q: cleanedQuery, limit: maxResults }, redis);
    const items = res?.tracks?.items || res?.data?.tracks?.items || [];
    qobuzTracks = items.map(t => mapQobuzTrack(t, userQuality)).filter(Boolean);
  } catch {}

  // Phase 2 — ISRC enrichment in parallel
  const [tidalMaster, deezerMaster] = await Promise.all([
    getIsrcFromTidal(cleanedQuery, hifiInstances, redis),
    getIsrcFromDeezer(cleanedQuery),
  ]);
  let bestMaster = null;
  if (tidalMaster && deezerMaster) {
    bestMaster = tidalMaster.score >= deezerMaster.score ? tidalMaster : deezerMaster;
  } else {
    bestMaster = tidalMaster || deezerMaster;
  }
  const masterIsrc = bestMaster?.isrc || null;
  let isrcTrack = null;
  if (masterIsrc) {
    try {
      const res = await qobuzProxyGet('/search', { q: masterIsrc, limit: 1 }, redis);
      const items = res?.tracks?.items || res?.data?.tracks?.items || [];
      if (items.length > 0) isrcTrack = mapQobuzTrack(items[0], userQuality);
    } catch {}
  }

  // Build final list: ISRC best match first, then rest deduplicated
  const finalTracks = [];
  if (isrcTrack) {
    finalTracks.push(isrcTrack);
    for (const t of qobuzTracks) {
      if (finalTracks.length >= maxResults) break;
      if (t.id !== isrcTrack.id) finalTracks.push(t);
    }
  } else {
    finalTracks.push(...qobuzTracks.slice(0, maxResults));
  }

  // Phase 3 — HiFi fallback only if Qobuz returned nothing at all
  if (qobuzTracks.length === 0 && finalTracks.length === 0 && bestMaster) {
    if (bestMaster.source === 'tidal') {
      const t = mapHifiTrack(bestMaster.track, tidalQuality);
      if (t) { t.audioQuality = tidalQualityLabel(tidalQuality) + ' · Fallback'; finalTracks.push(t); }
    } else {
      try {
        const hd = await fetchHifi('/search/?s=' + encodeURIComponent(cleanTitle(bestMaster.track.title)+' '+(bestMaster.track.artist?.name||'')) + '&limit=20', hifiInstances, redis);
        const items = hd?.data?.items || hd?.data?.tracks?.items || hd?.data || [];
        (Array.isArray(items)?items:[]).slice(0,maxResults).forEach(item => { const t=mapHifiTrack(item,tidalQuality); if(t) { t.audioQuality = tidalQualityLabel(tidalQuality) + ' · Fallback'; finalTracks.push(t); } });
      } catch {}
    }
  }

  // Phase 4 — raw HiFi search as last resort
  if (finalTracks.length === 0) {
    try {
      const hd = await fetchHifi('/search/?s=' + encodeURIComponent(cleanedQuery) + '&limit=' + maxResults, hifiInstances, redis);
      const items = hd?.data?.items || hd?.data?.tracks?.items || hd?.data || [];
      (Array.isArray(items)?items:[]).slice(0,maxResults).forEach(item => { const t=mapHifiTrack(item,tidalQuality); if(t) { t.audioQuality = tidalQualityLabel(tidalQuality) + ' · Fallback'; finalTracks.push(t); } });
    } catch {}
  }

  if (finalTracks.length > 0) return { tracks: finalTracks, total: finalTracks.length };
  throw new Error('Track not found.');
}

// ─── Search Albums ────────────────────────────────────────────────────────────
async function searchAlbums(query, limit, context) {
  const redis   = context?.redis;
  const results = [];
  try {
    const res = await qobuzProxyGet('/search', { q: query, limit: limit || 10 }, redis);
    const items = res?.albums?.items || res?.data?.albums?.items || [];
    results.push(...items.map(mapQobuzAlbum));
  } catch {}
  try {
    const hd = await fetchHifi('/search/?s=' + encodeURIComponent(query) + '&limit=10', context?.settings?.hifiInstances?.value || DEFAULT_HIFI_INSTANCES, redis);
    const albums = hd?.data?.albums?.items || hd?.albums?.items || [];
    if (Array.isArray(albums)) {
      for (const a of albums) {
        if (!a?.id) continue;
        results.push({ id:'hifi:'+String(a.id), title:a.title||'Unknown', artist:a.artist?.name||'Unknown', artworkURL:coverUrl(a.cover,1280), year:a.releaseDate?String(a.releaseDate).slice(0,4):undefined, trackCount:a.numberOfTracks||0 });
      }
    }
  } catch {}
  return { albums: results.slice(0, limit || 15), total: results.length };
}

// ─── Search Artists ───────────────────────────────────────────────────────────
async function searchArtists(query, limit, context) {
  const redis   = context?.redis;
  const results = [];
  try {
    const res = await qobuzProxyGet('/search', { q: query, limit: limit || 8 }, redis);
    const items = res?.artists?.items || res?.data?.artists?.items || [];
    results.push(...items.map(a => ({ id:String(a.id), name:a.name||'Unknown', artworkURL:a.picture||a.image?.large||null })));
  } catch {}
  try {
    const hd = await fetchHifi('/search/?s=' + encodeURIComponent(query) + '&limit=10', context?.settings?.hifiInstances?.value || DEFAULT_HIFI_INSTANCES, redis);
    const artists = hd?.data?.artists?.items || hd?.artists?.items || [];
    if (Array.isArray(artists)) {
      for (const a of artists) {
        if (!a?.id) continue;
        results.push({ id:'hifi:'+String(a.id), name:a.name||'Unknown', artworkURL:coverUrl(a.picture,480) });
      }
    }
  } catch {}
  return { artists: results.slice(0, limit || 10), total: results.length };
}

// ─── Search Playlists ─────────────────────────────────────────────────────────
async function searchPlaylists(query, limit, context) {
  try {
    const res   = await qobuzProxyGet('/search', { q: query, limit: limit || 8 }, context?.redis);
    const items = res?.playlists?.items || res?.data?.playlists?.items || [];
    return {
      playlists: items.map(p => ({
        id:         String(p.id),
        title:      p.name || p.title || 'Playlist',
        creator:    p.owner?.name || undefined,
        artworkURL: p.images300?.[0] || p.image_rectangle_mini?.[0] || p.image_rectangle?.[0] || null,
        trackCount: p.tracks_count || 0,
      })),
      total: items.length,
    };
  } catch { return { playlists: [], total: 0 }; }
}

// ─── Get Album ────────────────────────────────────────────────────────────────
async function getAlbum(albumId, context) {
  const redis = context?.redis;
  const userQ = context?.settings?.quality?.value || 'HIRES';

  if (!albumId.startsWith('hifi:')) {
    let res = null;
    const tries = [
      ['/album/' + albumId, null],
      ['/album/get', { album_id: albumId, limit: 100 }],
    ];
    for (const [ep, params] of tries) {
      try {
        res = await qobuzProxyAny(ep, params, redis);
        if (res && (res.id || res.title)) break;
      } catch {}
    }
    if (!res || (!res.id && !res.title)) throw new Error('Album not found on Qobuz');
    const tracks = (res.tracks?.items || [])
      .map(t => { if (!t.album) t.album = { id: albumId, title: res.title, artist: res.artist, image: res.image }; return mapQobuzTrack(t, userQ); })
      .filter(Boolean)
      .sort((a,b) => a.discNumber !== b.discNumber ? a.discNumber - b.discNumber : a.trackNumber - b.trackNumber);
    return {
      id:         String(albumId),
      title:      res.title || 'Unknown',
      artist:     res.artist?.name || (typeof res.artist === 'string' ? res.artist : 'Unknown'),
      artworkURL: res.image?.large || res.image?.thumbnail || null,
      year:       res.release_date_original?.substring(0,4) || res.release_date?.substring(0,4) || null,
      trackCount: tracks.length,
      tracks,
    };
  }

  // HiFi album
  const realId    = albumId.replace('hifi:', '');
  const tidalQ    = context?.settings?.tidalQuality?.value || 'HIGH';
  const instances = context?.settings?.hifiInstances?.value || DEFAULT_HIFI_INSTANCES;
  let data = null;
  const settled = await Promise.allSettled([
    fetchHifiAny('/album/?id=' + encodeURIComponent(realId) + '&limit=100', instances, redis),
    fetchHifiAny('/album/' + encodeURIComponent(realId) + '?limit=100', instances, redis),
    fetchHifiAny('/albums/' + encodeURIComponent(realId) + '/items', instances, redis),
  ]);
  for (const r of settled) { if (r.status==='fulfilled' && r.value) { data=r.value; break; } }
  if (!data) throw new Error('Album not found on any HiFi instance');

  const album    = data?.data?.album || data?.data || data?.album || data;
  let rawItems   = album?.items || album?.tracks?.items || album?.tracks || data?.items || data?.data?.items || [];
  if (!Array.isArray(rawItems)) rawItems = [];
  const artistName = album?.artist?.name || album?.artists?.map?.(a=>a.name).join(', ') || 'Unknown';
  const cover      = album?.cover || album?.image || album?.artwork;
  const tracks     = rawItems.map((item, i) => {
    const t = item?.item || item;
    if (!t?.id) return null;
    cacheTrackMeta(t.id, cleanTitle(t.title), trackArtist(t)||artistName, t.isrc||null);
    return { id:'hifi:'+String(t.id), title:cleanTitle(t.title), artist:trackArtist(t)||artistName, duration:t.duration||0, trackNumber:t.trackNumber||i+1, discNumber:t.volumeNumber||1, artworkURL:coverUrl(cover,1280), audioQuality:tidalQualityLabel(tidalQ), format:'aac' };
  }).filter(Boolean);
  return {
    id:         albumId,
    title:      album?.title || 'Unknown',
    artist:     artistName,
    artworkURL: coverUrl(cover, 1280),
    year:       album?.releaseDate ? String(album.releaseDate).slice(0, 4) : undefined,
    trackCount: tracks.length,
    tracks,
  };
}

// ─── Get Artist ───────────────────────────────────────────────────────────────
async function getArtist(artistId, context) {
  const redis = context?.redis;
  const userQ = context?.settings?.quality?.value || 'HIRES';

  if (!artistId.startsWith('hifi:')) {
    let a = {}, alb = {};
    const infoTries = [
      ['/artist/' + artistId, null],
      ['/artist/get', { artist_id: artistId }],
    ];
    for (const [ep, params] of infoTries) {
      try {
        a = await qobuzProxyAny(ep, params, redis);
        if (a && (a.id || a.name)) break;
      } catch {}
    }
    const albumTries = [
      ['/artist/' + artistId + '/albums', null],
      ['/artist/get', { artist_id: artistId, extra: 'albums' }],
    ];
    for (const [ep, params] of albumTries) {
      try {
        alb = await qobuzProxyAny(ep, params, redis);
        if (alb) break;
      } catch {}
    }
    const albumItems = a?.albums?.items || alb?.albums?.items || alb?.items || [];
    let trackItems = a?.tracks?.items || [];
    if (!trackItems.length) {
      try {
        const tr = await qobuzProxyGet('/search', { q: a.name || artistId, limit: 15 }, redis);
        trackItems = tr?.tracks?.items || tr?.data?.tracks?.items || [];
      } catch {}
    }
    return {
      id:        String(artistId),
      name:      a.name || 'Unknown',
      artworkURL: a.picture || a.image?.large || null,
      bio:       a.biography?.content || null,
      albums:    albumItems.map(mapQobuzAlbum),
      topTracks: trackItems.slice(0,15).map(t=>mapQobuzTrack(t,userQ)).filter(Boolean),
    };
  }

  // HiFi artist
  const realId    = artistId.replace('hifi:', '');
  const tidalQ    = context?.settings?.tidalQuality?.value || 'HIGH';
  const instances = context?.settings?.hifiInstances?.value || DEFAULT_HIFI_INSTANCES;
  const [infoRes, albumsRes, topRes, albumsRes2] = await Promise.allSettled([
    fetchHifiAny('/artist/?id=' + realId, instances, redis),
    fetchHifiAny('/artist/albums/?id=' + realId + '&limit=50', instances, redis),
    fetchHifiAny('/artist/toptracks/?id=' + realId + '&limit=20', instances, redis),
    fetchHifiAny('/artist/' + realId + '/albums?limit=50', instances, redis),
  ]);
  const safeD = r => { if (r.status!=='fulfilled') return {}; const v=r.value; return v?.data?.data||v?.data||v||{}; };
  const infoD    = safeD(infoRes);
  const albumsD  = safeD(albumsRes);
  const topD     = safeD(topRes);
  const albumsD2 = safeD(albumsRes2);
  const artistInfo = infoD?.artist?.id ? infoD.artist : (infoD?.id ? infoD : {});
  const artistName = artistInfo.name || 'Unknown';
  const _albumMap = {};
  for (const a of [...(albumsD?.items || albumsD?.albums?.items || albumsD?.albums || []),
                    ...(albumsD2?.items || albumsD2?.albums?.items || albumsD2?.albums || [])]) {
    if (a?.id) _albumMap[String(a.id)] = a;
  }
  const albumItems = Object.values(_albumMap);
  let trackItems = topD?.items || topD?.tracks?.items || topD?.tracks || [];
  if (!trackItems.length && artistName !== 'Unknown') {
    try {
      const sr = await fetchHifiAny('/search/?s=' + encodeURIComponent(artistName) + '&limit=30', instances, redis);
      const sItems = sr?.data?.items || sr?.data?.tracks?.items || sr?.items || [];
      const want = artistName.toLowerCase();
      trackItems = (Array.isArray(sItems) ? sItems : []).filter(t => {
        if (!t?.id) return false;
        const arts = t.artists || (t.artist ? [t.artist] : []);
        return arts.some(a => (a.name||'').toLowerCase().includes(want) || want.includes((a.name||'').toLowerCase()));
      }).slice(0, 20);
    } catch {}
  }
  const albums = (Array.isArray(albumItems)?albumItems:[]).map(al => ({
    id:'hifi:'+String(al.id), title:al.title||'Unknown', artist:artistName,
    artworkURL:coverUrl(al.cover,1280), trackCount:al.numberOfTracks,
    year:al.releaseDate?String(al.releaseDate).slice(0,4):undefined,
  }));
  const topTracks = (Array.isArray(trackItems)?trackItems:[]).map(t => {
    if (!t?.id) return null;
    cacheTrackMeta(t.id, cleanTitle(t.title), trackArtist(t)||artistName, t.isrc||null);
    return { id:'hifi:'+String(t.id), title:cleanTitle(t.title), artist:trackArtist(t)||artistName, artworkURL:coverUrl(t.album?.cover,1280), duration:t.duration||0, audioQuality:tidalQualityLabel(tidalQ), format:'aac' };
  }).filter(Boolean);
  return {
    id:        artistId,
    name:      artistName,
    artworkURL: coverUrl(artistInfo.picture, 480),
    bio:       null,
    albums,
    topTracks,
  };
}

// ─── Get Playlist ─────────────────────────────────────────────────────────────
async function getPlaylist(playlistId, context) {
  const redis = context?.redis;
  const userQ = context?.settings?.quality?.value || 'HIRES';
  let res = null;
  const tries = [
    ['/playlist/' + playlistId, null],
    ['/playlist/get', { playlist_id: playlistId, limit: 100 }],
  ];
  for (const [ep, params] of tries) {
    try {
      res = await qobuzProxyAny(ep, params, redis);
      if (res && (res.id || res.name || res.title)) break;
    } catch {}
  }
  if (!res?.id && !res?.name && !res?.title) throw new Error('Playlist not found on Qobuz');
  const tracks = (res?.tracks?.items || []).map(t => mapQobuzTrack(t, userQ)).filter(Boolean);
  return {
    id:         String(playlistId),
    title:      res.name || res.title || 'Playlist',
    creator:    res.owner?.name || undefined,
    artworkURL: res.images300?.[0] || res.image_rectangle_mini?.[0] || null,
    trackCount: res.tracks_count || tracks.length,
    tracks,
  };
}

// ─── Stream ───────────────────────────────────────────────────────────────────
async function getTrackStreamUrl(trackId, preferredQuality, context) {
  if (!trackId) throw new Error('Invalid track ID');
  const qualitySetting = context?.settings?.quality?.value || preferredQuality || 'HIRES';
  const tidalQuality   = context?.settings?.tidalQuality?.value || 'HIGH';
  const hifiInstances  = context?.settings?.hifiInstances?.value || DEFAULT_HIFI_INSTANCES;
  const env   = context?.env;
  const redis = context?.redis;
  const formatId = QUALITY_FORMAT_MAP[String(qualitySetting).toUpperCase()] || 27;

  if (trackId.startsWith('hifi:')) {
    const realId = trackId.replace('hifi:', '');
    const inst   = await getWorkingHiFiInstance(hifiInstances, redis);
    if (!inst) throw new Error('No working HiFi instance');
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
    try {
      const r    = await fetch(`${inst}/track/?id=${encodeURIComponent(realId)}&quality=${tidalQuality}`, { headers: { 'User-Agent': UA }, signal: ctrl.signal });
      clearTimeout(timer);
      const data = await r.json();
      const b64  = data?.data?.manifest || data?.manifest;
      if (!b64) throw new Error('Manifest missing');
      const manifest  = JSON.parse(atob(b64));
      const streamUrl = manifest?.urls?.[0];
      if (!streamUrl) throw new Error('No URL in manifest');
      return { url: streamUrl, format: 'aac', quality: tidalQualityLabel(tidalQuality) };
    } catch (e) { clearTimeout(timer); throw new Error('HiFi track not playable: ' + e.message); }
  }

  // Qobuz native stream
  const appId     = env?.QOBUZ_APP_ID     || APP_ID;
  const userToken = env?.QOBUZ_USER_TOKEN || USER_TOKEN;
  const secret    = env?.QOBUZ_SECRET     || SECRET;

  const streamKey = `qt:stream:${trackId}:${formatId}`;
  const cached    = await rGet(redis, streamKey);
  if (cached) { const r = typeof cached === 'string' ? JSON.parse(cached) : cached; if (r?.streamUrl) return r; }

  const ts  = Math.floor(Date.now() / 1000);
  const sig = md5('trackgetFileUrlformat_id' + formatId + 'intentstreamtrack_id' + trackId + ts + secret);
  const url = `https://www.qobuz.com/api.json/0.2/track/getFileUrl?app_id=${appId}&user_auth_token=${userToken}&track_id=${trackId}&format_id=${formatId}&intent=stream&request_ts=${ts}&request_sig=${sig}`;
  const r   = await fetch(url);
  if (!r.ok) throw new Error('Qobuz stream HTTP ' + r.status);
  const data = await r.json();
  if (!data?.url) throw new Error('URL stream missing for track ' + trackId);
  const result = {
    url: data.url,
    format: formatId === 5 ? 'mp3' : 'flac',
    quality: qobuzQualityLabel(formatId, data),
    expiresAt: ts + 3600,
  };
  await rSet(redis, streamKey, result, STREAM_TTL);
  return result;
}

// ─── Config page HTML ─────────────────────────────────────────────────────────
function buildConfigPage(baseUrl, redisConnected) {
  const maskUrl = u => {
    const pre = 'https://';
    if (u.startsWith(pre)) { const rest = u.slice(pre.length); return pre + rest.slice(0,8) + '\u2022'.repeat(Math.max(4, rest.length-8)); }
    return u.slice(0,12) + '\u2022'.repeat(6);
  };
  const HIFI_MASKED  = DEFAULT_HIFI_INSTANCES.map(maskUrl);
  const QOBUZ_MASKED = QOBUZ_INSTANCES.map(maskUrl);

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Qobuz + Tidal \u2014 Eclipse Addon</title>
  <style>
    *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
    body{background:#080808;color:#e0e0e0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:48px 20px 64px;-webkit-font-smoothing:antialiased}
    .card{background:#111;border:1px solid #1e1e1e;border-radius:18px;padding:36px;max-width:540px;width:100%;box-shadow:0 24px 64px rgba(0,0,0,.6);margin-bottom:20px}
    h1{font-size:22px;font-weight:700;margin-bottom:6px;color:#fff}
    h2{font-size:16px;font-weight:700;margin-bottom:14px;color:#fff}
    p.sub{font-size:14px;color:#666;margin-bottom:20px;line-height:1.6}
    .tip{background:#0a0a0a;border:1px solid #1e1e1e;border-radius:10px;padding:12px 14px;margin-bottom:20px;font-size:12px;color:#888;line-height:1.7}
    .tip b{color:#ccc}
    .pills{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:24px}
    .pill{border-radius:20px;font-size:11px;font-weight:600;padding:4px 10px;background:#181818;color:#aaa;border:1px solid #2a2a2a}
    .pill.hi{background:#0d1520;color:#4a9eff;border-color:#1a3050}
    .lbl{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:#444;margin-bottom:8px;margin-top:16px}
    .hint{font-size:12px;color:#3a3a3a;margin-bottom:12px;line-height:1.7}
    .ql-row{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:6px}
    .ql-btn{flex:1;cursor:pointer;border:1px solid #2a2a2a;border-radius:10px;background:#0a0a0a;color:#555;font-size:12px;font-weight:700;padding:10px 6px;text-align:center;transition:all .15s;letter-spacing:.04em;min-width:80px}
    .ql-btn:hover{border-color:#444;color:#aaa}
    .ql-btn.sel{background:#0d1520;border-color:#4a9eff;color:#4a9eff}
    button{cursor:pointer;border:none;border-radius:10px;font-size:15px;font-weight:700;padding:13px;width:100%;margin-top:6px;margin-bottom:12px;transition:background .15s}
    .bw{background:#fff;color:#000}.bw:hover{background:#e0e0e0}.bw:disabled{background:#1e1e1e;color:#333;cursor:not-allowed}
    .bg{background:#141414;color:#e0e0e0;border:1px solid #2a2a2a}.bg:hover{background:#1e1e1e}
    .bd{background:#0f0f0f;color:#777;border:1px solid #1a1a1a;font-size:13px;padding:10px}.bd:hover{background:#1a1a1a;color:#fff}
    .box{background:#0a0a0a;border:1px solid #1a1a1a;border-radius:12px;padding:18px;margin-bottom:14px}
    .blbl{font-size:10px;color:#444;text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px}
    .burl{font-size:12px;color:#fff;word-break:break-all;font-family:"SF Mono","Fira Code","Courier New",monospace;margin-bottom:14px;line-height:1.5}
    hr{border:none;border-top:1px solid #161616;margin:24px 0}
    .steps{display:flex;flex-direction:column;gap:12px}
    .step{display:flex;gap:12px;align-items:flex-start}
    .sn{background:#161616;border:1px solid #222;border-radius:50%;width:26px;height:26px;min-width:26px;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:#555}
    .st{font-size:13px;color:#555;line-height:1.6}.st b{color:#999}
    .inst-list{display:flex;flex-direction:column;gap:6px;margin-top:10px}
    .inst{display:flex;align-items:center;gap:8px;font-size:12px;padding:8px 12px;background:#0a0a0a;border:1px solid #161616;border-radius:8px}
    .dot{width:7px;height:7px;border-radius:50%;background:#333;flex-shrink:0}.dot.ok{background:#4a9a4a}.dot.err{background:#c04040}
    .inst-url{flex:1;color:#666;font-family:"SF Mono","Fira Code","Courier New",monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px}
    footer{margin-top:32px;font-size:12px;color:#2a2a2a;text-align:center;line-height:1.8}
    .redis-ok{color:#4a9a4a;font-size:11px;margin-top:4px}
    .redis-mem{color:#555;font-size:11px;margin-top:4px}
    .gen-count{font-size:11px;color:#444;text-align:center;margin-bottom:4px}
    .q-badge{display:inline-block;margin-top:8px;padding:3px 10px;background:#0d1520;color:#4a9eff;border:1px solid #1a3050;border-radius:6px;font-size:11px;font-weight:700}
  </style>
</head>
<body>
  <svg width="52" height="52" viewBox="0 0 52 52" fill="none" style="margin-bottom:22px">
    <circle cx="26" cy="26" r="26" fill="#fff"/>
    <rect x="10" y="20" width="4" height="12" rx="2" fill="#000"/>
    <rect x="17" y="14" width="4" height="24" rx="2" fill="#000"/>
    <rect x="24" y="18" width="4" height="16" rx="2" fill="#000"/>
    <rect x="31" y="11" width="4" height="30" rx="2" fill="#000"/>
    <rect x="38" y="17" width="4" height="18" rx="2" fill="#000"/>
  </svg>

  <div class="card">
    <h1>Qobuz + Tidal for Eclipse</h1>
    <p class="sub">Parallel ISRC scoring engine via Tidal &amp; Deezer. Qobuz native streams up to 24-bit/192kHz. HiFi proxy fallback across ${DEFAULT_HIFI_INSTANCES.length} instances.</p>
    <div class="tip"><b>Select quality first.</b> Each click generates a new unique URL with that quality baked in &mdash; paste into Eclipse to install.</div>
    <div class="pills">
      <span class="pill">Tracks &middot; Albums &middot; Artists &middot; Playlists</span>
      <span class="pill hi">FLAC / Hi-Res</span>
      <span class="pill hi">Qobuz 24-bit</span>
      <span class="pill hi">Tidal Fallback</span>
    </div>
    ${redisConnected ? '<div class="redis-ok">&#10003; Redis connected &mdash; persistent caching active</div>' : '<div class="redis-mem">&#9679; In-memory cache (set UPSTASH vars for Redis)</div>'}

    <div class="lbl">Preferred Quality</div>
    <div class="ql-row">
      <div class="ql-btn" id="ql-HIRES" onclick="selectQuality('HIRES')">Hi-Res Max<br><span style="font-size:10px;opacity:.6">24-bit/192kHz</span></div>
      <div class="ql-btn" id="ql-HIRES_96" onclick="selectQuality('HIRES_96')">Hi-Res 96<br><span style="font-size:10px;opacity:.6">24-bit/96kHz</span></div>
      <div class="ql-btn" id="ql-CD" onclick="selectQuality('CD')">CD Quality<br><span style="font-size:10px;opacity:.6">16-bit/44.1kHz</span></div>
      <div class="ql-btn" id="ql-MP3" onclick="selectQuality('MP3')">320 kbps<br><span style="font-size:10px;opacity:.6">MP3</span></div>
    </div>
    <div class="hint" id="qlHint">No preference selected &mdash; defaults to Hi-Res Max.</div>

    <button class="bw" id="genBtn" onclick="generate()">Generate Addon URL</button>
    <div class="gen-count" id="genCount" style="display:none"></div>
    <div class="box" id="genBox" style="display:none">
      <div class="blbl">Your addon URL &mdash; paste into Eclipse</div>
      <div class="burl" id="genUrl"></div>
      <div id="qBadge"></div>
      <button class="bd" style="margin-top:12px" id="copyBtn" onclick="copyUrl()">Copy URL</button>
    </div>

    <hr>
    <div class="steps">
      <div class="step"><div class="sn">1</div><div class="st">Select a quality tier above, then click Generate</div></div>
      <div class="step"><div class="sn">2</div><div class="st">Open <b>Eclipse</b> &rarr; Settings &rarr; Connections &rarr; Add Connection &rarr; Addon</div></div>
      <div class="step"><div class="sn">3</div><div class="st">Paste your URL and tap Install</div></div>
      <div class="step"><div class="sn">4</div><div class="st">Search Qobuz catalog &mdash; Tidal fallback kicks in automatically</div></div>
    </div>
  </div>

  <div class="card">
    <h2>Instance Health</h2>
    <p class="sub" style="margin-bottom:14px">Live status of HiFi &amp; Qobuz instances. URLs are partially masked for privacy.</p>
    <div class="inst-list" id="instList"><div style="color:#333;font-size:13px">Checking&#8230;</div></div>
    <button class="bg" style="margin-top:14px" onclick="checkHealth()">Refresh Status</button>
  </div>

  <footer>Qobuz + Tidal Eclipse Addon &bull; ISRC Scoring Engine &bull; v1.4.0</footer>

  <script>
    var genUrlVal=null,genCount=0,selQ=null;
    const HIFI_MASKED=${JSON.stringify(HIFI_MASKED)};
    const QOBUZ_MASKED=${JSON.stringify(QOBUZ_MASKED)};
    const HIFI_REAL=${JSON.stringify(DEFAULT_HIFI_INSTANCES)};
    const QOBUZ_REAL=${JSON.stringify(QOBUZ_INSTANCES)};
    const QLABELS={'HIRES':'Hi-Res Max \u00b7 24-bit/192kHz','HIRES_96':'Hi-Res 96 \u00b7 24-bit/96kHz','CD':'CD Quality \u00b7 16-bit/44.1kHz','MP3':'320 kbps MP3'};
    function selectQuality(q){
      if(selQ===q)selQ=null;else selQ=q;
      ['HIRES','HIRES_96','CD','MP3'].forEach(function(k){var el=document.getElementById('ql-'+k);if(el)el.classList.toggle('sel',selQ===k);});
      document.getElementById('qlHint').textContent=selQ?'Selected: '+QLABELS[selQ]+' \u2014 baked into your URL.':'No preference selected \u2014 defaults to Hi-Res Max.';
    }
    function generate(){
      var btn=document.getElementById('genBtn');
      btn.disabled=true;btn.textContent='Generating\u2026';
      fetch('/generate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(selQ?{quality:selQ}:{})})
        .then(function(r){return r.json();}).then(function(d){
          if(d.error){alert(d.error);btn.disabled=false;btn.textContent='Generate Addon URL';return;}
          genUrlVal=d.manifestUrl;genCount++;
          document.getElementById('genUrl').textContent=genUrlVal;
          document.getElementById('genBox').style.display='block';
          document.getElementById('genCount').style.display='block';
          document.getElementById('genCount').textContent='Generated '+genCount+' URL'+(genCount!==1?'s':'')+' this session';
          document.getElementById('qBadge').innerHTML='<span class="q-badge">'+(selQ?QLABELS[selQ]:'Hi-Res Max \u00b7 24-bit/192kHz')+'</span>';
          document.getElementById('copyBtn').textContent='Copy URL';
          btn.disabled=false;btn.textContent='Generate Another URL';
        }).catch(function(e){alert('Error: '+e.message);btn.disabled=false;btn.textContent='Generate Addon URL';});
    }
    function copyUrl(){
      if(!genUrlVal)return;
      try{var ta=document.createElement('textarea');ta.value=genUrlVal;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();document.execCommand('copy');document.body.removeChild(ta);}
      catch(_){try{navigator.clipboard.writeText(genUrlVal);}catch(_2){}}
      var b=document.getElementById('copyBtn');b.textContent='Copied!';setTimeout(function(){b.textContent='Copy URL';},1500);
    }
    function checkHealth(){
      var list=document.getElementById('instList');
      list.innerHTML='<div style="color:#333;font-size:13px">Checking\u2026</div>';
      var allInst=HIFI_REAL.concat(QOBUZ_REAL);
      var allMask=HIFI_MASKED.concat(QOBUZ_MASKED);
      Promise.all(allInst.map(function(inst){
        return fetch(inst+'/search/?s=test&limit=1',{signal:AbortSignal.timeout(4000)})
          .then(function(r){return{ok:r.ok};}).catch(function(){return{ok:false};});
      })).then(function(results){
        list.innerHTML='';
        results.forEach(function(res,i){
          var row=document.createElement('div');row.className='inst';
          var dot=document.createElement('span');dot.className='dot'+(res.ok?' ok':' err');
          var urlSpan=document.createElement('span');urlSpan.className='inst-url';urlSpan.textContent=allMask[i];
          row.appendChild(dot);row.appendChild(urlSpan);list.appendChild(row);
        });
      });
    }
    checkHealth();
  <\/script>
</body>
</html>`;
}

// ─── Routes ───────────────────────────────────────────────────────────────────

app.get('/', async c => {
  const redis = getRedis(c.env);
  return new Response(buildConfigPage(
    (c.req.header('x-forwarded-proto')||'https')+'://'+c.req.header('host'),
    redis !== null
  ), { headers: { 'Content-Type': 'text/html; charset=utf-8' } });
});

app.post('/generate', async c => {
  const redis = getRedis(c.env);
  let body = {};
  try { body = await c.req.json(); } catch {}
  const quality = body?.quality || 'HIRES';
  const token   = generateToken();
  await saveToken(redis, token, { quality, createdAt: Date.now() });
  const base = (c.req.header('x-forwarded-proto')||'https')+'://'+c.req.header('host');
  return c.json({ token, quality, manifestUrl: base + '/u/' + token + '/manifest.json' });
});

app.get('/u/:token/manifest.json', async c => {
  const token  = c.req.param('token');
  const redis  = getRedis(c.env);
  let stored   = await loadToken(redis, token);
  if (!stored) { stored = { quality: 'HIRES', createdAt: Date.now() }; await saveToken(redis, token, stored); }
  return c.json({
    id:          'com.jacobyz211.qobuz-tidal-eclipse.' + token.slice(0,8),
    name:        'Qobuz + Tidal',
    version:     '1.4.0',
    description: 'Qobuz FLAC streams with ISRC scoring & Tidal HiFi fallback. Tracks, Albums, Artists, Playlists.',
    icon:        'https://upload.wikimedia.org/wikipedia/commons/thumb/9/9c/Qobuz_logo.svg/320px-Qobuz_logo.svg.png',
    resources:   ['search', 'stream', 'catalog'],
    types:       ['track', 'album', 'artist', 'playlist'],
    settings: {
      quality: {
        type: 'selector', label: 'Qobuz Audio Quality',
        options: [
          { label: 'MP3 320kbps',                value: 'MP3'      },
          { label: 'CD \u2014 FLAC 16-bit/44.1kHz',   value: 'CD'       },
          { label: 'Hi-Res \u2014 24-bit/96kHz',      value: 'HIRES_96' },
          { label: 'Hi-Res Max \u2014 24-bit/192kHz', value: 'HIRES'    },
        ],
        defaultValue: stored.quality || 'HIRES',
      },
      tidalQuality: {
        type: 'selector', label: 'Tidal Proxy Quality',
        options: [
          { label: 'High (AAC 320kbps)', value: 'HIGH' },
          { label: 'Low',                value: 'LOW'  },
        ],
        defaultValue: 'HIGH',
      },
    },
  });
});

app.get('/u/:token/search', async c => {
  const token  = c.req.param('token');
  const q      = (c.req.query('q') || '').trim();
  if (!q) return c.json({ tracks: [], albums: [], artists: [], playlists: [] });
  const redis   = getRedis(c.env);
  const stored  = await loadToken(redis, token) || { quality: 'HIRES' };
  const context = { env: c.env, redis, settings: { quality: { value: stored.quality }, tidalQuality: { value: 'HIGH' } } };
  const [tr, al, ar, pl] = await Promise.allSettled([
    searchTracks(q,    20, context),
    searchAlbums(q,    10, context),
    searchArtists(q,    8, context),
    searchPlaylists(q,  6, context),
  ]);
  return c.json({
    tracks:    tr.status==='fulfilled' ? tr.value.tracks    : [],
    albums:    al.status==='fulfilled' ? al.value.albums    : [],
    artists:   ar.status==='fulfilled' ? ar.value.artists   : [],
    playlists: pl.status==='fulfilled' ? pl.value.playlists : [],
  });
});

app.get('/u/:token/stream/:id', async c => {
  const token  = c.req.param('token');
  const redis  = getRedis(c.env);
  const stored = await loadToken(redis, token) || { quality: 'HIRES' };
  try {
    const result = await getTrackStreamUrl(c.req.param('id'), stored.quality, { env:c.env, redis, settings:{ quality:{value:stored.quality}, tidalQuality:{value:'HIGH'} } });
    return c.json(result);
  } catch (e) { return c.json({ error: e.message }, 500); }
});

app.get('/u/:token/album/:id', async c => {
  const token  = c.req.param('token');
  const redis  = getRedis(c.env);
  const stored = await loadToken(redis, token) || { quality: 'HIRES' };
  try {
    const data = await getAlbum(c.req.param('id'), { env:c.env, redis, settings:{ quality:{value:stored.quality}, tidalQuality:{value:'HIGH'} } });
    return c.json(data);
  } catch (e) { return c.json({ error: e.message }, 500); }
});

app.get('/u/:token/artist/:id', async c => {
  const token  = c.req.param('token');
  const redis  = getRedis(c.env);
  const stored = await loadToken(redis, token) || { quality: 'HIRES' };
  try {
    const data = await getArtist(c.req.param('id'), { env:c.env, redis, settings:{ quality:{value:stored.quality}, tidalQuality:{value:'HIGH'} } });
    return c.json(data);
  } catch (e) { return c.json({ error: e.message }, 500); }
});

app.get('/u/:token/playlist/:id', async c => {
  const token  = c.req.param('token');
  const redis  = getRedis(c.env);
  const stored = await loadToken(redis, token) || { quality: 'HIRES' };
  try {
    const data = await getPlaylist(c.req.param('id'), { env:c.env, redis, settings:{ quality:{value:stored.quality} } });
    return c.json(data);
  } catch (e) { return c.json({ error: e.message }, 500); }
});

// Legacy flat routes
app.get('/manifest.json', c => c.json({
  id:'com.jacobyz211.qobuz-tidal-eclipse', name:'Qobuz + Tidal', version:'1.4.0',
  description:'Qobuz FLAC streams with ISRC scoring & Tidal HiFi fallback',
  icon:'https://upload.wikimedia.org/wikipedia/commons/thumb/9/9c/Qobuz_logo.svg/320px-Qobuz_logo.svg.png',
  resources:['search','stream','catalog'], types:['track','album','artist','playlist'],
}));

app.get('/search', async c => {
  const q = (c.req.query('q')||'').trim();
  if (!q) return c.json({ tracks:[], albums:[], artists:[], playlists:[] });
  const redis = getRedis(c.env);
  const context = { env:c.env, redis };
  const [tr,al,ar,pl] = await Promise.allSettled([searchTracks(q,20,context),searchAlbums(q,10,context),searchArtists(q,8,context),searchPlaylists(q,6,context)]);
  return c.json({ tracks:tr.status==='fulfilled'?tr.value.tracks:[], albums:al.status==='fulfilled'?al.value.albums:[], artists:ar.status==='fulfilled'?ar.value.artists:[], playlists:pl.status==='fulfilled'?pl.value.playlists:[] });
});

app.get('/stream/:id', async c => {
  const redis = getRedis(c.env);
  try { return c.json(await getTrackStreamUrl(c.req.param('id'), c.req.query('quality')||'HIRES', { env:c.env, redis })); }
  catch (e) { return c.json({ error: e.message }, 500); }
});
app.get('/album/:id', async c => {
  const redis = getRedis(c.env);
  try { return c.json(await getAlbum(c.req.param('id'), { env:c.env, redis })); }
  catch (e) { return c.json({ error: e.message }, 500); }
});
app.get('/artist/:id', async c => {
  const redis = getRedis(c.env);
  try { return c.json(await getArtist(c.req.param('id'), { env:c.env, redis })); }
  catch (e) { return c.json({ error: e.message }, 500); }
});
app.get('/playlist/:id', async c => {
  const redis = getRedis(c.env);
  try { return c.json(await getPlaylist(c.req.param('id'), { env:c.env, redis })); }
  catch (e) { return c.json({ error: e.message }, 500); }
});

app.get('/health', async c => {
  const redis    = getRedis(c.env);
  const qobuzInst = await getWorkingQobuzInstance(redis).catch(()=>null);
  const hifiInst  = await getWorkingHiFiInstance(DEFAULT_HIFI_INSTANCES, redis).catch(()=>null);
  return c.json({ status:(!!qobuzInst||!!hifiInst)?'ok':'degraded', qobuz:!!qobuzInst, hifi:!!hifiInst, redis:redis!==null, version:'1.4.0' });
});

export default app;
