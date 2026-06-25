import { Hono } from 'hono';
import { cors } from 'hono/cors';
import axios from 'axios';
import crypto from 'crypto';

const app = new Hono();

app.use('*', cors());

async function parseBody(c) {
try { return await c.req.json(); } catch(e) { return {}; }
}

const HIFI_INSTANCES = [
'https://hifi-api1.onrender.com',
'https://hifi-api-bffw.onrender.com',
];
let activeInstance = HIFI_INSTANCES[0];
let instanceHealthy = false;
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36';
const QOBUZ_INSTANCES = [
'https://qobuz-api1.onrender.com',
'https://qobuz-api.stremio123.duckdns.org',
];
let activeQobuzInstance = QOBUZ_INSTANCES[0];

// ─── In-memory track meta cache (title+artist by TIDAL id) ───────────────────
// Populated at search time, read at stream time. Survives within the same worker instance.
const TRACK_META_CACHE = new Map();

function cacheTrackMeta(id, title, artist, isrc) {
if (!id || !title) return;
TRACK_META_CACHE.set(String(id), { title, artist: artist || 'Unknown', isrc: isrc || null });
// cap size to avoid unbounded growth in long-lived instances
if (TRACK_META_CACHE.size > 5000) {
const firstKey = TRACK_META_CACHE.keys().next().value;
TRACK_META_CACHE.delete(firstKey);
}
}

function getCachedMeta(id) {
return TRACK_META_CACHE.get(String(id)) || null;
}

// Qobuz Track ID cache: TIDAL track id -> Qobuz track object
// Populated at search pre-warm, read in stream route to skip qobuzFindBestTrack entirely.
const QOBUZ_TRACK_ID_CACHE = new Map();
function cacheQobuzTrackId(tidalId, qobuzTrack) {
  if (!tidalId || !qobuzTrack || !qobuzTrack.id) return;
  QOBUZ_TRACK_ID_CACHE.set(String(tidalId), qobuzTrack);
  if (QOBUZ_TRACK_ID_CACHE.size > 5000) {
    const firstKey = QOBUZ_TRACK_ID_CACHE.keys().next().value;
    QOBUZ_TRACK_ID_CACHE.delete(firstKey);
  }
  // Also persist to Redis so other CF Worker isolates benefit from this pre-warm
  redisSaveQobuzId(tidalId, qobuzTrack).catch(() => {});
}
function getCachedQobuzTrack(tidalId) {
  return QOBUZ_TRACK_ID_CACHE.get(String(tidalId)) || null;
}

// ─── Unified in-memory TTL cache ─────────────────────────────────────────────
const _cache = new Map();
function cGet(key) {
  const v = _cache.get(key);
  if (!v) return null;
  if (v.exp && v.exp < Date.now()) { _cache.delete(key); return null; }
  return v.val;
}
function cSet(key, val, ttlSec) {
  _cache.set(key, { val, exp: ttlSec ? Date.now() + ttlSec * 1000 : null });
  if (_cache.size > 2000) {
    let del = Math.floor(_cache.size * 0.2);
    for (const k of _cache.keys()) { if (del-- <= 0) break; _cache.delete(k); }
  }
}

// ─── Inflight deduplication ───────────────────────────────────────────────────
// Two simultaneous requests for the same stream share ONE outbound call.
const _inflight = new Map();
async function dedupeCall(key, fn) {
  if (_inflight.has(key)) return _inflight.get(key);
  const p = Promise.resolve().then(fn).finally(() => _inflight.delete(key));
  _inflight.set(key, p);
  return p;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
function coverUrl(uuid, size) {
if (!uuid) return undefined;
var s = String(uuid);
if (s.startsWith('http')) return s;
size = size || 320;
return 'https://resources.tidal.com/images/' + s.replace(/-/g, '/') + '/' + size + 'x' + size + '.jpg';
}

function trackDuration(t) { return (t && t.duration) ? Math.floor(t.duration) : undefined; }
function trackArtist(t) {
if (!t) return 'Unknown';
if (t.artists && t.artists.length) return t.artists.map(function(a) { return a.name; }).join(', ');
if (t.artist && t.artist.name) return t.artist.name;
return 'Unknown';
}

function decodeManifest(manifest) {
try {
const raw = Buffer.from(manifest, 'base64').toString('utf8');
if (raw.trimStart().startsWith('<')) {
const urlMatch = raw.match(/<BaseURL[^>]*>([^<]+)<\/BaseURL>/i)
|| raw.match(/<SegmentURL[^>]+media="([^"]+)"/i);
if (urlMatch && urlMatch[1]) {
const url = urlMatch[1].trim()
.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"');
const codec = raw.match(/codecs="([^"]+)"/i)?.[1] || 'flac';
return { url, codec, isDash: true };
}
return null;
}
const decoded = JSON.parse(raw);
const url = (decoded.urls && decoded.urls.length > 0) ? decoded.urls[0] : (decoded.url || null);
const codec = decoded.codecs || decoded.codec || decoded.mimeType || null;
return { url, codec, isDash: false };
} catch(e) { return null; }
}

function isPlaylistUUID(id) {
return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(String(id));
}

function looksLikePlaylist(p) {
if (!p || !p.title) return false;
if (p.trackNumber !== undefined) return false;
if (p.replayGain !== undefined) return false;
if (p.peak !== undefined) return false;
if (p.isrc !== undefined) return false;
if (p.audioQuality !== undefined) return false;
return !!(p.uuid || p.creator || p.squareImage || p.numberOfTracks !== undefined);
}

function artistRelevance(name, query) {
var n = (name || '').toLowerCase().trim();
var q = (query || '').toLowerCase().trim();
if (n === q) return 4;
if (n.startsWith(q) || q.startsWith(n)) return 3;
if (n.includes(q) || q.includes(n)) return 2;
return 0;
}

// ─── Qobuz credentials (direct API — no proxy needed) ────────────────────────
const QOBUZ_APP_ID    = '312369995';
const QOBUZ_USER_TOKEN = '3GrgNWkOKdBMmMrEGnY_IS6Zksf0NXLJo_-QTcFeugsSvryxHinS3MKDpogrTtmb8HE9bSaog3jefTRvLmO6DA';
const QOBUZ_SECRET    = 'e79f8b9be485692b0e5f9dd895826368';

// Format ID map — same as QTE
const QOBUZ_FORMAT_MAP = {
  'HIMAX':    27,  // 24-bit / up to 192kHz
  'HI96':     7,   // 24-bit / up to 96kHz
  'LOSSLESS': 6,   // 16-bit / 44.1kHz FLAC
  'AAC320':   5,   // 320 kbps MP3
  'HI_RES_LOSSLESS': 27,
  'HIRESLOSSLESS':   27,
};

function md5(str) {
  return crypto.createHash('md5').update(str).digest('hex');
}

// qobuzQualityLabel — mirrors QTE exactly
function qobuzQualityLabel(formatId, data) {
  const sr = data?.sampling_rate || 0;
  const bd = data?.bit_depth      || 0;
  // Use actual sample rate + bit depth from Qobuz API when available — this is what Eclipse displays
  if (bd > 0 && sr > 0) {
    const srLabel = sr >= 1000 ? (sr / 1000).toFixed(0) + ' kHz' : sr + ' kHz';
    return bd + '-bit / ' + srLabel;
  }
  // Fallback to format tier labels when API doesn't return sr/bd
  if (formatId === 27) return '24-bit / 192 kHz';
  if (formatId === 7)  return '24-bit / 96 kHz';
  if (formatId === 6)  return '16-bit / 44.1 kHz FLAC';
  if (formatId === 5)  return '320 kbps AAC';
  return 'unknown';
}

function qobuzTrackQualityScore(t) {
  if (!t) return -1;
  const bd = Number(t.bit_depth || 0);
  const sr = Number(t.maximum_sampling_rate || t.sampling_rate || 0);
  const hires = !!(t.hires || t.hires_streamable || bd > 16 || sr > 48);
  const purchasable = t.streamable !== false && t.displayable !== false;
  let score = 0;
  if (purchasable) score += 1000;
  if (hires) score += 500;
  score += bd * 20;
  score += sr;
  if (t.version) score -= 10;
  return score;
}

function qobuzPickBestEdition(items, wantTitle, wantArtist, wantIsrc) {
  const norm = s => String(s || '').toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
  const titleNeedle = norm(removeFeat(wantTitle || ''));
  const artistNeedle = norm(wantArtist || '');
  const isrcNeedle = String(wantIsrc || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  let best = null, bestScore = -1e9;
  for (const t of (items || [])) {
    if (!t || !t.id) continue;
    const tTitle = norm(removeFeat(t.title || ''));
    const tArtist = norm(trackArtist(t));
    const tIsrc = String(t.isrc || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
    let score = qobuzTrackQualityScore(t);
    if (isrcNeedle && tIsrc === isrcNeedle) score += 10000;
    if (titleNeedle && tTitle === titleNeedle) score += 1000;
    else if (titleNeedle && tTitle.includes(titleNeedle)) score += 400;
    if (artistNeedle && tArtist.includes(artistNeedle)) score += 500;
    if (titleNeedle && tTitle && !tTitle.includes(titleNeedle) && !titleNeedle.includes(tTitle)) score -= 1500;
    if (artistNeedle && tArtist && !tArtist.includes(artistNeedle) && !artistNeedle.includes(tArtist)) score -= 500;
    if (score > bestScore) { best = t; bestScore = score; }
  }
  return best;
}

// ─── Qobuz client — direct signed API call (replaces proxy racing) ────────────
// Mirrors QTE's getTrackStreamUrl Qobuz path exactly:
//   ts   = Math.floor(Date.now() / 1000)
//   sig  = md5('trackgetFileUrlformat_id' + fmt + 'intentstreamtrack_id' + id + ts + secret)
//   hits https://www.qobuz.com/api.json/0.2/track/getFileUrl directly
// Stream URLs cached 28 min (Qobuz URLs expire ~30 min).
// Falls back through format priority order if a tier returns an error.
async function qobuzStream(trackId, prefKey) {
  if (prefKey === 'AAC96') return null; // TIDAL-only tier

  const cacheKey = 'qstream:' + trackId + ':' + (prefKey || 'auto');
  const cached = cGet(cacheKey);
  if (cached) return cached;

  // Determine format priority order based on prefKey (same as old proxy version)
  const PREF_FMT_ORDER = {
    'HIMAX':    [27, 7, 6, 5],
    'HI96':     [7, 27, 6, 5],
    'LOSSLESS': [6, 7, 27, 5],
    'AAC320':   [5],
    'HI_RES_LOSSLESS': [27, 7, 6, 5],
    'HIRESLOSSLESS':   [27, 7, 6, 5],
  };
  const fmtOrder = (prefKey && PREF_FMT_ORDER[prefKey]) || [27, 7, 6, 5];
  const fmtLabel = { 27: 'flac', 7: 'flac', 6: 'flac', 5: 'aac' };

  // Try each format in priority order — exactly like QTE's loop
  for (const fmt of fmtOrder) {
    try {
      const ts  = Math.floor(Date.now() / 1000);
      const sig = md5('trackgetFileUrlformat_id' + fmt + 'intentstreamtrack_id' + trackId + ts + QOBUZ_SECRET);
      const url = 'https://www.qobuz.com/api.json/0.2/track/getFileUrl'
        + '?app_id='          + QOBUZ_APP_ID
        + '&user_auth_token=' + QOBUZ_USER_TOKEN
        + '&track_id='        + trackId
        + '&format_id='       + fmt
        + '&intent=stream'
        + '&request_ts='      + ts
        + '&request_sig='     + sig;

      const res = await fetch(url, { headers: { 'User-Agent': UA }, signal: AbortSignal.timeout(8000) });
      if (!res.ok) continue; // try next format
      const data = await res.json();
      if (!data?.url) continue;

      const result = {
        url:       data.url,
        format:    fmtLabel[fmt] || 'flac',
        quality:   qobuzQualityLabel(fmt, data),
        source:    'qobuz',
        expiresAt: Math.floor(Date.now() / 1000) + 1680, // 28 min
      };
      cSet(cacheKey, result, 1680);
      return result;
    } catch (e) {
      continue; // network error — try next format
    }
  }

  return null; // all formats failed
}

// ─── Search scoring engine (ported from 8spine V2.0 Strict) ──────────────────
// Scores each candidate track against the search query.
// Returns { item, score } — caller picks item if score >= threshold.
// Anti-cover/karaoke penalty (-500) prevents junk results swamping real tracks.
// Title guillotine (-100) kills results with zero query-word overlap.
function normalizeStr(s) {
  return String(s || '').toLowerCase()
    .replace(/[''`´]/g, "'").replace(/[""«»]/g, '"')
    .replace(/\s+/g, ' ').trim();
}

function removeFeat(s) {
  if (!s) return '';
  const m = String(s).search(/\s+[\(\[](feat|ft|with|vs)[\.\s]/i);
  return (m > 0 ? s.substring(0, m) : s).trim();
}

function scoringFindBest(items, query, knownArtist) {
  let bestItem  = null;
  let bestScore = -1;

  const qNorm      = normalizeStr(query);
  const hasHyphen  = / - /.test(qNorm); // only space-hyphen-space, not word-internal hyphens
  const qWords     = qNorm.replace(/[^a-z0-9\s]/gi, ' ').split(/\s+/).filter(w => w.length > 1);
  const knownArtistNorm = knownArtist ? normalizeStr(knownArtist) : null;

  // For non-hyphen queries, try to isolate the title portion
  // by removing known-artist words from the query word list
  const knownArtistWords = knownArtistNorm
    ? knownArtistNorm.replace(/[^a-z0-9\s]/gi, ' ').split(/\s+/).filter(w => w.length > 1)
    : [];
  const titleOnlyWords = knownArtistWords.length
    ? qWords.filter(w => !knownArtistWords.includes(w))
    : qWords;

  let qLeft = qNorm, qRight = '';
  if (hasHyphen) {
    const parts = qNorm.split(' - ').map(p => p.trim());
    qLeft = parts[0]; qRight = parts[1] || '';
  }

  for (let i = 0; i < Math.min(items.length, 50); i++) {
    const t       = items[i];
    const tTitle  = normalizeStr(removeFeat(t.title || ''));
    const tArtist = normalizeStr(
      t.performer?.name || t.artist?.name || t.artists?.[0]?.name ||
      (t.artists && t.artists.length ? t.artists[0].name : '') || ''
    );
    let score = 0;

    const targetStr  = (tTitle + ' ' + tArtist).replace(/[^a-z0-9\s]/gi, ' ');
    const matchCount = qWords.filter(w => targetStr.includes(w)).length;
    score += matchCount * 10;

    let titleMatch = false, artistMatch = false;

    if (hasHyphen) {
      if (qLeft.length  && (tTitle  === qLeft  || tTitle.includes(qLeft)   || qLeft.includes(tTitle)))   titleMatch  = true;
      if (qRight.length && (tTitle  === qRight || tTitle.includes(qRight)  || qRight.includes(tTitle)))  titleMatch  = true;
      if (qLeft.length  && (tArtist === qLeft  || tArtist.includes(qLeft)  || qLeft.includes(tArtist)))  artistMatch = true;
      if (qRight.length && (tArtist === qRight || tArtist.includes(qRight) || qRight.includes(tArtist))) artistMatch = true;
    } else {
      // titleMatch: only award if the track title contains words that are TITLE-EXCLUSIVE
      // (i.e. words that are NOT part of the known artist name).
      // This prevents "Dead Butterflies" by Architects from getting titleMatch
      // when query is "dead butterflies embers" and we know artist is "Dead Butterflies".
      const titleHitsTitleOnly = titleOnlyWords.filter(w => tTitle.includes(w)).length;
      const titleHitsAll       = qWords.filter(w => tTitle.includes(w)).length;
      if (titleOnlyWords.length > 0) {
        // We have title-exclusive words — require at least one to be in tTitle
        if (titleHitsTitleOnly > 0 && (qNorm === tTitle || qNorm.includes(tTitle) || tTitle.includes(qNorm))) titleMatch = true;
        if (tTitle === qNorm) titleMatch = true;
      } else {
        // No title-only words (all query words are artist words) — use loose match
        if (tTitle.length && (qNorm === tTitle || qNorm.includes(tTitle) || tTitle.includes(qNorm))) titleMatch = true;
      }
      if (tArtist.length && (qNorm === tArtist || qNorm.includes(tArtist) || tArtist.includes(qNorm))) artistMatch = true;
    }

    if (titleMatch)  score += 40;
    if (artistMatch) score += 40;
    if (titleMatch && artistMatch) score += 100;

    // Exact title bonus
    if (!hasHyphen && (tTitle === qNorm || (titleOnlyWords.length && titleOnlyWords.every(w => tTitle.includes(w)) && tTitle.split(' ').length <= titleOnlyWords.length + 1))) score += 60;
    if (hasHyphen && (tTitle === qLeft || tTitle === qRight)) score += 60;

    // Title guillotine — kills results with no query word in the title
    const titleWordsMatch = qWords.filter(w => tTitle.includes(w)).length;
    if (titleWordsMatch === 0 && tTitle !== qNorm) {
      if (qNorm !== tArtist && !tArtist.includes(qNorm)) score -= 100;
    }

    // KEY: if we know the artist and this track's artist doesn't match → penalize heavily
    if (knownArtistNorm && tArtist && !tArtist.includes(knownArtistNorm) && !knownArtistNorm.includes(tArtist)) {
      score -= 150;
    }

    // Anti-cover/karaoke spam
    if (!/\b(cover|karaoke|tribute|instrumental|8-bit)\b/i.test(qNorm) &&
         /\b(cover|karaoke|tribute|instrumental|8-bit)\b/i.test(t.title || '')) {
      score -= 500;
    }

    if (score > bestScore) { bestScore = score; bestItem = t; }
  }
  return { item: bestItem, score: bestScore };
}

// ─── ISRC resolution: TIDAL + Deezer in parallel ─────────────────────────────
// Replaces the old single-source qobuzFindByIsrc sequential loop.
// Both engines run simultaneously — whichever scores higher wins.
// ISRC confirmed match cached 24h; miss cached 30 min.
const DEEZER_API = 'https://api.deezer.com';
const ISRC_MIN_SCORE = 100; // minimum scoring engine threshold for a valid match

async function getIsrcFromTidal(query, instanceUrl, knownArtist) {
  try {
    const data = await hifiGetForTokenSafe(instanceUrl, '/search', { s: query, limit: 20 });
    let items = data?.tracks?.items || data?.items || data?.data?.items ||
                data?.data?.tracks?.items || (Array.isArray(data) ? data : []);
    if (!items.length) return null;
    const match = scoringFindBest(items, query, knownArtist);
    if (!match.item || match.score < ISRC_MIN_SCORE) return null;
    const track = match.item;
    let isrc = track.isrc;
    if (!isrc) {
      const info = await hifiGetForTokenSafe(instanceUrl, '/info', { id: track.id });
      isrc = info?.isrc || info?.data?.isrc || null;
    }
    console.log('[isrc] TIDAL hit score=' + match.score + ' isrc=' + isrc + ' for: ' + query);
    return { isrc, track, source: 'tidal', score: match.score };
  } catch(e) { return null; }
}

async function getIsrcFromDeezer(query, knownArtist) {
  try {
    const r = await axios.get(DEEZER_API + '/search/track', {
      params: { q: query },
      headers: { 'User-Agent': UA },
      timeout: 8000
    });
    const items = r.data?.data || [];
    if (!items.length) return null;
    const match = scoringFindBest(items, query, knownArtist);
    if (!match.item || match.score < ISRC_MIN_SCORE) return null;
    const track = match.item;
    let isrc = track.isrc;
    if (!isrc) {
      try {
        const r2 = await axios.get(DEEZER_API + '/track/' + track.id, {
          headers: { 'User-Agent': UA }, timeout: 5000
        });
        isrc = r2.data?.isrc || null;
      } catch(e) {}
    }
    console.log('[isrc] Deezer hit score=' + match.score + ' isrc=' + isrc + ' for: ' + query);
    return { isrc, track, source: 'deezer', score: match.score };
  } catch(e) { return null; }
}

// resolveIsrc: race TIDAL and Deezer in parallel, pick the higher-scoring winner.
// Returns { isrc, track, source, score } or null.
async function resolveIsrc(title, artist, instanceUrl) {
  const query = (artist ? artist + ' ' : '') + removeFeat(title || '');
  const cacheKey = 'isrc2:' + query.toLowerCase();
  const cached = cGet(cacheKey);
  if (cached === 'MISS') return null;
  if (cached) return cached;

  const [tidalResult, deezerResult] = await Promise.all([
    getIsrcFromTidal(query, instanceUrl, artist),
    getIsrcFromDeezer(query, artist)
  ]);

  let winner = null;
  if (tidalResult && deezerResult) {
    winner = tidalResult.score >= deezerResult.score ? tidalResult : deezerResult;
    console.log('[isrc] Winner: ' + winner.source.toUpperCase() +
      ' (' + winner.score + ' vs ' + (winner === tidalResult ? deezerResult.score : tidalResult.score) + ')');
  } else {
    winner = tidalResult || deezerResult;
  }

  if (winner) {
    // Sanity: winner track title must share at least one word with the searched title
    // prevents "Dead Butterflies" (artist) matching the song "Dead Butterflies" by another artist
    const _norm = s => String(s||'').toLowerCase().replace(/[^a-z0-9\s]/g,' ').trim();
    const _qWords = _norm(removeFeat(title||'')).split(/\s+/).filter(w => w.length > 1);
    const _wTitle = _norm(winner.track?.title || winner.track?.name || '');
    const _overlap = _qWords.filter(w => _wTitle.includes(w)).length;
    if (_qWords.length > 0 && _overlap === 0) {
      console.log('[isrc] title mismatch rejected: query="' + title + '" winner="' + (winner.track?.title||'?') + '" — caching MISS');
      cSet(cacheKey, 'MISS', 1800);
      return null;
    }
    cSet(cacheKey, winner, 86400); // cache 24h
    return winner;
  }
  cSet(cacheKey, 'MISS', 1800); // miss cached 30 min
  return null;
}

// qobuzFindByIsrc: looks up a Qobuz track by confirmed ISRC via direct Qobuz API.
// Uses /track/search?query=ISRC — no proxy hop, no sequential fallback.
// Hit cached 24h, miss cached 30 min.
async function qobuzFindByIsrc(isrc, wantTitle = null, wantArtist = null) {
  if (!isrc) return null;
  const normIsrc = s => String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  const wantIsrc = normIsrc(isrc);
  if (!wantIsrc) return null;

  const cacheKey = 'qisrc:' + wantIsrc;
  const cached = cGet(cacheKey);
  if (cached === 'MISS') return null;
  if (cached) return cached;

  try {
    const r = await fetch(
      'https://www.qobuz.com/api.json/0.2/track/search'
        + '?app_id='          + QOBUZ_APP_ID
        + '&user_auth_token=' + QOBUZ_USER_TOKEN
        + '&query='           + encodeURIComponent(wantIsrc)
        + '&limit=20',
      { headers: { 'User-Agent': UA }, signal: AbortSignal.timeout(6000) }
    );
    if (r.ok) {
      const data = await r.json();
      const items = (data?.tracks?.items || []).filter(t => t && t.isrc && normIsrc(t.isrc) === wantIsrc);
      const match = qobuzPickBestEdition(items, wantTitle, wantArtist, wantIsrc);
      if (match && match.id) {
        cSet(cacheKey, match, 86400);
        console.log('[qobuz] isrc HIT', isrc, '->', match.id, match.title, '|', 'bd=' + (match.bit_depth || '?'), 'sr=' + (match.maximum_sampling_rate || match.sampling_rate || '?'));
        return match;
      }
    }
  } catch(e) {}

  cSet(cacheKey, 'MISS', 1800);
  return null;
}


// qobuzFindBestTrack: upgraded 4-tier lookup.
// Tier 1: resolve ISRC via parallel TIDAL+Deezer engines → Qobuz ISRC lookup (exact match).
// Tier 2: Qobuz title+artist search using scoring engine (was simple includes check).
// Tier 3: Qobuz raw text search (last resort fallback).
// All tiers cached to avoid redundant outbound calls.
async function qobuzFindBestTrack(title, artist, isrc, instanceUrl) {
  let isrcCandidate = null;
  let titleCandidate = null;

  if (isrc) {
    isrcCandidate = await qobuzFindByIsrc(isrc, title, artist);
    if (!isrcCandidate) console.log('[qobuz] bare ISRC miss for', isrc, '— trying parallel resolution');
  }

  if (!isrcCandidate && title) {
    const resolved = await resolveIsrc(title, artist, instanceUrl);
    if (resolved && resolved.isrc) {
      isrcCandidate = await qobuzFindByIsrc(resolved.isrc, title, artist);
      if (!isrcCandidate) console.log('[qobuz] resolved ISRC', resolved.isrc, 'not in Qobuz vault — falling to title search');
    }
  }

  if (title) {
    const cacheKey = 'qmatch2:' + (title || '').toLowerCase() + ':' + (artist || '').toLowerCase();
    const cached = cGet(cacheKey);
    if (cached !== 'MISS' && cached) titleCandidate = cached;
    else {
      const q = (artist ? artist + ' ' : '') + removeFeat(title);
      for (const inst of QOBUZ_INSTANCES) {
        try {
          const r = await axios.get(inst + '/search', {
            params: { q, limit: 15 },
            headers: { 'User-Agent': UA },
            timeout: 10000
          });
          const items = r.data?.tracks?.items || [];
          if (!items.length) continue;
          const match = scoringFindBest(items, (artist ? artist + ' ' : '') + title, artist);
          if (match.item && match.score >= 40) {
            if (inst !== activeQobuzInstance) activeQobuzInstance = inst;
            cSet(cacheKey, match.item, 3600);
            console.log('[qobuz] title search HIT score=' + match.score, match.item.title);
            titleCandidate = match.item;
            break;
          }
        } catch(e) { continue; }
      }
      if (!titleCandidate) cSet(cacheKey, 'MISS', 1800);
    }
  }

  if (isrcCandidate && titleCandidate) {
    // ISRC is an exact identifier — only allow title candidate to win if it is literally
    // the same Qobuz track (same id) but the ISRC lookup returned a lower-quality pressing.
    // Never let a different song override a confirmed ISRC match, even at higher quality.
    const isrcScore = qobuzTrackQualityScore(isrcCandidate);
    const titleScore = qobuzTrackQualityScore(titleCandidate);
    if (isrcCandidate.id === titleCandidate.id && titleScore > isrcScore) {
      // Same track, title search found a better pressing — use it
      console.log('[qobuz] same track, title pressing has higher quality', 'isrc=' + isrcScore, 'title=' + titleScore);
      return titleCandidate;
    }
    // Different tracks — ISRC always wins (it is the exact song by identifier)
    if (isrcCandidate.id !== titleCandidate.id && titleScore > isrcScore) {
      console.log('[qobuz] ISRC match wins over title candidate (different track)', 'isrc=' + isrcScore, 'title=' + titleScore, 'isrcId=' + isrcCandidate.id, 'titleId=' + titleCandidate.id);
    }
    return isrcCandidate;
  }

  return isrcCandidate || titleCandidate || null;
}


// ─── Hi-Fi API client ─────────────────────────────────────────────────────────
// Races ALL instances in parallel (Promise.any) — first success wins.
// Eliminates the sequential 15s-per-instance fallback that caused retry storms.
async function hifiGet(path, params) {
  const instances = instanceHealthy
    ? [activeInstance].concat(HIFI_INSTANCES.filter(i => i !== activeInstance))
    : HIFI_INSTANCES.slice();

  try {
    return await Promise.any(instances.map(inst =>
      axios.get(inst + path, {
        params,
        headers: { 'User-Agent': UA, 'Accept': 'application/json' },
        timeout: 8000
      }).then(r => {
        if (r.status === 200 && r.data) {
          if (inst !== activeInstance) { activeInstance = inst; instanceHealthy = true; }
          return r.data;
        }
        throw new Error('bad response from ' + inst);
      })
    ));
  } catch(e) {
    throw new Error('All Hi-Fi instances failed');
  }
}

async function hifiGetSafe(path, params) {
  try { return await hifiGet(path, params); } catch(e) { return null; }
}

async function hifiGetForToken(instanceUrl, path, params) {
  if (instanceUrl) {
    try {
      const r = await axios.get(instanceUrl + path, {
        params,
        headers: { 'User-Agent': UA, 'Accept': 'application/json' },
        timeout: 8000
      });
      if (r.status === 200 && r.data) return r.data;
      throw new Error('Non-200 from custom instance: ' + r.status);
    } catch(e) {
      throw new Error('Custom instance failed: ' + instanceUrl + ': ' + e.message);
    }
  }
  return hifiGet(path, params);
}

async function hifiGetForTokenSafe(instanceUrl, path, params) {
  try { return await hifiGetForToken(instanceUrl, path, params); } catch(e) { return null; }
}

// ─── Upstash Redis REST API ───────────────────────────────────────────────────
const UPSTASH_URL = typeof UPSTASH_REDIS_REST_URL !== 'undefined' ? UPSTASH_REDIS_REST_URL : null;
const UPSTASH_TOKEN = typeof UPSTASH_REDIS_REST_TOKEN !== 'undefined' ? UPSTASH_REDIS_REST_TOKEN : null;

async function upstashCmd(...args) {
if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
try {
const res = await fetch(UPSTASH_URL, {
method: 'POST',
headers: { 'Authorization': 'Bearer ' + UPSTASH_TOKEN, 'Content-Type': 'application/json' },
body: JSON.stringify(args)
});
const json = await res.json();
return json.result ?? null;
} catch(e) { return null; }
}

// Save title+artist+isrc to Redis keyed by TIDAL track id (TTL 24h)
async function redisCacheTrackMeta(tid, title, artist, isrc) {
if (!tid || !title) return;
await upstashCmd('SET', 'mc:tmeta:' + tid, JSON.stringify({ title, artist: artist || 'Unknown', isrc: isrc || null }), 'EX', 86400);
}

// Load title+artist+isrc from Redis by TIDAL track id
async function redisLoadTrackMeta(tid) {
const raw = await upstashCmd('GET', 'mc:tmeta:' + tid);
if (!raw) return null;
try { return JSON.parse(raw); } catch(e) { return null; }
}

// Persist Qobuz track ID mapping (TIDAL id -> Qobuz track obj) to Redis
// Survives across CF Worker isolates — fixes cross-isolate cache miss problem
async function redisSaveQobuzId(tidalId, qobuzTrack) {
  if (!tidalId || !qobuzTrack || !qobuzTrack.id) return;
  await upstashCmd('SET', 'mc:qid:' + tidalId,
    JSON.stringify({ id: qobuzTrack.id, title: qobuzTrack.title, bitdepth: qobuzTrack.bitdepth, maximumsamplingrate: qobuzTrack.maximumsamplingrate, samplingrate: qobuzTrack.samplingrate, hires: qobuzTrack.hires, hires_streamable: qobuzTrack.hires_streamable, streamable: qobuzTrack.streamable, displayable: qobuzTrack.displayable }),
    'EX', 86400);
}

async function redisLoadQobuzId(tidalId) {
  const raw = await upstashCmd('GET', 'mc:qid:' + tidalId);
  if (!raw) return null;
  try { return JSON.parse(raw); } catch(e) { return null; }
}

// Persist pre-warmed Qobuz stream URL to Redis so any isolate can serve it instantly
async function redisSaveQobuzStream(tidalId, prefKey, streamResult) {
  if (!tidalId || !streamResult) return;
  const k = 'mc:qstream:' + tidalId + ':' + (prefKey || 'auto');
  await upstashCmd('SET', k, JSON.stringify(streamResult), 'EX', 1680); // 28min = Qobuz URL lifetime
}

async function redisLoadQobuzStream(tidalId, prefKey) {
  const k = 'mc:qstream:' + tidalId + ':' + (prefKey || 'auto');
  const raw = await upstashCmd('GET', k);
  if (!raw) return null;
  try { return JSON.parse(raw); } catch(e) { return null; }
}

async function redisSave(token, entry) {
await upstashCmd('SET', 'mc:token:' + token, JSON.stringify({
createdAt: entry.createdAt,
lastUsed: entry.lastUsed,
reqCount: entry.reqCount || 0,
instanceUrl: entry.instanceUrl || null,
preferredQuality: entry.preferredQuality || null,
addonName: entry.addonName || null,
}), 'EX', 2592000);
}

async function redisLoad(token) {
const raw = await upstashCmd('GET', 'mc:token:' + token);
if (!raw) return null;
try {
const p = JSON.parse(raw);
return {
createdAt: p.createdAt || Date.now(),
lastUsed: p.lastUsed || Date.now(),
reqCount: p.reqCount || 0,
instanceUrl: p.instanceUrl || null,
preferredQuality: p.preferredQuality || null,
addonName: p.addonName || null,
};
} catch(e) { return null; }
}

// ─── Token auth ───────────────────────────────────────────────────────────────
const TOKEN_CACHE = new Map();
const IP_CREATES  = new Map();

// ─── Rate limit constants ─────────────────────────────────────────────────────
const MAX_TOKENS_PER_IP  = 3;        // reduced from 10 — prevents token farming
const MAX_GEN_PER_HOUR   = 5;        // max token generations per IP per hour
const RATE_MAX           = 80;       // general requests per token per minute
const RATE_WINDOW_MS     = 60000;

// Stream / download limits (per-token)
const STREAM_MAX_PER_MIN  = 15;      // max stream/download calls per token per minute
const STREAM_MAX_PER_HOUR = 300;     // hard session cap per token per hour (~5 albums)
const STREAM_WINDOW_MIN   = 60000;
const STREAM_WINDOW_HOUR  = 3600000;

// Bulk download guard — blocks insane playlist downloads (20k track attempts etc.)
const BULK_DL_MAX       = 500;       // max tracks per 10-minute window per token
const BULK_DL_WINDOW_MS = 600000;    // 10 minutes

// Search limits (per-token)
const SEARCH_MAX_PER_MIN = 20;
const SEARCH_WINDOW_MS   = 60000;

// Global daily budget — shared across ALL tokens / ALL addons on this worker
const GLOBAL_DAILY_LIMIT        = 85000; // conservative buffer below 100k/day cap
const GLOBAL_STREAM_DAILY_LIMIT = 60000; // 60k of daily cap reserved for streams
let   globalDailyCount    = 0;
let   globalStreamCount   = 0;
let   globalDayStart      = Date.now();

// Per-IP token generation hourly rate
const IP_GEN_RATE = new Map(); // ip -> { count, resetAt }

// Per-IP unauthenticated endpoint rate limit
const IP_UNAUTH_RATE = new Map(); // ip -> { count, resetAt }
const UNAUTH_MAX_PER_MIN = 10;

function generateToken() { return crypto.randomBytes(14).toString('hex'); }

function getOrCreateIpBucket(ip) {
  var now = Date.now();
  var b = IP_CREATES.get(ip);
  if (!b || now > b.resetAt) { b = { count: 0, resetAt: now + 86400000 }; IP_CREATES.set(ip, b); }
  return b;
}

// ─── Global daily budget helpers ──────────────────────────────────────────────
function consumeGlobalBudget() {
  const now = Date.now();
  if (now - globalDayStart > 86400000) {
    globalDailyCount = 0; globalStreamCount = 0; globalDayStart = now;
  }
  if (globalDailyCount >= GLOBAL_DAILY_LIMIT) return false;
  globalDailyCount++;
  return true;
}

function consumeGlobalStreamBudget() {
  const now = Date.now();
  if (now - globalDayStart > 86400000) {
    globalDailyCount = 0; globalStreamCount = 0; globalDayStart = now;
  }
  if (globalStreamCount >= GLOBAL_STREAM_DAILY_LIMIT) return false;
  globalStreamCount++;
  return true;
}

// ─── Per-token stream / download rate checker ──────────────────────────────────
// Uses escalating cooldown: first over-limit = 1hr wait, repeated abuse = 2hr then 4hr.
function checkStreamRateLimit(entry) {
  const now = Date.now();
  const cooldown = entry.streamCooldown || STREAM_WINDOW_HOUR;

  entry.streamWinMin  = (entry.streamWinMin  || []).filter(t => now - t < STREAM_WINDOW_MIN);
  entry.streamWinHour = (entry.streamWinHour || []).filter(t => now - t < cooldown);

  if (entry.streamWinMin.length  >= STREAM_MAX_PER_MIN)  return { ok: false, reason: 'stream_min' };
  if (entry.streamWinHour.length >= STREAM_MAX_PER_HOUR) {
    // Escalate: 1hr → 2hr → 4hr max
    entry.streamCooldown = Math.min((entry.streamCooldown || STREAM_WINDOW_HOUR) * 2, 14400000);
    return { ok: false, reason: 'stream_hour' };
  }

  // Reset escalation once they're under the limit again
  entry.streamCooldown = STREAM_WINDOW_HOUR;

  if (!consumeGlobalStreamBudget()) return { ok: false, reason: 'global_stream' };

  entry.streamWinMin.push(now);
  entry.streamWinHour.push(now);
  return { ok: true };
}

// ─── Bulk download / playlist guard ───────────────────────────────────────────
// Blocks anyone trying to download huge playlists (500+ tracks per 10min).
// 20k track download attempt → blocked after 500, then the window resets and
// blocks again — making it practically impossible to complete.
function checkBulkDownloadLimit(entry) {
  const now = Date.now();
  entry.bulkWin = (entry.bulkWin || []).filter(t => now - t < BULK_DL_WINDOW_MS);
  if (entry.bulkWin.length >= BULK_DL_MAX) return false;
  entry.bulkWin.push(now);
  return true;
}

// ─── Per-token search rate checker ────────────────────────────────────────────
function checkSearchRateLimit(entry) {
  const now = Date.now();
  entry.searchWin = (entry.searchWin || []).filter(t => now - t < SEARCH_WINDOW_MS);
  if (entry.searchWin.length >= SEARCH_MAX_PER_MIN) return false;
  entry.searchWin.push(now);
  return true;
}

// ─── Per-IP unauthenticated endpoint rate limiter ─────────────────────────────
function checkUnauthRateLimit(ip) {
  const now = Date.now();
  const b = IP_UNAUTH_RATE.get(ip) || { count: 0, resetAt: now + 60000 };
  if (now > b.resetAt) { b.count = 0; b.resetAt = now + 60000; }
  if (b.count >= UNAUTH_MAX_PER_MIN) return false;
  b.count++;
  IP_UNAUTH_RATE.set(ip, b);
  return true;
}

async function getTokenEntry(token) {
if (TOKEN_CACHE.has(token)) return TOKEN_CACHE.get(token);
var saved = await redisLoad(token);
if (saved) {
var entry = { createdAt: saved.createdAt, lastUsed: saved.lastUsed, reqCount: saved.reqCount, instanceUrl: saved.instanceUrl || null, preferredQuality: saved.preferredQuality || null, addonName: saved.addonName || null, rateWin: [] };
TOKEN_CACHE.set(token, entry);
return entry;
}
if (/^[a-f0-9]{28}$/.test(token)) {
var fresh = { createdAt: Date.now(), lastUsed: Date.now(), reqCount: 0, rateWin: [], instanceUrl: null, preferredQuality: null, addonName: null };
TOKEN_CACHE.set(token, fresh);
return fresh;
}
return null;
}

function checkRateLimit(entry) {
var now = Date.now();
entry.rateWin = (entry.rateWin || []).filter(function(t) { return now - t < RATE_WINDOW_MS; });
if (entry.rateWin.length >= RATE_MAX) return false;
entry.rateWin.push(now); entry.lastUsed = now; entry.reqCount = (entry.reqCount || 0) + 1; return true;
}

function getBaseUrl(req) { return (req.headers['x-forwarded-proto'] || req.protocol) + '://' + req.get('host'); }

// ─── withToken ────────────────────────────────────────────────────────────────
async function withToken(c, handler) {
const rawParam = c.req.param('token');
const { token, embeddedInstance } = parseTokenParam(rawParam);
const entry = await getTokenEntry(token);
if (!entry) return Response.json({ error: 'Invalid token.' }, { status: 404 });
// Global daily cap — protects the 100k/day limit across all tokens and all addons
if (!consumeGlobalBudget()) return Response.json({ error: 'Daily request limit reached. Service will resume tomorrow.' }, { status: 429 });
if (!checkRateLimit(entry)) return Response.json({ error: 'Rate limit exceeded. Max 80 requests per minute per token.' }, { status: 429 });
if (embeddedInstance) entry.instanceUrl = embeddedInstance;
if (entry.reqCount % 20 === 0) await redisSave(token, entry);
return handler(entry);
}

function parseTokenParam(rawParam) {
const parts = rawParam.split('~');
const token = parts[0];
let embeddedInstance = null, embeddedName = null;
try { if (parts[1]) embeddedInstance = Buffer.from(parts[1], 'base64url').toString('utf8'); } catch(e) {}
try { if (parts[2]) embeddedName = decodeURIComponent(Buffer.from(parts[2], 'base64url').toString('utf8')).slice(0, 40); } catch(e) {}
return { token, embeddedInstance, embeddedName };
}

// ─── Config page ──────────────────────────────────────────────────────────────
function buildConfigPage(baseUrl) {
var h = '';
h += '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">';
h += '<meta name="viewport" content="width=device-width,initial-scale=1">';
h += '<title>Claudo - TIDAL + Qobuz</title>';
h += '<style>';
h += '*{box-sizing:border-box;margin:0;padding:0}';
h += 'body{background:#080808;color:#e0e0e0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:48px 20px 64px}';
h += '.card{background:#111;border:1px solid #1e1e1e;border-radius:18px;padding:36px;max-width:540px;width:100%;box-shadow:0 24px 64px rgba(0,0,0,.6);margin-bottom:20px}';
h += 'h1{font-size:22px;font-weight:700;margin-bottom:6px;color:#fff}h2{font-size:16px;font-weight:700;margin-bottom:14px;color:#fff}';
h += 'p.sub{font-size:14px;color:#666;margin-bottom:20px;line-height:1.6}';
h += '.tip{background:#0a0a0a;border:1px solid #1e1e1e;border-radius:10px;padding:12px 14px;margin-bottom:20px;font-size:12px;color:#888;line-height:1.7}.tip b{color:#ccc}';
h += '.pills{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:24px}';
h += '.pill{border-radius:20px;font-size:11px;font-weight:600;padding:4px 10px;background:#181818;color:#aaa;border:1px solid #2a2a2a}';
h += '.pill.hi{background:#0d1520;color:#4a9eff;border-color:#1a3050}';
h += '.lbl{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:#444;margin-bottom:8px;margin-top:16px}';
h += 'input{width:100%;background:#0a0a0a;border:1px solid #1e1e1e;border-radius:10px;color:#e0e0e0;font-size:14px;padding:12px 14px;margin-bottom:6px;outline:none;transition:border-color .15s}';
h += 'input:focus{border-color:#fff}input::placeholder{color:#2e2e2e}';
h += '.hint{font-size:12px;color:#3a3a3a;margin-bottom:12px;line-height:1.7}';
h += 'button{cursor:pointer;border:none;border-radius:10px;font-size:15px;font-weight:700;padding:13px;width:100%;margin-top:6px;margin-bottom:12px;transition:background .15s}';
h += '.bw{background:#fff;color:#000}.bw:hover{background:#e0e0e0}.bw:disabled{background:#1e1e1e;color:#333;cursor:not-allowed}';
h += '.bg{background:#141414;color:#e0e0e0;border:1px solid #2a2a2a}.bg:hover{background:#1e1e1e}.bg:disabled{background:#0f0f0f;color:#333;cursor:not-allowed}';
h += '.bd{background:#0f0f0f;color:#777;border:1px solid #1a1a1a;font-size:13px;padding:10px}.bd:hover{background:#1a1a1a;color:#fff}';
h += '.box{display:none;background:#0a0a0a;border:1px solid #1a1a1a;border-radius:12px;padding:18px;margin-bottom:14px}';
h += '.blbl{font-size:10px;color:#444;text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px}';
h += '.burl{font-size:12px;color:#fff;word-break:break-all;font-family:"SF Mono","Fira Code",monospace;margin-bottom:14px;line-height:1.5}';
h += 'hr{border:none;border-top:1px solid #161616;margin:24px 0}';
h += '.steps{display:flex;flex-direction:column;gap:12px}.step{display:flex;gap:12px;align-items:flex-start}';
h += '.sn{background:#161616;border:1px solid #222;border-radius:50%;width:26px;height:26px;min-width:26px;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:#555}';
h += '.st{font-size:13px;color:#555;line-height:1.6}.st b{color:#999}';
h += '.warn{background:#0d0d0d;border:1px solid #1e1e1e;border-radius:10px;padding:14px;margin-top:20px;font-size:12px;color:#555;line-height:1.7}';
// Quality selector styles — two groups, single selection across both
h += '.ql-section{margin-bottom:4px}';
h += '.ql-group-label{font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px;margin-top:12px}';
h += '.ql-row{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:4px}';
h += '.ql-btn{flex:1;min-width:calc(50% - 4px);cursor:pointer;border:1px solid #2a2a2a;border-radius:12px;background:#0a0a0a;color:#555;font-size:12px;font-weight:700;padding:12px 8px;text-align:center;transition:all .15s;letter-spacing:.04em;line-height:1.4}';
h += '.ql-btn:hover{border-color:#444;color:#aaa}';
h += '.ql-btn.sel-q{background:#0d1520;border-color:#4a9eff;color:#4a9eff}';  // Qobuz selected
h += '.ql-btn.sel-t{background:#120d20;border-color:#9b4aff;color:#9b4aff}';  // TIDAL selected
h += '.ql-sub{font-size:10px;font-weight:400;opacity:.6;display:block;margin-top:2px}';
h += '.qual-badge{display:inline-flex;align-items:center;gap:6px;padding:6px 12px;border-radius:20px;font-size:12px;font-weight:600;margin-bottom:10px}';
h += '.qual-badge.qobuz{background:#0d1520;color:#4a9eff;border:1px solid #1a3050}';
h += '.qual-badge.tidal{background:#120d20;color:#9b4aff;border:1px solid #1a2a40}';
h += '.qual-badge.none{background:#111;color:#555;border:1px solid #1e1e1e}';
// Instance health
h += '.inst-list{display:flex;flex-direction:column;gap:6px;margin-top:10px}';
h += '.inst{display:flex;align-items:center;gap:8px;font-size:12px;padding:8px 12px;background:#0a0a0a;border:1px solid #161616;border-radius:8px}';
h += '.dot{width:7px;height:7px;border-radius:50%;background:#333;flex-shrink:0}.dot.ok{background:#4a9a4a}.dot.err{background:#c04040}';
h += '.inst-url{flex:1;color:#666;font-family:"SF Mono","Fira Code",monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}';
h += '.inst-ms{color:#444;margin-left:auto;font-size:11px}';
h += '</style></head><body>';

// Logo
h += '<svg width="52" height="52" viewBox="0 0 52 52" fill="none" style="margin-bottom:22px"><circle cx="26" cy="26" r="26" fill="#fff"/><rect x="10" y="20" width="4" height="12" rx="2" fill="#000"/><rect x="17" y="14" width="4" height="24" rx="2" fill="#000"/><rect x="24" y="18" width="4" height="16" rx="2" fill="#000"/><rect x="31" y="11" width="4" height="30" rx="2" fill="#000"/><rect x="38" y="17" width="4" height="18" rx="2" fill="#000"/></svg>';

h += '<div class="card">';
h += '<h1>Claudo for Eclipse</h1>';
h += '<p class="sub">Full TIDAL catalog &mdash; FLAC Hi-Res 24-bit, FLAC 16-bit, AAC fallback &mdash; no account needed. Qobuz Hi-Res &rarr; TIDAL FLAC &rarr; fallback.</p>';
h += '<div class="tip"><b>Save your URL.</b> Paste it below to refresh without reinstalling.</div>';
h += '<div class="pills"><span class="pill">Tracks &middot; Albums &middot; Artists</span><span class="pill hi">FLAC Hi-Res 24-bit</span><span class="pill hi">FLAC 16-bit</span><span class="pill hi">Qobuz Hi-Res</span></div>';

h += '<div class="lbl">Custom Hi&#8209;Fi Instance <span style="color:#2a2a2a;font-weight:400;text-transform:none">(optional)</span></div>';
h += '<input type="text" id="customInstance" placeholder="https://your-instance.example.com">';
h += '<div class="hint">Leave blank to use the shared pool. Paste your own self-hosted Hi-Fi API URL to lock this token exclusively to your instance.</div>';

// Quality selector — split into Qobuz group + TIDAL group, single selection across both
h += '<div class="lbl">Preferred Audio Quality <span style="color:#2a2a2a;font-weight:400;text-transform:none">(optional)</span></div>';

h += '<div class="ql-group-label">&#9675; Qobuz</div>';
h += '<div class="ql-row">';
h += '<div class="ql-btn" id="ql-HIMAX"       onclick="selectQ(\'HIMAX\',\'q\')">Hi-Res 192<span class="ql-sub">24-bit / up to 192kHz</span></div>';
h += '<div class="ql-btn" id="ql-HI96"        onclick="selectQ(\'HI96\',\'q\')">Hi-Res 96<span class="ql-sub">24-bit / up to 96kHz</span></div>';
h += '</div>';
h += '<div class="ql-row">';
h += '<div class="ql-btn" id="ql-LOSSLESS"    onclick="selectQ(\'LOSSLESS\',\'q\')">CD Quality<span class="ql-sub">16-bit / 44.1kHz</span></div>';
h += '</div>';

h += '<div class="ql-group-label" style="margin-top:14px">&#9675; TIDAL (skips Qobuz)</div>';
h += '<div class="ql-row">';
h += '<div class="ql-btn" id="ql-TIDAL_HIMAX"    onclick="selectQ(\'TIDAL_HIMAX\',\'t\')">FLAC Hi-Res<span class="ql-sub">24-bit / up to 192kHz</span></div>';
h += '<div class="ql-btn" id="ql-TIDAL_LOSSLESS" onclick="selectQ(\'TIDAL_LOSSLESS\',\'t\')">FLAC 16-bit<span class="ql-sub">44.1kHz Lossless</span></div>';
h += '</div>';
h += '<div class="ql-row">';
h += '<div class="ql-btn" id="ql-TIDAL_HIGH"  onclick="selectQ(\'TIDAL_HIGH\',\'t\')">AAC 320<span class="ql-sub">TIDAL AAC 320 kbps</span></div>';
h += '<div class="ql-btn" id="ql-TIDAL_LOW"   onclick="selectQ(\'TIDAL_LOW\',\'t\')">AAC 96<span class="ql-sub">TIDAL AAC 96 kbps</span></div>';
h += '</div>';

h += '<div class="hint" id="qlHint" style="margin-top:8px">No preference &mdash; auto-selects: Qobuz Hi-Res 24-bit &rarr; TIDAL Hi-Res FLAC &rarr; FLAC 16-bit &rarr; AAC 320 &rarr; AAC 96.</div>';

h += '<div class="lbl">Addon Name <span style="color:#2a2a2a;font-weight:400;text-transform:none">(optional)</span></div>';
h += '<input type="text" id="customAddonName" placeholder="Claudo" maxlength="40">';
h += '<div class="hint">Customize the name shown in Eclipse\'s connections list. Leave blank to keep the previous name.</div>';
h += '<button class="bw" id="genBtn" onclick="generate()">Generate My Addon URL</button>';

// Generate result box — styled like QTE screenshot
h += '<div class="box" id="genBox">';
h += '<div id="genQualBadge" class="qual-badge none"></div>';
h += '<div class="blbl">Your Addon URL &mdash; paste into Eclipse</div>';
h += '<div class="burl" id="genUrl"></div>';
h += '<button class="bd" id="copyGenBtn" onclick="copyGen()">Copy URL</button>';
h += '</div>';

h += '<hr>';
h += '<div class="lbl">Refresh existing URL</div>';
h += '<input type="text" id="existingUrl" placeholder="Paste your existing addon URL here">';
h += '<div class="hint">Keeps the same URL active &mdash; nothing to reinstall.</div>';
h += '<button class="bg" id="refBtn" onclick="doRefresh()">Refresh Existing URL</button>';
h += '<div class="box" id="refBox"><div class="blbl">Refreshed &mdash; same URL still works in Eclipse</div><div class="burl" id="refUrl"></div><button class="bd" id="copyRefBtn" onclick="copyRef()">Copy URL</button></div>';

h += '<hr>';
h += '<div class="steps">';
h += '<div class="step"><div class="sn">1</div><div class="st">Select a quality tier above, then click <b>Generate</b></div></div>';
h += '<div class="step"><div class="sn">2</div><div class="st">Open <b>Eclipse</b> &rarr; Settings &rarr; Connections &rarr; Add Connection &rarr; Addon</div></div>';
h += '<div class="step"><div class="sn">3</div><div class="st">Paste your URL and tap <b>Install</b></div></div>';
h += '<div class="step"><div class="sn">4</div><div class="st">Search TIDAL\'s full catalog &mdash; Qobuz Hi-Res played first automatically</div></div>';
h += '</div>';
h += '<div class="warn">Stream priority: <b>Qobuz Hi-Res 24-bit FLAC</b> &rarr; TIDAL Hi-Res FLAC &rarr; TIDAL FLAC 16-bit &rarr; AAC 320 &rarr; AAC 96. Selecting a <b>TIDAL</b> tier skips Qobuz entirely. AAC 320 only plays if FLAC is unavailable.</div>';
h += '</div>';



// TIDAL Instance Health card
h += '<div class="card">';
h += '<h2>TIDAL Instance Health</h2>';
h += '<p class="sub" style="margin-bottom:14px">Live status of all Hi-Fi API instances.</p>';
h += '<div class="inst-list" id="instList"><div style="color:#333;font-size:13px">Checking...</div></div>';
h += '<button class="bg" style="margin-top:14px" onclick="checkHealth()">Refresh Status</button>';
h += '</div>';

h += '<footer>Claudo Eclipse Addon &bull; TIDAL search &bull; Qobuz Hi-Res streams</footer>';

// JS
h += '<script>';
h += 'var gu,ru,selQ=null,selGroup=null;';

h += 'var QKEYS=["HIMAX","HI96","LOSSLESS"];';
h += 'var TKEYS=["TIDAL_HIMAX","TIDAL_LOSSLESS","TIDAL_HIGH","TIDAL_LOW"];';
h += 'var ALLKEYS=QKEYS.concat(TKEYS);';

h += 'var QLABELS={';
h += '"HIMAX":"Hi-Res 192 \u00b7 24-bit/192kHz",';
h += '"HI96":"Hi-Res 96 \u00b7 24-bit/96kHz",';
h += '"LOSSLESS":"CD Quality \u00b7 16-bit/44.1kHz",';
h += '"TIDAL_HIMAX":"TIDAL Hi-Res FLAC \u00b7 24-bit/192kHz",';
h += '"TIDAL_LOSSLESS":"TIDAL FLAC \u00b7 16-bit/44.1kHz",';
h += '"TIDAL_HIGH":"TIDAL AAC 320 kbps",';
h += '"TIDAL_LOW":"TIDAL AAC 96 kbps"';
h += '};';

h += 'function selectQ(q,grp){';
h += '  if(selQ===q){selQ=null;selGroup=null;}else{selQ=q;selGroup=grp;}';
h += '  ALLKEYS.forEach(function(k){';
h += '    var el=document.getElementById("ql-"+k);';
h += '    if(!el)return;';
h += '    el.classList.remove("sel-q","sel-t");';
h += '    if(selQ===k)el.classList.add(selGroup==="q"?"sel-q":"sel-t");';
h += '  });';
h += '  var hint=document.getElementById("qlHint");';
h += '  if(selQ){';
h += '    hint.textContent="Selected: "+QLABELS[selQ]+(selGroup==="t"?" \u2014 TIDAL only, skips Qobuz":" \u2014 Qobuz first, TIDAL fallback")+".";';
h += '  }else{';
h += '    hint.textContent="No preference \u2014 auto-selects: Qobuz Hi-Res \u2192 TIDAL Lossless \u2192 AAC 320 \u2192 AAC 96.";';
h += '  }';
h += '  updateBadge();';
h += '}';

h += 'function updateBadge(){';
h += '  var b=document.getElementById("genQualBadge");';
h += '  if(!b)return;';
h += '  if(!selQ){b.className="qual-badge none";b.textContent="No quality preference";return;}';
h += '  if(selGroup==="q"){b.className="qual-badge qobuz";b.innerHTML="&#9675; Qobuz \u00b7 "+QLABELS[selQ];}';
h += '  else{b.className="qual-badge tidal";b.innerHTML="&#9675; TIDAL \u00b7 "+QLABELS[selQ];}';
h += '}';

h += 'function generate(){';
h += '  var btn=document.getElementById("genBtn");';
h += '  btn.disabled=true;btn.textContent="Generating...";';
h += '  var ci=document.getElementById("customInstance").value.trim();';
h += '  var an=document.getElementById("customAddonName").value.trim();';
h += '  while(ci.length&&ci[ci.length-1]==="/")ci=ci.slice(0,-1);';
h += '  var body={};';
h += '  if(ci)body.instanceUrl=ci;';
h += '  if(an)body.addonName=an;';
h += '  if(selQ)body.preferredQuality=selQ;';
h += '  fetch("/generate",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)})';
h += '  .then(function(r){return r.json();})';
h += '  .then(function(d){';
h += '    if(d.error){alert(d.error);btn.disabled=false;btn.textContent="Generate My Addon URL";return;}';
h += '    gu=d.manifestUrl;';
h += '    document.getElementById("genUrl").textContent=gu;';
h += '    updateBadge();';
h += '    document.getElementById("genQualBadge").style.display="inline-flex";';
h += '    document.getElementById("genBox").style.display="block";';
h += '    btn.disabled=false;btn.textContent="Generate Another URL";';
h += '  })';
h += '  .catch(function(e){alert("Error: "+e.message);btn.disabled=false;btn.textContent="Generate My Addon URL";});';
h += '}';

h += 'function copyGen(){if(!gu)return;try{navigator.clipboard.writeText(gu);}catch(e){var t=document.createElement("textarea");t.value=gu;document.body.appendChild(t);t.select();document.execCommand("copy");document.body.removeChild(t);}var b=document.getElementById("copyGenBtn");b.textContent="Copied!";setTimeout(function(){b.textContent="Copy URL";},1500);}';
h += 'function doRefresh(){var btn=document.getElementById("refBtn");var eu=document.getElementById("existingUrl").value.trim();if(!eu){alert("Paste your existing addon URL first.");return;}btn.disabled=true;btn.textContent="Refreshing...";fetch("/refresh",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({existingUrl:eu})}).then(function(r){return r.json();}).then(function(d){if(d.error){alert(d.error);btn.disabled=false;btn.textContent="Refresh Existing URL";return;}ru=d.manifestUrl;document.getElementById("refUrl").textContent=ru;document.getElementById("refBox").style.display="block";btn.disabled=false;btn.textContent="Refresh Again";}).catch(function(e){alert("Error: "+e.message);btn.disabled=false;btn.textContent="Refresh Existing URL";});}';
h += 'function copyRef(){if(!ru)return;try{navigator.clipboard.writeText(ru);}catch(e){var t=document.createElement("textarea");t.value=ru;document.body.appendChild(t);t.select();document.execCommand("copy");document.body.removeChild(t);}var b=document.getElementById("copyRefBtn");b.textContent="Copied!";setTimeout(function(){b.textContent="Copy URL";},1500);}';

h += 'function checkHealth(){';
h += '  var list=document.getElementById("instList");';
h += '  list.innerHTML=\'<div style="color:#333;font-size:13px">Checking...</div>\';';
h += '  fetch("/instances").then(function(r){return r.json();}).then(function(data){';
h += '    list.innerHTML="";';
h += '    data.instances.forEach(function(inst){';
h += '      var row=document.createElement("div");row.className="inst";';
h += '      var dot=document.createElement("span");dot.className="dot "+(inst.ok?"ok":"err");';
h += '      var urlSpan=document.createElement("span");urlSpan.className="inst-url";';
h += '      var raw=inst.url.replace(/^https?:\\/\\//,"");';
h += '      var masked=raw.slice(0,3)+"\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022";';
h += '      urlSpan.textContent=masked;';
h += '      row.appendChild(dot);row.appendChild(urlSpan);';
h += '      if(inst.ok){var ms=document.createElement("span");ms.className="inst-ms";ms.textContent=inst.ms+"ms";row.appendChild(ms);}';
h += '      list.appendChild(row);';
h += '    });';
h += '  }).catch(function(){list.innerHTML=\'<div style="color:#c04040;font-size:13px">Could not reach server</div>\';});';
h += '}';

// Qobuz ping


h += 'checkHealth();';
h += '</script>';
h += '</body></html>';
return h;
}


// ─── Routes ───────────────────────────────────────────────────────────────────
app.get('/', async c => {
const baseUrl = (c.req.header('x-forwarded-proto') || 'https') + '://' + c.req.header('host');
return new Response(buildConfigPage(baseUrl), { headers: { 'Content-Type': 'text/html; charset=utf-8' } });
});

app.post('/generate', async c => {
const body = await parseBody(c);
const ip = (c.req.header('cf-connecting-ip') || c.req.header('x-forwarded-for') || 'unknown').split(',')[0].trim();
// Daily token cap per IP
const bucket = getOrCreateIpBucket(ip);
if (bucket.count >= MAX_TOKENS_PER_IP) return Response.json({ error: 'Too many tokens from this IP today. Max ' + MAX_TOKENS_PER_IP + ' per day.' }, { status: 429 });
// Hourly generation rate — prevents rapid token cycling / farming
const genBucket = IP_GEN_RATE.get(ip) || { count: 0, resetAt: Date.now() + 3600000 };
if (Date.now() > genBucket.resetAt) { genBucket.count = 0; genBucket.resetAt = Date.now() + 3600000; }
if (genBucket.count >= MAX_GEN_PER_HOUR) return Response.json({ error: 'Too many tokens generated this hour. Try again in an hour.' }, { status: 429 });
genBucket.count++;
IP_GEN_RATE.set(ip, genBucket);
let instanceUrl = (body && body.instanceUrl) ? String(body.instanceUrl).trim().replace(/\/$/, '') : null;
if (instanceUrl) {
if (!/^https?:\/\//.test(instanceUrl)) return Response.json({ error: 'Instance URL must start with http or https' }, { status: 400 });
try {
await axios.get(instanceUrl + '/search', { params: { s: 'test', limit: 1 }, timeout: 8000 });
} catch(e) { return Response.json({ error: 'Could not reach your instance: ' + e.message }, { status: 400 }); }
}
const VALID_QUALITIES = ['HI_RES_LOSSLESS','HIRESLOSSLESS','HIMAX','HI96','LOSSLESS','HIGH','AAC320','LOW','AAC96','TIDAL_HIMAX','TIDAL_LOSSLESS','TIDAL_HIGH','TIDAL_LOW'];
const preferredQuality = (body && body.preferredQuality && VALID_QUALITIES.includes(body.preferredQuality)) ? body.preferredQuality : null;
const token = generateToken();
const addonName = (body && body.addonName && String(body.addonName).trim()) ? String(body.addonName).trim().slice(0, 40) : null;
const entry = { createdAt: Date.now(), lastUsed: Date.now(), reqCount: 0, rateWin: [], instanceUrl, preferredQuality, addonName };
TOKEN_CACHE.set(token, entry);
await redisSave(token, entry);
bucket.count++;
const baseUrl = (c.req.header('x-forwarded-proto') || 'https') + '://' + c.req.header('host');
let tokenSegment = instanceUrl ? token + '~' + Buffer.from(instanceUrl).toString('base64url') : token;
if (addonName) {
  if (!instanceUrl) tokenSegment += '~';
  tokenSegment += '~' + Buffer.from(encodeURIComponent(addonName)).toString('base64url');
}
return Response.json({ token, manifestUrl: baseUrl + '/u/' + tokenSegment + '/manifest.json', usingCustomInstance: !!instanceUrl, preferredQuality, addonName });
});

app.post('/refresh', async c => {
const body = await parseBody(c);
const raw = (body && body.existingUrl) ? String(body.existingUrl).trim() : '';
const segMatch = raw.match(/\/u\/([^/]+)\/manifest\.json/);
const rawSegment = segMatch ? segMatch[1] : raw;
const { token: parsedToken } = parseTokenParam(rawSegment);
const token = parsedToken;
if (!token || !/^[a-f0-9]{28}$/.test(token)) return Response.json({ error: 'Paste your full addon URL.' }, { status: 400 });
const entry = await getTokenEntry(token);
if (!entry) return Response.json({ error: 'URL not found. Generate a new one.' }, { status: 404 });
const baseUrl = (c.req.header('x-forwarded-proto') || 'https') + '://' + c.req.header('host');
const instanceUrl = entry.instanceUrl;
const tokenSegment = instanceUrl ? token + '~' + Buffer.from(instanceUrl).toString('base64url') : token;
return Response.json({ token, manifestUrl: baseUrl + '/u/' + tokenSegment + '/manifest.json', refreshed: true });
});

app.get('/instances', async c => {
  const cached = cGet('instances:health');
  if (cached) return Response.json({ instances: cached, cached: true });
  const results = await Promise.all(HIFI_INSTANCES.map(async inst => {
    const start = Date.now();
    try {
      await axios.get(inst + '/search', { params: { s: 'test', limit: 1 }, timeout: 6000 });
      return { url: inst, ok: true, ms: Date.now() - start };
    } catch(e) { return { url: inst, ok: false, ms: null }; }
  }));
  cSet('instances:health', results, 30); // cache 30s — prevents 12-req burst per poll
  return Response.json({ instances: results });
});

app.get('/qobuz-ping', async c => {
  try {
    const ts  = Math.floor(Date.now() / 1000);
    const sig = md5('trackgetFileUrlformat_id6intentstreamtrack_id1' + ts + QOBUZ_SECRET);
    const url = 'https://www.qobuz.com/api.json/0.2/track/getFileUrl'
      + '?app_id=' + QOBUZ_APP_ID
      + '&user_auth_token=' + QOBUZ_USER_TOKEN
      + '&track_id=1&format_id=6&intent=stream'
      + '&request_ts=' + ts + '&request_sig=' + sig;
    const r = await fetch(url, { headers: { 'User-Agent': UA }, signal: AbortSignal.timeout(5000) });
    // 400 = bad track ID but auth worked, 200 = full success
    return Response.json({ ok: r.status === 200 || r.status === 400, status: r.status });
  } catch(e) {
    return Response.json({ ok: false, error: e.message });
  }
});

app.get('/health', c => {
return Response.json({
  status: 'ok',
  version: '2.4.6',
  activeInstance,
  instanceHealthy,
  qobuzBase: activeQobuzInstance,
  cachedTracks: TRACK_META_CACHE.size,
  cachedQobuzIds: QOBUZ_TRACK_ID_CACHE.size,
  activeTokens: TOKEN_CACHE.size,
  rateLimits: {
    globalDailyUsed: globalDailyCount,
    globalDailyLimit: GLOBAL_DAILY_LIMIT,
    globalStreamUsed: globalStreamCount,
    globalStreamLimit: GLOBAL_STREAM_DAILY_LIMIT,
    globalDayResetIn: Math.max(0, Math.ceil((86400000 - (Date.now() - globalDayStart)) / 60000)) + 'min',
  },
  timestamp: new Date().toISOString(),
});
});

app.get('/u/:token/manifest.json', async c => {
return withToken(c, entry => {
const rawParam = c.req.param('token');
const { token } = parseTokenParam(rawParam);
return Response.json({
id: 'com.eclipse.claudo.' + token.slice(0, 8),
name: (() => { const { embeddedName } = parseTokenParam(c.req.param('token')); return embeddedName || entry.addonName || 'Claudo'; })(),
version: '3.0.0',
description: 'TIDAL catalog search + Qobuz Hi-Res 24-bit streams. Falls back to TIDAL Lossless/AAC. No account required.',
icon: 'https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRtklZxzKIxXbfKsPsGTlnL6lbQqrr1fsIuJY2g4Xtt4w&s=10',
resources: ['search', 'stream', 'catalog'],
types: ['track', 'album', 'artist', 'playlist']
});
});
});

// ─── Search — TIDAL + cache track meta for stream ─────────────────────────────
app.get('/u/:token/search', async c => {
return withToken(c, async entry => {
// Per-token search rate limit — 20 searches per minute
if (!checkSearchRateLimit(entry)) {
  return Response.json({ error: 'Search rate limit exceeded. Max 20 searches per minute.', tracks: [], albums: [], artists: [], playlists: [] }, { status: 429 });
}
const q = String(c.req.query('q') || c.req.query('query') || c.req.query('s') || '').trim();
const limit = Math.min(parseInt(c.req.query('limit') || '20', 10) || 20, 50);
const inst = entry.instanceUrl;
if (!q) return Response.json({ tracks: [], albums: [], artists: [], playlists: [] });

const cacheKey = 'mc:search:' + (inst || 'pool') + ':' + q.toLowerCase() + ':' + limit;
  // In-memory cache check (fast path — avoids Upstash round-trip)
  const memCached = cGet(cacheKey);
  if (memCached) return Response.json(memCached);
  const cached = await upstashCmd('GET', cacheKey);
if (cached) {
try {
const parsed = JSON.parse(cached);
// Re-populate in-memory meta cache from cached search results
if (parsed.tracks) parsed.tracks.forEach(t => { if (t && t.id && t.title && !TRACK_META_CACHE.has(String(t.id))) cacheTrackMeta(t.id, t.title, t.artist); });
return Response.json(parsed);
} catch(e) {}
}

try {
// Fire track search + playlist search in parallel.
// Track search: GET /search/?s=query  (returns tracks, albums, artists)
// Playlist search: GET /search/?p=query  (TIDAL top-hits PLAYLISTS via HiFi proxy)
const [mainResult, plResult] = await Promise.allSettled([
  hifiGetForToken(inst, '/search', { s: q, limit, offset: 0 }),
  hifiGetForTokenSafe(inst, '/search', { p: q, limit: 10, offset: 0 }),
]);
const data  = mainResult.status === 'fulfilled' ? (mainResult.value || null) : null;
// Items array (tracks) at data.data.items OR data.items
const items = data?.data?.items || data?.items || [];

const albumMap = {}, artistMap = {}, artistHits = {}, tracks = [];
for (let i = 0; i < items.length; i++) {
const t = items[i];
if (!t || !t.id) continue;
if (t.album && t.album.id) {
const abid = String(t.album.id);
if (!albumMap[abid]) albumMap[abid] = { id: abid, title: t.album.title || 'Unknown', artist: trackArtist(t), artworkURL: coverUrl(t.album.cover, 1080), trackCount: t.album.numberOfTracks, year: t.album.releaseDate ? String(t.album.releaseDate).slice(0, 4) : undefined };
}
(t.artists || (t.artist ? [t.artist] : [])).forEach(a => {
if (!a || !a.id) return;
const arid = String(a.id);
if (!artistMap[arid]) artistMap[arid] = { id: arid, name: a.name || 'Unknown', artworkURL: coverUrl(a.picture, 320), ...(a.genres && a.genres.length ? { genres: a.genres.map(g => g.name || g).filter(Boolean) } : {}) };
artistHits[arid] = (artistHits[arid] || 0) + 1;
});
if (t.streamReady === false || t.allowStreaming === false) continue;
const tTitle = t.title || 'Unknown';
const tArtist = trackArtist(t);
cacheTrackMeta(t.id, tTitle, tArtist, t.isrc || null);
redisCacheTrackMeta(String(t.id), tTitle, tArtist, t.isrc || null);
// Background Qobuz pre-warm — find track AND pre-warm stream URL into cache
// so /stream/:id returns instantly from cache without any live Qobuz API calls.
// Also persists to Redis so any CF Worker isolate can serve the stream instantly.
(async () => {
  try {
    const qTrack = await qobuzFindBestTrack(tTitle, tArtist, t.isrc || null, inst);
    if (qTrack && qTrack.id) {
      cacheQobuzTrackId(t.id, qTrack); // writes to in-memory + Redis (cross-isolate)
      // Pre-warm all quality tiers in parallel — await so streams are in Redis before user taps play
      const preWarmKeys = [null, 'HIMAX', 'HI96', 'LOSSLESS', 'AAC320'];
      const streamResults = await Promise.allSettled(
        preWarmKeys.map(async key => {
          const cKey = 'qstream:' + qTrack.id + ':' + (key || 'auto');
          let result = cGet(cKey);
          if (!result) result = await qobuzStream(qTrack.id, key).catch(() => null);
          if (result) {
            // Write to Redis keyed by TIDAL id so cross-isolate stream lookups are instant
            await redisSaveQobuzStream(t.id, key, result).catch(() => {});
          }
          return result;
        })
      );
      const firstStream = streamResults.find(r => r.status === 'fulfilled' && r.value)?.value;
      if (firstStream) {
        console.log('[prewarm] cached stream to Redis for tid=' + t.id + ' qid=' + qTrack.id + ' quality=' + firstStream.quality);
      }
    }
  } catch(e) {}
})();
const tFormat = (t.audioQuality === 'HIGH' || t.audioQuality === 'LOW') ? 'aac' : 'flac';
tracks.push({ id: String(t.id), title: tTitle, artist: tArtist, album: t.album ? t.album.title : undefined, duration: trackDuration(t), artworkURL: coverUrl(t.album ? t.album.cover : null, 1080), format: tFormat });
}

const artistList = Object.keys(artistMap)
.sort((a, b) => (artistRelevance(artistMap[b].name, q) * 100 + (artistHits[b] || 0)) - (artistRelevance(artistMap[a].name, q) * 100 + (artistHits[a] || 0)))
.slice(0, 5).map(k => artistMap[k]);

// ── Playlists from dedicated p= search + fallback embedded field ─────────────
// The HiFi proxy calls TIDAL's top-hits?types=PLAYLISTS endpoint via GET /search/?p=query.
// Response shape: { data: { playlists: { items: [...] } } }
// Each playlist has: uuid, title, squareImage, image, creator, numberOfTracks
const plData = plResult.status === 'fulfilled' ? (plResult.value || null) : null;
const plFromSearch = plData?.data?.playlists?.items || plData?.data?.playlists
  || plData?.playlists?.items || plData?.playlists || plData?.data?.items || plData?.items || [];
// Also check if the main search response has a playlists field (some instances embed them)
const plEmbedded = data?.data?.playlists?.items || data?.data?.playlists
  || data?.playlists?.items || data?.playlists || [];

const seenPlIds = new Set();
const plItems = [];
for (const p of [...(Array.isArray(plFromSearch) ? plFromSearch : []),
                  ...(Array.isArray(plEmbedded)   ? plEmbedded   : [])]) {
  if (!p) continue;
  const pid = String(p.uuid || p.id || '');
  // Only real TIDAL playlists — UUID format, never numeric track IDs
  if (!pid || !isPlaylistUUID(pid) || seenPlIds.has(pid)) continue;
  seenPlIds.add(pid);
  plItems.push({
    id: pid,
    title: p.title || 'Playlist',
    creator: p.creator?.name || (p.type === 'EDITORIAL' ? 'TIDAL' : undefined),
    artworkURL: coverUrl(p.squareImage || p.image || p.cover, 1080),
    trackCount: p.numberOfTracks || p.trackCount,
  });
  if (plItems.length >= 10) break;
}

const result = { tracks, albums: Object.values(albumMap).slice(0, 8), artists: artistList, playlists: plItems };
  cSet(cacheKey, result, 300); // also cache in-memory for instant repeat hits
  upstashCmd('SET', cacheKey, JSON.stringify(result), 'EX', 300);
return Response.json(result);
} catch(e) {
return Response.json({ error: 'Search failed: ' + e.message, tracks: [], albums: [], artists: [], playlists: [] }, { status: 502 });
}
});
});

// ─── Stream: Qobuz Hi-Res first, TIDAL fallback ────────────────────────────────
app.get('/u/:token/stream/:id', async c => {
return withToken(c, async entry => {

// ── Stream / download rate limiting ──────────────────────────────────────────
// This is the most expensive route — called once per track for every stream AND download.
// A 20k-track playlist download attempt will be blocked after 500 tracks per 10-minute window.
const streamCheck = checkStreamRateLimit(entry);
if (!streamCheck.ok) {
  const msgs = {
    stream_min:    'Stream rate limit: too fast. Max ' + STREAM_MAX_PER_MIN + ' per minute.',
    stream_hour:   'Hourly stream limit reached (' + STREAM_MAX_PER_HOUR + ' tracks). Resume in ' + Math.ceil((entry.streamCooldown || STREAM_WINDOW_HOUR) / 3600000) + ' hour(s).',
    global_stream: 'Service stream capacity reached for today. Try again tomorrow.',
  };
  return Response.json({ error: msgs[streamCheck.reason] || 'Stream rate limit exceeded.' }, { status: 429 });
}

// Bulk download / playlist guard — catches insane playlist downloads
if (!checkBulkDownloadLimit(entry)) {
  return Response.json({
    error: 'Download limit reached. Max ' + BULK_DL_MAX + ' tracks per 10 minutes. Wait before resuming. Large playlist downloads are not supported.',
  }, { status: 429 });
}

const tid = c.req.param('id');
const inst = entry.instanceUrl;
const pref = entry.preferredQuality;

return dedupeCall('stream:' + tid + ':' + (inst || 'pool') + ':' + (pref || 'auto'), async () => {

// Step 1: title+artist from Eclipse query params (some clients send these)
let qTitle = String(c.req.query('title') || '').trim();
let qArtist = String(c.req.query('artist') || '').trim();

// Step 2: look up from in-memory cache (populated at search time)
let qIsrc = String(c.req.query('isrc') || '').trim() || null;
if (!qTitle) {
const mem = getCachedMeta(tid);
if (mem) { qTitle = mem.title; qArtist = mem.artist; if (!qIsrc) qIsrc = mem.isrc || null; console.log('meta: hit in-memory cache for', tid, '->', qTitle, qIsrc ? '(isrc: ' + qIsrc + ')' : ''); }
}

// Step 3: look up from Redis — if Redis disagrees with in-memory title, prefer Redis
// (in-memory cache can be poisoned by other searches on the same worker isolate)
const redisMeta = await redisLoadTrackMeta(tid);
if (redisMeta) {
  if (!qTitle) {
    qTitle = redisMeta.title; qArtist = redisMeta.artist; if (!qIsrc) qIsrc = redisMeta.isrc || null;
    console.log('meta: hit Redis cache for', tid, '->', qTitle, qIsrc ? '(isrc: ' + qIsrc + ')' : '');
  } else if (redisMeta.title && redisMeta.title.toLowerCase() !== qTitle.toLowerCase()) {
    // Redis has different title — Redis is more trustworthy (written from fresh search), prefer it
    console.log('meta: Redis/memory disagree for', tid, '— memory="' + qTitle + '" redis="' + redisMeta.title + '" — using Redis');
    qTitle = redisMeta.title; qArtist = redisMeta.artist; if (redisMeta.isrc) qIsrc = redisMeta.isrc;
  }
}

// ── Cold-start meta lookup: if cache missed, fetch from HiFi API ─────────────
// This handles the case where Eclipse calls /stream/:id directly (album, artist,
// playlist flows) without a preceding /search — meaning TRACK_META_CACHE is empty.
if (!qTitle && !qIsrc) {
  try {
    // Use /info (metadata endpoint) — NOT /track (stream endpoint) which returns manifest, not title/artist
    const trackInfo = await hifiGetForTokenSafe(inst, '/info', { id: tid });
    // HiFi API returns either {title, artist/artists, isrc} or {data:{...}} or {resource:{...}}
    const payload = trackInfo?.resource || trackInfo?.data || trackInfo;
    const coldTitle = payload?.title || null;
    if (coldTitle) {
      qTitle  = coldTitle;
      qArtist = trackArtist(payload) || qArtist;
      qIsrc   = payload?.isrc || qIsrc || null;
      console.log('meta: HiFi cold-lookup HIT for tid', tid, '->', qTitle, qIsrc ? '(isrc: ' + qIsrc + ')' : '');
      cacheTrackMeta(tid, qTitle, qArtist, qIsrc);
      redisCacheTrackMeta(String(tid), qTitle, qArtist, qIsrc).catch(() => {});
    }
  } catch(e) {
    console.log('meta: HiFi cold-lookup failed for tid', tid, '-', e.message);
  }
}
if (!qTitle && !qIsrc) console.log('meta: no cache for tid', tid, '- skipping Qobuz');

// ── Quality maps ─────────────────────────────────────────────────────────────
const PREF_TO_QOBUZ_KEY = {
  'HI_RES_LOSSLESS': 'HIMAX', 'HIRESLOSSLESS': 'HIMAX', 'HIMAX': 'HIMAX',
  'HI96': 'HI96', 'LOSSLESS': 'LOSSLESS',
  'HIGH': 'AAC320', 'AAC320': 'AAC320',
  'LOW': 'AAC96',  'AAC96': 'AAC96',
  // TIDAL-only tiers — Qobuz is skipped via isTidalOnlyPref above, these are never reached
  'TIDAL_HIMAX': null, 'TIDAL_LOSSLESS': null, 'TIDAL_HIGH': null, 'TIDAL_LOW': null,
};
const PREF_TO_TIDAL = {
  'HI_RES_LOSSLESS': 'HI_RES_LOSSLESS', 'HIRESLOSSLESS': 'HI_RES_LOSSLESS',
  'HIMAX': 'HI_RES_LOSSLESS', 'HI96': 'HI_RES_LOSSLESS',
  'LOSSLESS': 'LOSSLESS', 'HIGH': 'HIGH', 'AAC320': 'HIGH',
  'LOW': 'LOW', 'AAC96': 'LOW',
  // TIDAL-only tiers — map directly to TIDAL quality
  'TIDAL_HIMAX': 'HI_RES_LOSSLESS', 'TIDAL_LOSSLESS': 'LOSSLESS',
  'TIDAL_HIGH': 'HIGH', 'TIDAL_LOW': 'LOW',
};
const ALL_QUALITIES  = ['HI_RES_LOSSLESS', 'LOSSLESS', 'HIGH', 'LOW'];
const AUTO_QUALITIES = ['HI_RES_LOSSLESS', 'LOSSLESS', 'HIGH', 'LOW'];  // always try Hi-Res FLAC first

// ── TIDAL stream helper ───────────────────────────────────────────────────────
async function getTidalStream() {
  const tidalStartTier = pref ? (PREF_TO_TIDAL[pref] || 'LOSSLESS') : null;
  const qualities = tidalStartTier
    ? [tidalStartTier, ...ALL_QUALITIES.filter(q => ALL_QUALITIES.indexOf(q) > ALL_QUALITIES.indexOf(tidalStartTier))]
    : AUTO_QUALITIES;
  for (let qi = 0; qi < qualities.length; qi++) {
    const ql = qualities[qi];
    try {
      const data = await hifiGetForToken(inst, '/track', { id: tid, quality: ql });
      const payload = data && data.data ? data.data : data;
      if (payload && payload.manifest) {
        const decoded = decodeManifest(payload.manifest);
        if (decoded && decoded.url) {
          const codec = (decoded.codec || '').toLowerCase();
          const isFlac = decoded.isDash || codec.includes('flac') || codec.includes('audio/flac');
          const qualityLabel = ql === 'HI_RES_LOSSLESS' ? 'Hi-Res FLAC' : ql === 'LOSSLESS' ? 'FLAC 16-bit / 44.1 kHz' : ql === 'HIGH' ? '320kbps AAC' : '96kbps AAC';
          return { url: decoded.url, format: isFlac ? 'flac' : 'aac', quality: qualityLabel, codec: decoded.codec || null, expiresAt: Math.floor(Date.now() / 1000 + 21600) };
        }
      }
      if (payload && payload.url) {
        const looksLikeFlac = (payload.url || '').match(/\.flac(\?|$)/i);
        const isLosslessTier = ql === 'HI_RES_LOSSLESS' || ql === 'LOSSLESS';
        const qualityLabel = ql === 'HI_RES_LOSSLESS' ? 'Hi-Res FLAC' : ql === 'LOSSLESS' ? 'FLAC 16-bit / 44.1 kHz' : ql === 'HIGH' ? '320kbps AAC' : '96kbps AAC';
        return { url: payload.url, format: (looksLikeFlac || isLosslessTier) ? 'flac' : 'aac', quality: qualityLabel, expiresAt: Math.floor(Date.now() / 1000 + 21600) };
      }
    } catch(e) {
      if (qi === qualities.length - 1) throw e;
    }
  }
  return null;
}

// ── Steps 4+5: Fire Qobuz AND TIDAL in parallel ───────────────────────────────
// Qobuz is preferred — but both race simultaneously so TIDAL never adds latency.
// If Qobuz resolves first with a result, we return it immediately.
// If TIDAL resolves first (Qobuz still searching), we hold it and return it only
// if Qobuz ultimately fails. This gives Qobuz Hi-Res quality without Qobuz latency cost.
const qobuzPrefKey = pref ? (PREF_TO_QOBUZ_KEY[pref] || null) : null;
// Skip Qobuz only if: user explicitly chose a TIDAL-only tier, OR we have no metadata at all.
// For all Qobuz/auto tiers, always try Qobuz first — cascade ALL formats (27->7->6->5)
// before falling back to TIDAL. pref only controls TIDAL fallback quality, not Qobuz skipping.
const isTidalOnlyPref = pref && ['TIDAL_HIMAX','TIDAL_LOSSLESS','TIDAL_HIGH','TIDAL_LOW'].includes(pref);
const skipQobuz = isTidalOnlyPref || (!qTitle && !qIsrc);

// Qobuz promise — full find+stream pipeline
const qobuzPromise = skipQobuz ? Promise.resolve(null) : (async () => {
  try {
    // Fast path: check TIDAL->Qobuz track ID cache (populated at search/pre-warm time).
    // Cache hit = skip the entire qobuzFindBestTrack pipeline entirely.
    let qTrack = getCachedQobuzTrack(tid);
    if (qTrack) {
      console.log('[stream] qobuz id-cache HIT (memory) tid=' + tid + ' -> qobuzId=' + qTrack.id);
    } else {
      // Check Redis — survives across CF Worker isolates (fixes cross-isolate cache miss)
      qTrack = await redisLoadQobuzId(tid);
      if (qTrack) {
        cSet('qid:' + tid, qTrack, 3600); // warm local memory too
        console.log('[stream] qobuz id-cache HIT (redis) tid=' + tid + ' -> qobuzId=' + qTrack.id);
      }
    }
    if (!qTrack) {
      // Also check if Redis has a pre-warmed stream URL directly (fastest possible path)
      const preWarmedStream = await redisLoadQobuzStream(tid, qobuzPrefKey);
      if (preWarmedStream) {
        console.log('[stream] qobuz stream HIT (redis pre-warm) tid=' + tid + ' quality=' + preWarmedStream.quality);
        return preWarmedStream;
      }
    }
    if (!qTrack) {
      qTrack = await qobuzFindBestTrack(qTitle, qArtist, qIsrc, entry.instanceUrl);
      if (qTrack && qTrack.id) cacheQobuzTrackId(tid, qTrack); // writes to both memory + Redis
    }
    if (!qTrack || !qTrack.id) return null;
    // Title sanity check: if we have a known title and the Qobuz track title shares
    // zero words with it, reject the match — prevents wrong-song streams.
    if (qTitle && qTrack.title) {
      const norm = s => String(s||'').toLowerCase().replace(/[^a-z0-9\s]/g,' ').trim();
      const wantWords = norm(qTitle).split(/\s+/).filter(w => w.length > 1);
      const gotWords  = norm(qTrack.title).split(/\s+/).filter(w => w.length > 1);
      const overlap   = wantWords.filter(w => gotWords.includes(w)).length;
      if (wantWords.length > 0 && overlap === 0) {
        console.warn('[stream] qobuz title mismatch: wanted "' + qTitle + '" got "' + qTrack.title + '" — skipping');
        return null;
      }
    }
    const qStream = await qobuzStream(qTrack.id, qobuzPrefKey);
    if (qStream) {
      console.log('[stream] qobuz HIT', qTrack.id, qStream.quality);
      return qStream;
    }
    return null;
  } catch(e) {
    console.warn('[stream] qobuz error:', e.message);
    return null;
  }
})();

// TIDAL promise — runs immediately in parallel
const tidalPromise = (async () => {
  try { return await getTidalStream(); } catch(e) { return null; }
})();

// Race: return Qobuz if it wins with a result, otherwise TIDAL, otherwise error.
const [qResult, tResult] = await Promise.all([qobuzPromise, tidalPromise]);

if (qResult) {
  cSet('tstream:' + tid + ':' + (pref || 'auto'), qResult, 1680);
  return Response.json(qResult);
}
if (tResult) {
  cSet('tstream:' + tid + ':' + (pref || 'auto'), tResult, 1680);
  return Response.json(tResult);
}
return Response.json({ error: 'No stream found for track ' + tid }, { status: 404 });
}); // end dedupeCall

});
});

// ─── Album ────────────────────────────────────────────────────────────────────
app.get('/u/:token/album/:id', async c => {
return withToken(c, async entry => {
const aid = c.req.param('id');
const inst = entry.instanceUrl;
try {
  const data = await hifiGetForToken(inst, '/album', { id: aid, limit: 100, offset: 0 });
  // Unwrap all known HiFi API response shapes
  const album = data?.data?.id ? data.data
    : data?.data?.album?.id ? data.data.album
    : data?.album?.id ? data.album
    : data?.id ? data
    : data?.data ? data.data
    : data;
  // Collect track items — handle every nesting shape the API might return
  let rawItems = album?.items
    || album?.tracks?.items
    || album?.tracks
    || data?.items
    || data?.tracks?.items
    || [];
  if (!Array.isArray(rawItems)) rawItems = [];
  const artistName = album?.artist?.name
    || album?.artists?.map(a => a.name).join(', ')
    || 'Unknown';
  const cover = album?.cover || album?.image || album?.artwork;
  const tracks = rawItems.map((item, i) => {
    const t = item?.item || item;
    // Don't hard-filter on streamReady — TIDAL sometimes incorrectly marks playable tracks false
    if (!t || !t.id) return null;
    const tTitle = t.title || 'Unknown';
    const tArtist = trackArtist(t) || artistName;
    cacheTrackMeta(t.id, tTitle, tArtist, t.isrc || null);
    redisCacheTrackMeta(String(t.id), tTitle, tArtist, t.isrc || null);
    return { id: String(t.id), title: tTitle, artist: tArtist, duration: trackDuration(t), trackNumber: t.trackNumber || i + 1, artworkURL: coverUrl(cover, 1080) };
  }).filter(Boolean);
  return Response.json({ id: String(album?.id || aid), title: album?.title || 'Unknown', artist: artistName, artworkURL: coverUrl(cover, 1080), year: album?.releaseDate ? String(album.releaseDate).slice(0, 4) : undefined, trackCount: album?.numberOfTracks || tracks.length, tracks });
} catch(e) {
  return Response.json({ error: 'Album fetch failed: ' + e.message }, { status: 502 });
}
});
});

// ─── Artist ───────────────────────────────────────────────────────────────────
app.get('/u/:token/artist/:id', async c => {
  return withToken(c, async entry => {
    const aid = parseInt(c.req.param('id'), 10);
    const inst = entry.instanceUrl;
    if (isNaN(aid)) return Response.json({ error: 'Invalid artist ID' }, { status: 400 });

    // Pick the active base URL (custom instance or pool)
    const base = inst || activeInstance;

    try {
      // ── Step 1: Fire ALL known endpoints in parallel ─────────────────────────
      // Different HiFi instances expose different endpoint shapes/paths.
      // We fire them all at once and merge — zero extra latency vs. sequential.
      const [
        infoRes,       // GET /artist/?id=   — basic artist info
        discRes,       // GET /artist/?f=&skip_tracks=false  — full discography (albums+tracks)
        disc2Res,      // GET /artist/discography/?id=  — alternate discography endpoint
        topRes,        // GET /artist/toptracks/?id=
        albRes,        // GET /artist/albums/?id=  — no filter (returns whatever default is)
        albAlbumsRes,  // GET /artist/albums/?id=&filter=ALBUMS
        albEpsRes,     // GET /artist/albums/?id=&filter=EPSSINGLES
        albCompRes,    // GET /artist/albums/?id=&filter=COMPILATIONS
        albAltRes,     // GET /artist/albums/?artistId=  — some instances use artistId param
        searchRes,     // GET /search/?s=artistName  — fallback
      ] = await Promise.allSettled([
        axios.get(base + '/artist/',         { params: { id: aid },                                    headers: { 'User-Agent': UA, Accept: 'application/json' }, timeout: 12000 }),
        axios.get(base + '/artist/',         { params: { f: aid, skip_tracks: false },                 headers: { 'User-Agent': UA, Accept: 'application/json' }, timeout: 12000 }),
        axios.get(base + '/artist/discography/', { params: { id: aid, limit: 100 },                   headers: { 'User-Agent': UA, Accept: 'application/json' }, timeout: 12000 }),
        axios.get(base + '/artist/toptracks/', { params: { id: aid, limit: 30 },                      headers: { 'User-Agent': UA, Accept: 'application/json' }, timeout: 12000 }),
        axios.get(base + '/artist/albums/',  { params: { id: aid, limit: 100, offset: 0 },            headers: { 'User-Agent': UA, Accept: 'application/json' }, timeout: 12000 }),
        axios.get(base + '/artist/albums/',  { params: { id: aid, filter: 'ALBUMS',       limit: 100 }, headers: { 'User-Agent': UA, Accept: 'application/json' }, timeout: 12000 }),
        axios.get(base + '/artist/albums/',  { params: { id: aid, filter: 'EPSSINGLES',   limit: 100 }, headers: { 'User-Agent': UA, Accept: 'application/json' }, timeout: 12000 }),
        axios.get(base + '/artist/albums/',  { params: { id: aid, filter: 'COMPILATIONS', limit: 100 }, headers: { 'User-Agent': UA, Accept: 'application/json' }, timeout: 12000 }),
        axios.get(base + '/artist/albums/',  { params: { artistId: aid, limit: 100 },                 headers: { 'User-Agent': UA, Accept: 'application/json' }, timeout: 12000 }),
        // search result deferred — we need artistName first, filled below if needed
        Promise.resolve(null),
      ]);

      // ── Step 2: Extract artist info ──────────────────────────────────────────
      const extractData = r => {
        if (!r || r.status !== 'fulfilled' || !r.value) return {};
        return r.value.data?.data || r.value.data || {};
      };

      let artistInfo = {};
      const infoD = extractData(infoRes);
      if      (infoD.artist?.id)   artistInfo = infoD.artist;
      else if (infoD.id && infoD.name) artistInfo = infoD;
      // Fallback: disc response often has artist embedded
      if (!artistInfo.name) {
        const discD = extractData(discRes);
        if      (discD.artist?.id)    artistInfo = discD.artist;
        else if (discD.id && discD.name) artistInfo = discD;
      }
      if (!artistInfo.name) {
        const disc2D = extractData(disc2Res);
        if      (disc2D.artist?.id)     artistInfo = disc2D.artist;
        else if (disc2D.id && disc2D.name) artistInfo = disc2D;
      }

      const artistName = artistInfo.name || 'Unknown';
      const coverData  = infoD.cover;
      const artworkURL = coverData
        ? (coverData[750] || coverData[480] || coverData[320])
        : coverUrl(artistInfo.picture, 480);

      // ── Step 3: Merge albums from every source ────────────────────────────────
      const albumMap = {};
      const addAlbums = arr => {
        for (const a of (Array.isArray(arr) ? arr : [])) {
          if (!a?.id) continue;
          albumMap[String(a.id)] = albumMap[String(a.id)] || a;
        }
      };
      const extractAlbums = r => {
        const d = extractData(r);
        if (Array.isArray(d))               return d;
        if (Array.isArray(d.albums))        return d.albums;
        if (Array.isArray(d.albums?.items)) return d.albums.items;
        if (Array.isArray(d.items))         return d.items;
        return [];
      };
      const extractTracks = r => {
        const d = extractData(r);
        if (Array.isArray(d.tracks))        return d.tracks;
        if (Array.isArray(d.tracks?.items)) return d.tracks.items;
        if (Array.isArray(d.items))         return d.items;
        if (Array.isArray(d))               return d;
        return [];
      };

      // Albums from discography endpoints
      addAlbums(extractAlbums(discRes));
      addAlbums(extractAlbums(disc2Res));
      addAlbums(extractAlbums(infoRes));

      // Albums from per-type album endpoints
      for (const r of [albRes, albAlbumsRes, albEpsRes, albCompRes, albAltRes]) {
        addAlbums(extractAlbums(r));
      }

      // If any per-type page came back full (100), fetch page 2
      const albumTypeParams = [
        { filter: undefined },
        { filter: 'ALBUMS' },
        { filter: 'EPSSINGLES' },
        { filter: 'COMPILATIONS' },
      ];
      const page2Fetches = [];
      const typeResults  = [albRes, albAlbumsRes, albEpsRes, albCompRes];
      for (let i = 0; i < typeResults.length; i++) {
        const r = typeResults[i];
        if (r.status !== 'fulfilled') continue;
        const page1 = extractAlbums(r);
        if (page1.length >= 100) {
          const p = { id: aid, limit: 100, offset: 100 };
          if (albumTypeParams[i].filter) p.filter = albumTypeParams[i].filter;
          page2Fetches.push(
            axios.get(base + '/artist/albums/', { params: p, headers: { 'User-Agent': UA, Accept: 'application/json' }, timeout: 12000 })
              .then(r2 => { addAlbums(extractAlbums({ status: 'fulfilled', value: r2 })); })
              .catch(() => {})
          );
        }
      }
      if (page2Fetches.length) await Promise.allSettled(page2Fetches);

      // ── Step 4: Search fallback if albums still empty ────────────────────────
      const trackMap = {};
      const addTracks = arr => {
        for (const t of (Array.isArray(arr) ? arr : [])) {
          if (!t?.id) continue;
          trackMap[String(t.id)] = trackMap[String(t.id)] || t;
        }
      };

      // Tracks from discography
      addTracks(extractTracks(discRes));
      addTracks(extractTracks(disc2Res));
      // Tracks from toptracks
      addTracks(extractTracks(topRes));

      // Search fallback — always run to supplement tracks; albums only if still empty
      try {
        const sr = await axios.get(base + '/search/', {
          params: { s: artistName, limit: 50 },
          headers: { 'User-Agent': UA, Accept: 'application/json' },
          timeout: 12000,
        });
        const sItems = sr.data?.data?.items || sr.data?.items || [];
        const want   = artistName.toLowerCase();
        const isMain = t => {
          const arts = t.artists || (t.artist ? [t.artist] : []);
          if (!arts.length) return false;
          const mains = arts.filter(a => !a.type || a.type === 'MAIN');
          return (mains.length ? mains : [arts[0]]).some(a => {
            const n = (a.name || '').toLowerCase();
            return n === want || n.includes(want) || want.includes(n);
          });
        };
        for (const t of sItems) {
          if (!t?.id) continue;
          const ar = trackArtist(t).toLowerCase();
          if (ar.includes(want) || want.includes(ar)) addTracks([t]);
          if (t.album?.id && isMain(t)) {
            const alId = String(t.album.id);
            albumMap[alId] = albumMap[alId] || {
              id: t.album.id, title: t.album.title, cover: t.album.cover,
              releaseDate: t.album.releaseDate, numberOfTracks: t.album.numberOfTracks,
            };
          }
        }
      } catch(_) {}

      // ── Step 5: Build topTracks ───────────────────────────────────────────────
      const seenTrackIds = new Set();
      const topTracks = Object.values(trackMap)
        .filter(t => {
          if (!t?.id || t.allowStreaming === false) return false;
          const k = String(t.id);
          if (seenTrackIds.has(k)) return false;
          seenTrackIds.add(k);
          return true;
        })
        .sort((a, b) => (b.popularity || 0) - (a.popularity || 0))
        .slice(0, 20)
        .map(t => {
          const tTitle  = t.title || 'Unknown';
          const tArtist = trackArtist(t) || artistName;
          cacheTrackMeta(t.id, tTitle, tArtist, t.isrc || null);
          redisCacheTrackMeta(String(t.id), tTitle, tArtist, t.isrc || null);
          return {
            id: String(t.id), title: tTitle, artist: tArtist,
            duration: trackDuration(t),
            artworkURL: coverUrl(t.album?.cover || t.album?.image || t.album?.artwork, 1080),
          };
        });

      // ── Step 6: Build albums ──────────────────────────────────────────────────
      const albums = Object.values(albumMap)
        .sort((a, b) => {
          const ya = a.releaseDate ? parseInt(String(a.releaseDate).slice(0, 4), 10) : 0;
          const yb = b.releaseDate ? parseInt(String(b.releaseDate).slice(0, 4), 10) : 0;
          if (yb !== ya) return yb - ya;
          return (b.releaseDate || '').localeCompare(a.releaseDate || '');
        })
        .map(al => ({
          id: String(al.id), title: al.title || 'Unknown', artist: artistName,
          artworkURL: coverUrl(al.cover || al.image || al.artwork, 1080),
          trackCount: al.numberOfTracks,
          year: al.releaseDate ? String(al.releaseDate).slice(0, 4) : undefined,
        }));

      const artistGenres = (artistInfo.genres && artistInfo.genres.length)
        ? artistInfo.genres.map(g => g.name || g).filter(Boolean)
        : undefined;
      return Response.json({
        id: String(artistInfo.id || aid), name: artistName,
        artworkURL, bio: null, topTracks, albums,
        ...(artistGenres ? { genres: artistGenres } : {}),
      });
    } catch(e) {
      return Response.json({ error: 'Artist fetch failed: ' + e.message }, { status: 502 });
    }
  });
});

// ─── Playlist ─────────────────────────────────────────────────────────────────
app.get('/u/:token/playlist/:id', async c => {
return withToken(c, async entry => {
const pid = c.req.param('id');
const inst = entry.instanceUrl;
if (!isPlaylistUUID(pid)) return Response.json({ error: 'Invalid playlist ID. TIDAL playlist IDs must be UUIDs.' }, { status: 404 });
try {
const data = await hifiGetForToken(inst, '/playlist', { id: pid, limit: 100, offset: 0 });
let pl = null, rawItems = [];
if (data.playlist?.uuid || data.playlist?.id) { pl = data.playlist; rawItems = data.items || data.playlist.items || []; }
else if (data.data?.playlist) { pl = data.data.playlist; rawItems = data.data.items || data.items || []; }
else if (data.uuid || data.title) { pl = data; rawItems = data.items || []; }
else if (data.data?.uuid || data.data?.title) { pl = data.data; rawItems = data.data.items || data.items || []; }
else { pl = data; rawItems = data.items || []; }
const tracks = rawItems.map(item => {
const t = item.item || item;
if (!t || !t.id || t.streamReady === false) return null;
const tTitle = t.title || 'Unknown';
const tArtist = trackArtist(t);
cacheTrackMeta(t.id, tTitle, tArtist, t.isrc || null);
redisCacheTrackMeta(String(t.id), tTitle, tArtist, t.isrc || null);
return { id: String(t.id), title: tTitle, artist: tArtist, duration: trackDuration(t), artworkURL: coverUrl(t.album?.cover, 1080) };
}).filter(Boolean);
return Response.json({ id: String(pl?.uuid || pl?.id || pid), title: pl?.title || 'Playlist', creator: pl?.creator?.name, artworkURL: (pl?.squareImage || pl?.image) ? coverUrl(pl.squareImage || pl.image, 1080) : undefined, trackCount: pl?.numberOfTracks || tracks.length, tracks });
} catch(e) {
return Response.json({ error: 'Playlist fetch failed: ' + e.message }, { status: 502 });
}
});
});


// ─── Claudo 8SPINE Module Code ─────────────────────────────────────────
// Loaded by 8SPINE via: const createModule = new Function(code); createModule();
// Must end with a top-level `return { id, ... }` — no IIFE wrapper.
// __BASE_URL__ replaced at serve-time with the actual deployment URL.
// All vars are prefixed _spine* to avoid any naming collision with Eclipse routes.
const CLAUDO_SPINE_MODULE_CODE = `
var _spineBaseUrl = '__BASE_URL__';
var _spineToken = null;

// Eager pre-fetch: fires the moment 8SPINE loads this module.
// Token is ready before the user taps search — eliminates cold-start delay.
var _spineTokenPromise = fetch(_spineBaseUrl + '/generate', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({})
}).then(function(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
  .then(function(d) { _spineToken = d.token || null; return _spineToken; })
  .catch(function() { return null; });

function _spineEnsureToken() {
  if (_spineToken) return Promise.resolve(_spineToken);
  return _spineTokenPromise;
}

function _spineFetch(path, params) {
  var qs = '';
  if (params) {
    var keys = Object.keys(params);
    if (keys.length) {
      qs = '?' + keys.map(function(k) {
        return encodeURIComponent(k) + '=' + encodeURIComponent(params[k]);
      }).join('&');
    }
  }
  return fetch(_spineBaseUrl + path + qs, { headers: { 'Accept': 'application/json' } })
    .then(function(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); });
}

async function _spineSearchTracks(query, limit) {
  var lim = limit || 20;
  var token = await _spineEnsureToken();
  if (!token) return { tracks: [], total: 0 };
  return _spineFetch('/u/' + token + '/search', { q: query, limit: lim })
    .then(function(data) {
      var tracks = (data.tracks || []).map(function(t) {
        return {
          id: token + '__' + t.id,
          title: t.title || 'Unknown',
          artist: t.artist || 'Unknown',
          album: t.album || '',
          duration: t.duration || 0,
          albumCover: t.artworkURL || ''
        };
      });
      return { tracks: tracks, total: tracks.length };
    }).catch(function() { return { tracks: [], total: 0 }; });
}

async function _spineGetTrackStreamUrl(trackId, quality) {
  var sep = trackId.indexOf('__');
  if (sep === -1) return { streamUrl: null, track: { id: trackId, audioQuality: 'HIGH' } };
  var token = trackId.slice(0, sep);
  var tidalId = trackId.slice(sep + 2);
  return _spineFetch('/u/' + token + '/stream/' + encodeURIComponent(tidalId), {})
    .then(function(data) {
      var aq = (data.quality === 'hires' || data.quality === 'lossless') ? 'LOSSLESS' : 'HIGH';
      return { streamUrl: data.url || data.streamUrl || null, track: { id: trackId, audioQuality: aq } };
    }).catch(function() {
      return { streamUrl: null, track: { id: trackId, audioQuality: 'HIGH' } };
    });
}

async function _spineGetAlbum(albumId) {
  var token = await _spineEnsureToken();
  if (!token) return { album: null, tracks: [] };
  var sep = albumId.indexOf('__');
  var tok = sep !== -1 ? albumId.slice(0, sep) : token;
  var aid = sep !== -1 ? albumId.slice(sep + 2) : albumId;
  return _spineFetch('/u/' + tok + '/album/' + encodeURIComponent(aid), {})
    .then(function(data) {
      var tracks = (data.tracks || []).map(function(t) {
        return {
          id: tok + '__' + t.id,
          title: t.title || 'Unknown',
          artist: t.artist || data.artist || 'Unknown',
          album: data.title || '',
          duration: t.duration || 0,
          albumCover: t.artworkURL || data.artworkURL || ''
        };
      });
      return {
        album: { id: albumId, title: data.title || 'Unknown', artist: data.artist || 'Unknown', cover: data.artworkURL || '', year: data.year || '' },
        tracks: tracks
      };
    }).catch(function() { return { album: null, tracks: [] }; });
}

async function _spineGetArtist(artistId) {
  var token = await _spineEnsureToken();
  if (!token) return { artist: null, tracks: [], albums: [] };
  var sep = artistId.indexOf('__');
  var tok = sep !== -1 ? artistId.slice(0, sep) : token;
  var aid = sep !== -1 ? artistId.slice(sep + 2) : artistId;
  return _spineFetch('/u/' + tok + '/artist/' + encodeURIComponent(aid), {})
    .then(function(data) {
      var topTracks = (data.topTracks || []).map(function(t) {
        return { id: tok + '__' + t.id, title: t.title || 'Unknown', artist: t.artist || data.name || 'Unknown', album: '', duration: t.duration || 0, albumCover: t.artworkURL || data.artworkURL || '' };
      });
      var albums = (data.albums || []).map(function(a) {
        return { id: tok + '__' + a.id, title: a.title || 'Unknown', artist: a.artist || data.name || 'Unknown', cover: a.artworkURL || '', year: a.year || '' };
      });
      return {
        artist: { id: artistId, name: data.name || 'Unknown', cover: data.artworkURL || '', bio: data.bio || null },
        tracks: topTracks,
        albums: albums
      };
    }).catch(function() { return { artist: null, tracks: [], albums: [] }; });
}

// ─── Module export — top-level return required by 8SPINE Module Manager ───────
return {
  id: 'claudo-tidal',
  name: 'Claudo',
  version: '3.0.0',
  labels: ['FLAC', 'LOSSLESS', 'HI-RES', 'QOBUZ', 'TIDAL'],
  searchTracks: _spineSearchTracks,
  getTrackStreamUrl: _spineGetTrackStreamUrl,
  getAlbum: _spineGetAlbum,
  getArtist: _spineGetArtist
};
`;

// ─── Helper: inject runtime base URL into module code ────────────────────────
function buildClaudoSpineJs(baseUrl) {
  return CLAUDO_SPINE_MODULE_CODE.replace(/__BASE_URL__/g, baseUrl);
}

// ─── 8SPINE: module info ──────────────────────────────────────────────────────
app.get('/8spine', async c => {
  const ip = (c.req.header('cf-connecting-ip') || c.req.header('x-forwarded-for') || 'unknown').split(',')[0].trim();
  if (!checkUnauthRateLimit(ip)) return Response.json({ error: 'Rate limit exceeded.' }, { status: 429 });
  const base = (c.req.header('x-forwarded-proto') || 'https') + '://' + c.req.header('host');
  return c.json({
    id: 'claudo-tidal',
    name: 'Claudo',
    author: 'Ricky',
    version: '3.0.0',
    description: 'TIDAL full catalog search + Qobuz Hi-Res 24-bit streams. FLAC/Lossless/HiRes. No account required.',
    download: base + '/8spine.js'
  });
});

// ─── 8SPINE: serve module JS ──────────────────────────────────────────────────
app.get('/8spine.js', async c => {
  const ip = (c.req.header('cf-connecting-ip') || c.req.header('x-forwarded-for') || 'unknown').split(',')[0].trim();
  if (!checkUnauthRateLimit(ip)) return Response.json({ error: 'Rate limit exceeded.' }, { status: 429 });
  const base = (c.req.header('x-forwarded-proto') || 'https') + '://' + c.req.header('host');
  return new Response(buildClaudoSpineJs(base), {
    headers: { 'Content-Type': 'application/javascript; charset=utf-8' }
  });
});

// ─── 8SPINE: source list — merges Claudo + any extra source URLs ────────
// Add more 8spine-source.json URLs to EXTRA_SPINE_SOURCES to include them.
const EXTRA_SPINE_SOURCES = [
  'https://all-in-one.rickyaddons.dpdns.org/8spine-source.json',
  'https://eclipse3.cyrusna29.workers.dev/8spine-source.json',
  'https://qobuz-tidal-eclipse.cyrusna29.workers.dev/8spine-source.json',
  'https://improved-all-in-one.cyrusna29.workers.dev/8spine-source.json',
];

app.get('/8spine-source.json', async c => {
  const ip = (c.req.header('cf-connecting-ip') || c.req.header('x-forwarded-for') || 'unknown').split(',')[0].trim();
  if (!checkUnauthRateLimit(ip)) return Response.json({ error: 'Rate limit exceeded.' }, { status: 429 });
  const base = (c.req.header('x-forwarded-proto') || 'https') + '://' + c.req.header('host');

  const ourEntry = {
    id: 'claudo-tidal',
    name: 'Claudo',
    author: 'Ricky',
    version: '3.0.0',
    description: 'TIDAL full catalog search + Qobuz Hi-Res 24-bit streams. FLAC/Lossless/HiRes. No account required.',
    labels: ['FLAC', 'LOSSLESS', 'HI-RES', 'QOBUZ', 'TIDAL'],
    download: base + '/8spine.js'
  };

  const merged = { 'category:music': [ourEntry] };

  const results = await Promise.all(
    EXTRA_SPINE_SOURCES.map(url =>
      fetch(url, { headers: { 'Accept': 'application/json' } })
        .then(r => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
        .catch(() => null)
    )
  );

  for (const ext of results) {
    if (!ext || typeof ext !== 'object') continue;
    for (const cat of Object.keys(ext)) {
      const items = ext[cat];
      if (!Array.isArray(items)) continue;
      if (!merged[cat]) merged[cat] = [];
      for (const item of items) {
        if (!merged[cat].find(e => e.id === item.id)) merged[cat].push(item);
      }
    }
  }

  return c.json(merged);
});


export default app;
