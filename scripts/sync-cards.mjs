// ripjr.com: daily card sync (run by GitHub Actions, see .github/workflows/card-sync.yml)
// Downloads Scryfall's "Default Cards" bulk file, keeps only what packs need, and loads it into Supabase.
// Needs two repository secrets: SUPABASE_URL and SUPABASE_SECRET_KEY (the sb_secret_… key).
// The secret key only ever lives in GitHub's secret settings, never in index.html.

const SB_URL = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SB_KEY = process.env.SUPABASE_SECRET_KEY || '';
const SCRYFALL = process.env.SCRYFALL_API || 'https://api.scryfall.com';
const BATCH = 1000;
const BASICS = new Set(['Plains', 'Island', 'Swamp', 'Mountain', 'Forest']);
const UA = { 'User-Agent': 'ripjr.com-card-sync/1.0 (github actions)', 'Accept': 'application/json' };

if (!SB_URL || !SB_KEY) { console.error('Missing SUPABASE_URL or SUPABASE_SECRET_KEY repository secret.'); process.exit(1); }

const started = new Date().toISOString();
const noQuery = u => (u ? String(u).split('?')[0] : null);
const num = v => (v == null || v === '' ? null : +v);

function slim(c) {
  if (!c || c.object !== 'card') return null;
  if (!(c.games || []).includes('paper') || c.digital || c.oversized) return null;
  if (c.layout === 'art_series' || c.set_type === 'memorabilia' || (c.lang && c.lang !== 'en')) return null;
  const f = c.card_faces || [];
  const img = c.image_uris || (f[0] && f[0].image_uris) || {};
  const back = !c.image_uris && f[1] && f[1].image_uris ? f[1].image_uris.normal : null;
  const type = c.type_line || f.map(x => x.type_line).filter(Boolean).join(' // ');
  const oracle = c.oracle_text != null ? c.oracle_text : f.map(x => x.oracle_text || '').join('\n//\n');
  const p = c.prices || {};
  if (!img.normal && !img.large) return null;
  return {
    id: c.id, name: c.name, set_code: c.set, set_name: c.set_name || null, set_type: c.set_type || null,
    cn: c.collector_number || null, rarity: c.rarity || null, booster: !!c.booster,
    is_basic: BASICS.has(c.name) && /^Basic (Snow )?Land/.test(type || ''),
    is_token: /\bToken\b/.test(type || ''),
    judge: (c.promo_types || []).includes('judgegift'),
    usd: num(p.usd) ?? num(p.usd_foil) ?? num(p.usd_etched),
    img: noQuery(img.normal || img.large), back_img: noQuery(back),
    type_line: type || null,
    mana: c.mana_cost || f.map(x => x.mana_cost).filter(Boolean).join(' // ') || null,
    oracle: oracle ? oracle.slice(0, 1200) : null,
    uri: noQuery(c.scryfall_uri), buy: (c.purchase_uris || {}).tcgplayer || null,
    synced_at: started,
  };
}

async function sb(path, body, tries = 4) {
  for (let i = 1; ; i++) {
    const r = await fetch(SB_URL + path, {
      method: 'POST',
      headers: { apikey: SB_KEY, Authorization: 'Bearer ' + SB_KEY, 'Content-Type': 'application/json', Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: JSON.stringify(body),
    });
    if (r.ok) { const t = await r.text(); return t ? JSON.parse(t) : null; }
    const msg = `${r.status} ${await r.text()}`;
    if (i >= tries || (r.status >= 400 && r.status < 500 && r.status !== 429)) throw new Error(`Supabase ${path}: ${msg}`);
    console.log(`  retrying ${path} (${msg.slice(0, 120)})`);
    await new Promise(res => setTimeout(res, 2000 * i));
  }
}

// Reads a huge JSON array one object at a time, so the whole file never has to sit in memory.
async function* objectsIn(stream) {
  const dec = new TextDecoder();
  let depth = 0, inStr = false, esc = false, parts = [], start = -1;
  for await (const chunk of stream) {
    const s = dec.decode(chunk, { stream: true });
    if (start >= 0) start = 0;
    for (let i = 0; i < s.length; i++) {
      const ch = s[i];
      if (inStr) { if (esc) esc = false; else if (ch === '\\') esc = true; else if (ch === '"') inStr = false; continue; }
      if (ch === '"') { inStr = true; continue; }
      if (ch === '{' || ch === '[') { if (depth === 1 && ch === '{') { start = i; parts = []; } depth++; }
      else if (ch === '}' || ch === ']') {
        depth--;
        if (depth === 1 && ch === '}' && start >= 0) { parts.push(s.slice(start, i + 1)); yield JSON.parse(parts.join('')); start = -1; parts = []; }
      }
    }
    if (start >= 0) parts.push(s.slice(start));
  }
}

async function main() {
  console.log('Asking Scryfall for the Default Cards file…');
  const meta = await (await fetch(SCRYFALL + '/bulk-data/default-cards', { headers: UA })).json();
  if (!meta.download_uri) throw new Error('Scryfall did not return a download link: ' + JSON.stringify(meta).slice(0, 200));
  console.log(`Downloading ${meta.download_uri} (${Math.round((meta.size || 0) / 1e6)} MB, updated ${meta.updated_at})`);
  const res = await fetch(meta.download_uri, { headers: { 'User-Agent': UA['User-Agent'] } });
  if (!res.ok || !res.body) throw new Error('Download failed: ' + res.status);

  let seen = 0, kept = 0, batch = [];
  const flush = async () => { if (!batch.length) return; await sb('/rest/v1/cards?on_conflict=id', batch); kept += batch.length; batch = []; };
  for await (const c of objectsIn(res.body)) {
    seen++;
    const row = slim(c);
    if (row) batch.push(row);
    if (batch.length >= BATCH) { await flush(); if (kept % 10000 === 0) console.log(`  ${kept.toLocaleString()} cards loaded…`); }
  }
  await flush();
  console.log(`Read ${seen.toLocaleString()} cards from Scryfall, loaded ${kept.toLocaleString()} into Supabase.`);
  if (kept < 1000) throw new Error('Too few cards loaded; something went wrong.');

  console.log('Rebuilding pack pools and bust lines…');
  const report = await sb('/rest/v1/rpc/finish_card_sync', { p_started: started });
  console.log(JSON.stringify(report, null, 2));
  console.log('Done.');
}

main().catch(e => { console.error(e.message || e); process.exit(1); });
