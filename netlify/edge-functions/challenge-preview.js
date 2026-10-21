// ripjr.com challenge links: ripjr.com/c/rip-and-flip/abc123
// Discord (and iMessage, Slack, etc.) read the preview tags on this little page: who's challenging you, which mode,
// and a picture for that mode. People who click it get sent straight on to the site to play.
// Uses only the public (publishable) Supabase key, the same one the website itself uses.
const SUPABASE_URL = 'https://icapzxlawhmopqelkeft.supabase.co';
const SUPABASE_KEY = 'sb_publishable_jqY4GbGYhqUh_N4tsos3Dg_R4WazLzl';

const MODES = {
  flip:  {slug: 'rip-and-flip',    name: 'Rip & Flip',      img: 'share-flip.jpg',   rule: 'Best of 5. Pick a pack, rip its rare in half. The cheaper rip wins the round.'},
  duel:  {slug: 'money-spread',    name: 'Money Spread',    img: 'share-spread.jpg', rule: '7 packs each. The bigger pack wins the round.'},
  guess: {slug: 'guess-the-value', name: 'Guess the Value', img: 'share-guess.jpg',  rule: 'Prices hidden. 20 seconds to guess what each pack is worth.'},
  bust:  {slug: 'bank-or-bust',    name: 'Bank or Bust',    img: 'share-home.jpg',   rule: 'Stack pack value. Bank before one comes in under the line.'},
};
const BY_SLUG = Object.fromEntries(Object.entries(MODES).map(([k, m]) => [m.slug, k]));
const ERA = {all: 'Any', old: 'Old school', new: 'Newest set'};
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));

export default async (request) => {
  const url = new URL(request.url);
  const parts = url.pathname.split('/').filter(Boolean);           // ['c', 'rip-and-flip', 'abc123'] or ['c', 'abc123']
  const id = (parts[parts.length - 1] || '').replace(/[^A-Za-z0-9]/g, '').slice(0, 24);
  let mode = parts.length > 2 ? BY_SLUG[parts[1]] : null;
  let who = '', era = '', note = '';
  if (id) {
    try {
      const ctl = new AbortController(); const t = setTimeout(() => ctl.abort(), 2500);
      const r = await fetch(`${SUPABASE_URL}/rest/v1/rpc/challenge_peek`, {
        method: 'POST', signal: ctl.signal,
        headers: {apikey: SUPABASE_KEY, 'content-type': 'application/json'},
        body: JSON.stringify({p_id: id}),
      });
      clearTimeout(t);
      if (r.ok) {
        const c = await r.json();
        if (c && c.mode) {
          mode = c.mode; who = c.creator_name || ''; era = ERA[c.era] || '';
          if (c.cancelled) note = 'This challenge was cancelled.';
          else if (c.done) note = 'This one’s finished. Open it to see who won.';
          else if (c.expired) note = 'This challenge expired.';
          else if (c.taken) note = 'Someone already took this one.';
        }
      }
    } catch (e) { /* no details: the mode from the link still works */ }
  }
  const m = MODES[mode];
  const title = m ? (who ? `${who} challenged you to ${m.name}` : `You’ve been challenged to ${m.name}`) : 'You’ve been challenged on ripjr.com';
  const desc = note || (m ? `${era ? era + ' packs · ' : ''}${m.rule} Free to play.` : 'Rip free Magic booster packs and go head to head.');
  const img = `${url.origin}/${m ? m.img : 'share-home.jpg'}`;
  const go = `/?c=${encodeURIComponent(id)}`;
  const html = `<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${esc(title)}</title>
<meta name="description" content="${esc(desc)}">
<meta property="og:site_name" content="ripjr.com">
<meta property="og:type" content="website">
<meta property="og:title" content="${esc(title)}">
<meta property="og:description" content="${esc(desc)}">
<meta property="og:url" content="${esc(url.href)}">
<meta property="og:image" content="${esc(img)}">
<meta property="og:image:width" content="1200"><meta property="og:image:height" content="630">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="${esc(title)}"><meta name="twitter:description" content="${esc(desc)}"><meta name="twitter:image" content="${esc(img)}">
<meta name="theme-color" content="#F4BF3A">
<meta http-equiv="refresh" content="0; url=${esc(go)}">
</head><body style="background:#1A130E;color:#F3E4C4;font-family:system-ui,sans-serif;display:grid;place-items:center;min-height:100vh;margin:0">
<p><a href="${esc(go)}" style="color:#F4BF3A">Open the challenge on ripjr.com</a></p>
<script>location.replace(${JSON.stringify(go)});</script>
</body></html>`;
  return new Response(html, {headers: {'content-type': 'text/html; charset=utf-8', 'cache-control': 'public, max-age=60'}});
};

export const config = {path: '/c/*'};
