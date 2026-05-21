// scanner-feed/src/index.js
//
// Cloudflare Worker for live scanner feed.
//
// Routes:
//   POST /ingest  — accepts JSON lines from scanner_transcribe.py (auth required)
//   GET  /lines   — returns last 50 lines as JSON (public)
//   GET  /        — serves the live feed HTML page (public)
//
// Setup:
//   1. Create a KV namespace called SCANNER_KV in the Cloudflare dashboard
//   2. Add it to wrangler.toml:  kv_namespaces = [{ binding = "SCANNER_KV", id = "your-kv-id" }]
//   3. Set the auth secret:  npx wrangler secret put INGEST_TOKEN
//      (then paste your chosen token — use any random string)
//   4. Deploy:  npx wrangler deploy

const MAX_LINES = 50;
const KV_KEY = "feed_lines";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS headers for API routes
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    // POST /ingest — receive new lines from the scanner script
    if (url.pathname === "/ingest" && request.method === "POST") {
      // Auth check
      const auth = request.headers.get("Authorization");
      if (!auth || auth !== `Bearer ${env.INGEST_TOKEN}`) {
        return new Response("Unauthorized", { status: 401 });
      }

      try {
        const body = await request.json();
        const line = body.line;
        const lineType = body.type || "transcript"; // transcript, tone, pulsepoint, nanaimo_fire

        if (!line || typeof line !== "string") {
          return new Response("Bad request: 'line' required", { status: 400 });
        }

        // Get existing lines
        const existing = await env.SCANNER_KV.get(KV_KEY, "json") || [];

        // Append new line with metadata
        existing.push({
          text: line,
          type: lineType,
          ts: new Date().toISOString(),
        });

        // Trim to last N lines
        const trimmed = existing.slice(-MAX_LINES);

        // Store back
        await env.SCANNER_KV.put(KV_KEY, JSON.stringify(trimmed));

        return new Response("OK", { status: 200, headers: corsHeaders });
      } catch (e) {
        return new Response("Error: " + e.message, { status: 500 });
      }
    }

    // GET /lines — return current lines as JSON
    if (url.pathname === "/lines") {
      const lines = await env.SCANNER_KV.get(KV_KEY, "json") || [];
      return new Response(JSON.stringify(lines), {
        headers: { "Content-Type": "application/json", ...corsHeaders },
      });
    }

    // GET / — serve the live feed page
    if (url.pathname === "/" || url.pathname === "") {
      return new Response(HTML_PAGE, {
        headers: { "Content-Type": "text/html; charset=utf-8" },
      });
    }

    return new Response("Not found", { status: 404 });
  },
};

// ---------------------------------------------------------------------------
// Inline HTML page — auto-refreshing scanner feed
// ---------------------------------------------------------------------------
const HTML_PAGE = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Scanner Feed</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=DM+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #0a0e14;
    --surface: #0f1319;
    --border: #1a1f2b;
    --text: #c5cdd8;
    --text-dim: #5c6674;
    --accent-radio: #4fc3f7;
    --accent-tone: #ffb74d;
    --accent-fire: #ef5350;
    --accent-pp: #66bb6a;
    --accent-nf: #ff7043;
    --glow-radio: rgba(79, 195, 247, 0.08);
    --glow-fire: rgba(239, 83, 80, 0.08);
  }

  * { margin: 0; padding: 0; box-sizing: border-box; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'DM Sans', sans-serif;
    min-height: 100vh;
    overflow-x: hidden;
  }

  .header {
    position: sticky;
    top: 0;
    z-index: 10;
    background: var(--bg);
    border-bottom: 1px solid var(--border);
    padding: 16px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    backdrop-filter: blur(12px);
  }

  .header h1 {
    font-family: 'JetBrains Mono', monospace;
    font-size: 14px;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--accent-radio);
  }

  .status {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 12px;
    color: var(--text-dim);
    font-family: 'JetBrains Mono', monospace;
  }

  .status-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--accent-fire);
    animation: pulse-dot 2s ease-in-out infinite;
  }

  .status-dot.connected { background: #66bb6a; }

  @keyframes pulse-dot {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.4; }
  }

  .feed {
    max-width: 900px;
    margin: 0 auto;
    padding: 16px 24px 80px;
    display: flex;
    flex-direction: column;
    gap: 2px;
  }

  .line {
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    line-height: 1.65;
    padding: 8px 12px;
    border-radius: 4px;
    border-left: 3px solid transparent;
    animation: fade-in 0.4s ease-out;
    word-break: break-word;
  }

  .line .time {
    color: var(--text-dim);
    margin-right: 8px;
    font-size: 11px;
  }

  .line.transcript {
    border-left-color: var(--accent-radio);
    background: var(--glow-radio);
  }

  .line.tone {
    border-left-color: var(--accent-tone);
    color: var(--accent-tone);
  }

  .line.pulsepoint {
    border-left-color: var(--accent-pp);
    background: rgba(102, 187, 106, 0.05);
  }

  .line.nanaimo_fire {
    border-left-color: var(--accent-nf);
    background: var(--glow-fire);
  }

  .line .label {
    display: inline-block;
    font-size: 9px;
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    padding: 2px 5px;
    border-radius: 3px;
    margin-right: 8px;
    vertical-align: middle;
  }

  .line.transcript .label {
    background: rgba(79, 195, 247, 0.15);
    color: var(--accent-radio);
  }

  .line.tone .label {
    background: rgba(255, 183, 77, 0.15);
    color: var(--accent-tone);
  }

  .line.pulsepoint .label {
    background: rgba(102, 187, 106, 0.15);
    color: var(--accent-pp);
  }

  .line.nanaimo_fire .label {
    background: rgba(255, 112, 67, 0.15);
    color: var(--accent-nf);
  }

  .empty {
    text-align: center;
    color: var(--text-dim);
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    padding: 60px 24px;
  }

  @keyframes fade-in {
    from { opacity: 0; transform: translateY(4px); }
    to { opacity: 1; transform: translateY(0); }
  }

  @media (max-width: 600px) {
    .header { padding: 12px 16px; }
    .feed { padding: 12px 16px 60px; }
    .line { font-size: 12px; padding: 6px 8px; }
  }
</style>
</head>
<body>

<div class="header">
  <h1>&#x1F4E1; Scanner Feed</h1>
  <div class="status">
    <span class="status-dot" id="statusDot"></span>
    <span id="statusText">connecting...</span>
  </div>
</div>

<div class="feed" id="feed">
  <div class="empty" id="emptyMsg">Waiting for transmissions...</div>
</div>

<script>
const POLL_MS = 5000;
const feed = document.getElementById('feed');
const emptyMsg = document.getElementById('emptyMsg');
const statusDot = document.getElementById('statusDot');
const statusText = document.getElementById('statusText');

let lastCount = 0;
let lastTs = null;

const LABELS = {
  transcript: 'RADIO',
  tone: 'PAGE',
  pulsepoint: 'CAD',
  nanaimo_fire: 'NFR',
};

function formatTime(isoStr) {
  try {
    const d = new Date(isoStr);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  } catch {
    return '';
  }
}

function stripEmoji(text) {
  return text.replace(/^[\\u{1F300}-\\u{1FAD6}\\u{2600}-\\u{27BF}\\u{FE00}-\\u{FE0F}\\u{1F900}-\\u{1F9FF}]+\\s*/u, '');
}

async function poll() {
  try {
    const resp = await fetch('/lines');
    if (!resp.ok) throw new Error(resp.status);
    const lines = await resp.json();

    statusDot.classList.add('connected');
    statusText.textContent = lines.length + ' lines';

    if (lines.length === 0) {
      emptyMsg.style.display = '';
      return;
    }

    emptyMsg.style.display = 'none';

    // Only re-render if data changed
    const newTs = lines.length > 0 ? lines[lines.length - 1].ts : null;
    if (newTs === lastTs && lines.length === lastCount) return;
    lastTs = newTs;
    lastCount = lines.length;

    feed.innerHTML = '';
    for (const line of lines) {
      const div = document.createElement('div');
      div.className = 'line ' + (line.type || 'transcript');

      const time = formatTime(line.ts);
      const label = LABELS[line.type] || 'LOG';
      const text = stripEmoji(line.text);

      div.innerHTML =
        '<span class="time">' + time + '</span>' +
        '<span class="label">' + label + '</span>' +
        text;

      feed.appendChild(div);
    }

    // Auto-scroll to bottom
    window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });

  } catch (e) {
    statusDot.classList.remove('connected');
    statusText.textContent = 'offline';
  }
}

poll();
setInterval(poll, POLL_MS);
</script>
</body>
</html>`;
