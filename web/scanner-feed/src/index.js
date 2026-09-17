// scanner-feed/src/index.js
//
// Cloudflare Worker for the Tent Floor feed.
//
// Public:
//   GET  /                     the live feed page
//   GET  /v1/events?since=     replay, oldest first
//   GET  /v1/stream?since=     live tail (SSE), resumes via Last-Event-ID
//   GET  /lines                deprecated v0 shim, last 50 rendered lines
//
// Authenticated:
//   POST /v1/ingest            a batch of envelopes (INGEST_TOKEN)
//   POST /ingest               deprecated v0 shim, one rendered line
//
// Reading is tiered rather than all-or-nothing. See TIER ENFORCEMENT below.
//
// Setup:
//   npx wrangler deploy
//   npx wrangler secret put INGEST_TOKEN    # the scanner's write token
//   npx wrangler secret put READER_TOKEN    # optional; unlocks full replay

import { FeedLog, MAX_BATCH, MAX_LIMIT } from "./log.js";
import { forReader, SENSITIVE } from "./tier.js";
import { HTML_PAGE } from "./page.js";

export { FeedLog };

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
};

const json = (body, status = 200, headers = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8", ...CORS, ...headers },
  });

function bearer(request, url) {
  const header = request.headers.get("Authorization") || "";
  if (header.startsWith("Bearer ")) return header.slice(7);
  // An EventSource cannot set headers, so a browser tailing the stream has
  // nowhere else to put its token.
  return url.searchParams.get("token") || "";
}

/** Constant-time compare. A token is only worth having if guessing it is not
 *  cheaper than brute force. */
function tokenMatches(supplied, expected) {
  if (!expected || !supplied || supplied.length !== expected.length) return false;
  let diff = 0;
  for (let i = 0; i < supplied.length; i++) diff |= supplied.charCodeAt(i) ^ expected.charCodeAt(i);
  return diff === 0;
}

function logStub(env) {
  // One log, one object. idFromName is stable, so every request lands on the
  // same instance and ordering is whatever that instance saw.
  return env.FEED_LOG.get(env.FEED_LOG.idFromName("v1"));
}

function isEnvelope(value) {
  return (
    value &&
    typeof value === "object" &&
    typeof value.id === "string" &&
    typeof value.ts === "string" &&
    typeof value.type === "string" &&
    typeof value.tier === "string"
  );
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") return new Response(null, { headers: CORS });

    const supplied = bearer(request, url);
    const authorised =
      tokenMatches(supplied, env.READER_TOKEN) || tokenMatches(supplied, env.INGEST_TOKEN);

    try {
      if (url.pathname === "/v1/ingest" && request.method === "POST") {
        return await ingest(request, env);
      }
      if (url.pathname === "/ingest" && request.method === "POST") {
        return await ingestLegacy(request, env);
      }
      if (url.pathname === "/v1/events" && request.method === "GET") {
        return await events(url, env, authorised);
      }
      if (url.pathname === "/v1/stream" && request.method === "GET") {
        return await stream(request, url, env, authorised);
      }
      if (url.pathname === "/lines" && request.method === "GET") {
        return await lines(env, authorised);
      }
      if ((url.pathname === "/" || url.pathname === "") && request.method === "GET") {
        return new Response(HTML_PAGE, {
          headers: { "Content-Type": "text/html; charset=utf-8" },
        });
      }
      return json({ error: "not found" }, 404);
    } catch (e) {
      return json({ error: `${e.name}: ${e.message}` }, 500);
    }
  },
};

// -- writing ----------------------------------------------------------------
async function ingest(request, env) {
  if (!tokenMatches(bearer(request, new URL(request.url)), env.INGEST_TOKEN)) {
    return json({ error: "unauthorized" }, 401);
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return json({ error: "body must be JSON" }, 400);
  }

  const batch = Array.isArray(body) ? body : Array.isArray(body.events) ? body.events : [body];
  if (batch.length === 0) return json({ accepted: 0, duplicates: 0 });
  if (batch.length > MAX_BATCH) {
    return json({ error: `at most ${MAX_BATCH} events per request` }, 413);
  }
  if (!batch.every(isEnvelope)) {
    return json({ error: "each event needs id, ts, type and tier" }, 400);
  }

  const response = await logStub(env).fetch("https://log/append", {
    method: "POST",
    body: JSON.stringify(batch),
  });
  return json(await response.json(), 200);
}

/** The v0 shape: one pre-rendered line. Kept so a scanner that has not been
 *  updated yet keeps working through the cutover; it is not the surface anyone
 *  should be building against. */
async function ingestLegacy(request, env) {
  if (!tokenMatches(bearer(request, new URL(request.url)), env.INGEST_TOKEN)) {
    return json({ error: "unauthorized" }, 401);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return json({ error: "body must be JSON" }, 400);
  }
  if (!body.line || typeof body.line !== "string") {
    return json({ error: "'line' required" }, 400);
  }

  const now = new Date();
  const event = {
    v: 1,
    // No ULID from a v0 sender, so mint one that still sorts by time.
    id: legacyUlid(now.getTime()),
    ts: now.toISOString(),
    type: LEGACY_TYPES[body.type] || "transcript.final",
    source: "legacy",
    group: null,
    stream: null,
    tier: body.type === "transcript" || !body.type ? SENSITIVE : "public",
    data: { text: body.line },
    render: { plain: body.line, discord: body.line },
  };

  const response = await logStub(env).fetch("https://log/append", {
    method: "POST",
    body: JSON.stringify([event]),
  });
  await response.json();
  return new Response("OK", { status: 200, headers: CORS });
}

const LEGACY_TYPES = {
  transcript: "transcript.final",
  tone: "tone.page",
  pulsepoint: "cad.incident.new",
  nanaimo_fire: "cad.nfr.incident",
  bc_wildfire: "wildfire.update",
};

const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
function legacyUlid(ms) {
  let time = "";
  let value = ms;
  for (let i = 0; i < 10; i++) {
    time = CROCKFORD[value % 32] + time;
    value = Math.floor(value / 32);
  }
  let random = "";
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  for (const byte of bytes) random += CROCKFORD[byte % 32];
  return time + random;
}

// -- reading ----------------------------------------------------------------
async function events(url, env, authorised) {
  const since = url.searchParams.get("since") || "";
  const limit = Math.min(Number(url.searchParams.get("limit")) || 100, MAX_LIMIT);

  const response = await logStub(env).fetch(
    `https://log/read?since=${encodeURIComponent(since)}&limit=${limit}`
  );
  const page = await response.json();

  const now = Date.now();
  const visible = page.events.map((e) => forReader(e, { authorised, now })).filter(Boolean);

  return json({
    events: visible,
    has_more: page.has_more,
    // The cursor is the last event *read*, not the last one shown. Advancing
    // only past visible events would wedge a reader whose next page is entirely
    // withheld, re-requesting the same range forever.
    cursor: page.cursor,
    withheld: page.events.length - visible.length,
    authorised,
  });
}

async function stream(request, url, env, authorised) {
  const since = url.searchParams.get("since") || request.headers.get("Last-Event-ID") || "";

  const response = await logStub(env).fetch(
    `https://log/subscribe?since=${encodeURIComponent(since)}&authorised=${authorised ? 1 : 0}`
  );

  return new Response(response.body, {
    headers: {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-store",
      "X-Accel-Buffering": "no",
      ...CORS,
    },
  });
}

/** The v0 read shape the current page polls. Deprecated; /v1/events is the
 *  surface with the structure in it. */
async function lines(env, authorised) {
  const response = await logStub(env).fetch("https://log/read?limit=500");
  const page = await response.json();
  const now = Date.now();

  const recent = page.events.slice(-50);
  const out = [];
  for (const event of recent) {
    const visible = forReader(event, { authorised, now });
    if (!visible) continue;
    out.push({
      text: (visible.render && visible.render.plain) || "",
      type: LEGACY_LINE_TYPES[visible.type] || "transcript",
      ts: visible.ts,
    });
  }
  return json(out);
}

const LEGACY_LINE_TYPES = {
  "transcript.final": "transcript",
  "tone.page": "tone",
  "cad.incident.new": "pulsepoint",
  "cad.incident.cleared": "pulsepoint",
  "cad.unit.added": "pulsepoint",
  "cad.unit.status": "pulsepoint",
  "cad.nfr.incident": "nanaimo_fire",
  "wildfire.declared": "bc_wildfire",
  "wildfire.update": "bc_wildfire",
  "wildfire.removed": "bc_wildfire",
};
