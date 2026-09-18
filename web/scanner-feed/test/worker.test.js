// Phase 4 is the first time the Worker has had behaviour worth being wrong
// about: idempotent ingest, a replay cursor, a live tail, and tiers that decide
// who sees a transcript. These run against a real Durable Object under
// miniflare rather than a stub, because the parts most likely to break —
// storage key ordering, SSE framing, one object serialising concurrent
// appends — are exactly what a stub would define away.

import assert from "node:assert/strict";
import { after, before, beforeEach, describe, it } from "node:test";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { Miniflare } from "miniflare";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..");

const INGEST_TOKEN = "ingest-secret";
const READER_TOKEN = "reader-secret";

let mf;

async function bundle() {
  // miniflare's `script` takes one module, so the sources are inlined as
  // additional modules rather than bundled by a build step the repo does not
  // otherwise need.
  const read = (name) => readFile(join(root, "src", name), "utf8");
  return {
    index: await read("index.js"),
    log: await read("log.js"),
    tier: await read("tier.js"),
    page: await read("page.js"),
  };
}

before(async () => {
  const src = await bundle();
  mf = new Miniflare({
    modules: [
      { type: "ESModule", path: "index.js", contents: src.index },
      { type: "ESModule", path: "log.js", contents: src.log },
      { type: "ESModule", path: "tier.js", contents: src.tier },
      { type: "ESModule", path: "page.js", contents: src.page },
    ],
    modulesRoot: "/",
    scriptPath: "/index.js",
    durableObjects: { FEED_LOG: "FeedLog" },
    bindings: { INGEST_TOKEN, READER_TOKEN },
    compatibilityDate: "2024-09-23",
  });
  // Surface a startup failure here rather than as a confusing 500 later.
  await mf.ready;
});

after(async () => {
  await mf?.dispose();
});

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
let counter = 0;

/** A ULID with a controllable timestamp, so age-based tier rules are testable
 *  without waiting an hour.
 *
 *  The counter is encoded in Crockford base32 directly. An earlier version used
 *  toString(32).toUpperCase() and mapped the letters Crockford omits (I L O U)
 *  to "0", which collided — 400 distinct events minted in one millisecond
 *  deduplicated down to 294, and the log was right to reject them.
 */
function ulid(ms = Date.now()) {
  let time = "";
  let value = Math.floor(ms);
  for (let i = 0; i < 10; i++) {
    time = CROCKFORD[value % 32] + time;
    value = Math.floor(value / 32);
  }
  let n = counter++;
  let rand = "";
  for (let i = 0; i < 16; i++) {
    rand = CROCKFORD[n % 32] + rand;
    n = Math.floor(n / 32);
  }
  return time + rand;
}

function envelope(overrides = {}) {
  const ms = overrides.ms ?? Date.now();
  delete overrides.ms;
  return {
    v: 1,
    id: ulid(ms),
    ts: new Date(ms).toISOString(),
    type: "transcript.final",
    source: "test-host",
    group: "vancouver-island",
    stream: "Mid Island",
    tier: "sensitive",
    data: { text: "engine three responding", duration_s: 4.2 },
    render: { plain: "📻 Mid Island (4.2s): engine three responding", discord: "x" },
    ...overrides,
  };
}

const publicEvent = (overrides = {}) =>
  envelope({ type: "tone.page", tier: "public", ...overrides });

function url(path) {
  return `https://feed.example${path}`;
}

async function post(path, body, token = INGEST_TOKEN) {
  return mf.dispatchFetch(url(path), {
    method: "POST",
    headers: token ? { Authorization: `Bearer ${token}` } : {},
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
}

async function get(path, token) {
  return mf.dispatchFetch(url(path), {
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });
}

/** Read an SSE stream until `count` frames have arrived or the budget expires.
 *  Returns the raw text so framing itself can be asserted. */
async function readFrames(response, count, budgetMs = 4000) {
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let text = "";
  const deadline = Date.now() + budgetMs;
  while (Date.now() < deadline) {
    const framesSoFar = (text.match(/\n\n/g) || []).length;
    if (framesSoFar >= count) break;
    const chunk = await Promise.race([
      reader.read(),
      new Promise((resolve) => setTimeout(() => resolve({ timedOut: true }), 250)),
    ]);
    if (chunk.timedOut) continue;
    if (chunk.done) break;
    text += decoder.decode(chunk.value, { stream: true });
  }
  reader.cancel().catch(() => {});
  return text;
}

function dataFrames(text) {
  return text
    .split("\n\n")
    .filter((f) => f.includes("data:"))
    .map((f) => JSON.parse(f.slice(f.indexOf("data: ") + 6)));
}

// ---------------------------------------------------------------------------
describe("ingest", () => {
  it("rejects an unauthenticated write", async () => {
    const response = await post("/v1/ingest", [envelope()], null);
    assert.equal(response.status, 401);
  });

  it("rejects a wrong token", async () => {
    const response = await post("/v1/ingest", [envelope()], "nope");
    assert.equal(response.status, 401);
  });

  it("does not accept the reader token for writes", async () => {
    const response = await post("/v1/ingest", [envelope()], READER_TOKEN);
    assert.equal(response.status, 401);
  });

  it("accepts a batch", async () => {
    const response = await post("/v1/ingest", [envelope(), envelope(), envelope()]);
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.accepted, 3);
    assert.equal(body.duplicates, 0);
  });

  it("accepts a bare object as a batch of one", async () => {
    const body = await (await post("/v1/ingest", envelope())).json();
    assert.equal(body.accepted, 1);
  });

  it("accepts {events: [...]}", async () => {
    const body = await (await post("/v1/ingest", { events: [envelope()] })).json();
    assert.equal(body.accepted, 1);
  });

  it("is idempotent on the envelope id", async () => {
    // The whole point of a stable ULID: a retried send must not show a
    // subscriber the same transmission twice.
    const event = envelope();
    const first = await (await post("/v1/ingest", [event])).json();
    const again = await (await post("/v1/ingest", [event])).json();
    assert.equal(first.accepted, 1);
    assert.equal(again.accepted, 0);
    assert.equal(again.duplicates, 1);
  });

  it("de-duplicates within a single batch too", async () => {
    const event = envelope();
    const body = await (await post("/v1/ingest", [event, event])).json();
    assert.equal(body.accepted, 1);
    assert.equal(body.duplicates, 1);
  });

  it("rejects malformed JSON", async () => {
    const response = await post("/v1/ingest", "{not json");
    assert.equal(response.status, 400);
  });

  it("rejects an event missing required fields", async () => {
    const response = await post("/v1/ingest", [{ id: "x" }]);
    assert.equal(response.status, 400);
  });

  it("rejects an oversized batch", async () => {
    const batch = Array.from({ length: 501 }, () => envelope());
    const response = await post("/v1/ingest", batch);
    assert.equal(response.status, 413);
  });

  it("accepts an empty batch without writing", async () => {
    const body = await (await post("/v1/ingest", [])).json();
    assert.equal(body.accepted, 0);
  });

  it("serialises concurrent appends without losing any", async () => {
    // This is the bug the Durable Object exists to fix: the KV version did a
    // read-modify-write on one key, so simultaneous posts clobbered each other.
    const batches = Array.from({ length: 8 }, () => [envelope(), envelope()]);
    const responses = await Promise.all(batches.map((b) => post("/v1/ingest", b)));
    const totals = await Promise.all(responses.map((r) => r.json()));
    const accepted = totals.reduce((sum, t) => sum + t.accepted, 0);
    assert.equal(accepted, 16, "every event in every concurrent batch must land");
  });
});

// ---------------------------------------------------------------------------
describe("replay", () => {
  it("returns events oldest first", async () => {
    const batch = [envelope(), envelope(), envelope()];
    await post("/v1/ingest", batch);
    const body = await (await get("/v1/events?limit=500", INGEST_TOKEN)).json();
    const ids = body.events.map((e) => e.id);
    assert.deepEqual([...ids].sort(), ids, "ULID order is time order");
  });

  it("a cursor excludes the event it names", async () => {
    const batch = [envelope(), envelope(), envelope()];
    await post("/v1/ingest", batch);
    const all = await (await get("/v1/events?limit=500", INGEST_TOKEN)).json();
    const middle = all.events[all.events.length - 2].id;
    const rest = await (await get(`/v1/events?since=${middle}`, INGEST_TOKEN)).json();
    assert.ok(!rest.events.some((e) => e.id === middle), "since is exclusive");
  });

  it("paginates with has_more and a usable cursor", async () => {
    await post("/v1/ingest", Array.from({ length: 5 }, () => envelope()));
    const first = await (await get("/v1/events?limit=2", INGEST_TOKEN)).json();
    assert.equal(first.events.length, 2);
    assert.equal(first.has_more, true);
    const second = await (
      await get(`/v1/events?since=${first.cursor}&limit=2`, INGEST_TOKEN)
    ).json();
    const overlap = second.events.filter((e) => first.events.some((f) => f.id === e.id));
    assert.equal(overlap.length, 0, "pages must not overlap");
  });

  it("ignores a malformed cursor rather than failing", async () => {
    const response = await get("/v1/events?since=not-a-ulid", INGEST_TOKEN);
    assert.equal(response.status, 200);
  });

  it("caps the limit", async () => {
    const response = await get("/v1/events?limit=99999", INGEST_TOKEN);
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.ok(body.events.length <= 500);
  });
});

// ---------------------------------------------------------------------------
describe("tiers", () => {
  const hoursAgo = (n) => Date.now() - n * 60 * 60 * 1000;

  it("withholds old transcripts from an unauthenticated reader", async () => {
    const old = envelope({ ms: hoursAgo(3) });
    await post("/v1/ingest", [old]);
    const body = await (await get("/v1/events?limit=500")).json();
    assert.ok(!body.events.some((e) => e.id === old.id), "3h-old transcript is withheld");
    assert.ok(body.withheld > 0);
  });

  it("shows recent transcripts to anyone", async () => {
    const fresh = envelope({ ms: hoursAgo(0) });
    await post("/v1/ingest", [fresh]);
    const body = await (await get("/v1/events?limit=500")).json();
    assert.ok(body.events.some((e) => e.id === fresh.id), "inside the live window");
  });

  it("shows old public events to anyone", async () => {
    const old = publicEvent({ ms: hoursAgo(72) });
    await post("/v1/ingest", [old]);
    const body = await (await get("/v1/events?limit=500")).json();
    assert.ok(body.events.some((e) => e.id === old.id), "CAD and tones are already public");
  });

  it("shows old transcripts to a reader token", async () => {
    const old = envelope({ ms: hoursAgo(5) });
    await post("/v1/ingest", [old]);
    const body = await (await get("/v1/events?limit=500", READER_TOKEN)).json();
    assert.ok(body.events.some((e) => e.id === old.id));
    assert.equal(body.authorised, true);
  });

  it("the ingest token also reads, so the capture host can replay itself", async () => {
    const old = envelope({ ms: hoursAgo(5) });
    await post("/v1/ingest", [old]);
    const body = await (await get("/v1/events?limit=500", INGEST_TOKEN)).json();
    assert.ok(body.events.some((e) => e.id === old.id));
  });

  it("withholds an undateable transcript rather than guessing", async () => {
    const broken = envelope({ ts: "not a date" });
    await post("/v1/ingest", [broken]);
    const body = await (await get("/v1/events?limit=500")).json();
    assert.ok(!body.events.some((e) => e.id === broken.id));
  });

  it("strips the clip url but keeps the rest of the audio block", async () => {
    const withAudio = envelope({
      data: {
        text: "x",
        audio: { id: "a".repeat(64), codec: "opus", duration_s: 2, url: "https://clips/x" },
      },
    });
    await post("/v1/ingest", [withAudio]);
    const body = await (await get("/v1/events?limit=500")).json();
    const seen = body.events.find((e) => e.id === withAudio.id);
    assert.ok(seen, "a fresh transcript is inside the live window");
    assert.equal(seen.data.audio.url, null, "fetching the clip needs a token");
    assert.equal(seen.data.audio.codec, "opus", "its existence is not the secret");
  });

  it("keeps the clip url for an authorised reader", async () => {
    const withAudio = envelope({
      data: { text: "x", audio: { id: "b".repeat(64), url: "https://clips/y" } },
    });
    await post("/v1/ingest", [withAudio]);
    const body = await (await get("/v1/events?limit=500", READER_TOKEN)).json();
    const seen = body.events.find((e) => e.id === withAudio.id);
    assert.equal(seen.data.audio.url, "https://clips/y");
  });

  it("advances the cursor past withheld events", async () => {
    // Otherwise a reader whose whole page is withheld re-requests the same
    // range forever and never reaches the events it may see.
    await post("/v1/ingest", [envelope({ ms: hoursAgo(9) })]);
    const body = await (await get("/v1/events?limit=1")).json();
    assert.ok(body.cursor, "a fully withheld page still reports where it read to");
  });
});

// ---------------------------------------------------------------------------
describe("live tail", () => {
  it("delivers events appended after connecting", async () => {
    const response = await get("/v1/stream", INGEST_TOKEN);
    assert.equal(response.headers.get("content-type"), "text/event-stream; charset=utf-8");

    const event = envelope();
    const frames = readFrames(response, 2);
    await new Promise((r) => setTimeout(r, 100));
    await post("/v1/ingest", [event]);
    const text = await frames;

    const seen = dataFrames(text);
    assert.ok(seen.some((e) => e.id === event.id), "the live event arrived");
    assert.ok(text.includes(`id: ${event.id}`), "framed with its ULID as the SSE id");
    assert.ok(text.includes("event: transcript.final"), "and its type as the event name");
  });

  it("backfills from a cursor before going live", async () => {
    const missed = envelope();
    await post("/v1/ingest", [missed]);
    const all = await (await get("/v1/events?limit=500", INGEST_TOKEN)).json();
    const before = all.events[all.events.length - 2];

    const response = await get(`/v1/stream?since=${before.id}`, INGEST_TOKEN);
    const text = await readFrames(response, 2);
    assert.ok(
      dataFrames(text).some((e) => e.id === missed.id),
      "a reconnect must not silently lose the gap"
    );
  });

  it("honours Last-Event-ID as a cursor", async () => {
    const missed = envelope();
    await post("/v1/ingest", [missed]);
    const all = await (await get("/v1/events?limit=500", INGEST_TOKEN)).json();
    const before = all.events[all.events.length - 2];

    const response = await mf.dispatchFetch(url("/v1/stream"), {
      headers: { Authorization: `Bearer ${INGEST_TOKEN}`, "Last-Event-ID": before.id },
    });
    const text = await readFrames(response, 2);
    assert.ok(dataFrames(text).some((e) => e.id === missed.id));
  });

  it("withholds sensitive backfill from an unauthenticated tail", async () => {
    // The replay path is not the only way to read the log. An unfiltered
    // subscriber would be handed every transcript for as long as it stayed
    // connected, which is the exact thing the tiers exist to prevent.
    const fourHoursAgo = Date.now() - 4 * 60 * 60 * 1000;
    const old = envelope({ ms: fourHoursAgo });
    await post("/v1/ingest", [old]);

    // The cursor has to predate the event. Its ULID is back-dated too, so it
    // sorts into the middle of the log rather than onto the end — taking
    // "the second-to-last event" as a cursor would skip straight past it.
    const since = ulid(fourHoursAgo - 60 * 60 * 1000);

    const response = await get(`/v1/stream?since=${since}`);
    const text = await readFrames(response, 40, 3000);
    assert.ok(
      !dataFrames(text).some((e) => e.id === old.id),
      "an old transcript must not reach an unauthenticated tail"
    );
    assert.ok(text.includes(`id: ${old.id}`), "but its id still goes out");
    assert.ok(text.includes("withheld"), "marked as withheld, so the cursor advances");
  });

  it("delivers the same backfill to an authorised tail", async () => {
    const fourHoursAgo = Date.now() - 4 * 60 * 60 * 1000;
    const old = envelope({ ms: fourHoursAgo });
    await post("/v1/ingest", [old]);
    const since = ulid(fourHoursAgo - 60 * 60 * 1000);

    const response = await get(`/v1/stream?since=${since}`, READER_TOKEN);
    const text = await readFrames(response, 40, 3000);
    assert.ok(
      dataFrames(text).some((e) => e.id === old.id),
      "a token unlocks the same range the replay path would give"
    );
  });

  it("opens the stream immediately", async () => {
    const response = await get("/v1/stream");
    const text = await readFrames(response, 1, 2000);
    assert.ok(text.startsWith(": connected"), "a buffering proxy must not stall it");
  });

  it("one subscriber going away does not stop the others", async () => {
    const staying = await get("/v1/stream", INGEST_TOKEN);
    const leaving = await get("/v1/stream", INGEST_TOKEN);
    await leaving.body.cancel();

    const event = envelope();
    const frames = readFrames(staying, 2);
    await new Promise((r) => setTimeout(r, 100));
    await post("/v1/ingest", [event]);
    const text = await frames;
    assert.ok(dataFrames(text).some((e) => e.id === event.id));
  });
});

// ---------------------------------------------------------------------------
describe("v0 compatibility", () => {
  it("still accepts a rendered line", async () => {
    const response = await post("/ingest", { line: "📻 Mid Island: test", type: "transcript" });
    assert.equal(response.status, 200);
  });

  it("rejects an unauthenticated legacy write", async () => {
    const response = await post("/ingest", { line: "x" }, null);
    assert.equal(response.status, 401);
  });

  it("requires a line", async () => {
    const response = await post("/ingest", { type: "transcript" });
    assert.equal(response.status, 400);
  });

  it("a legacy line becomes a real envelope", async () => {
    await post("/ingest", { line: "📟 page", type: "tone" });
    const body = await (await get("/v1/events?limit=500", INGEST_TOKEN)).json();
    const seen = body.events.find((e) => e.render && e.render.plain === "📟 page");
    assert.ok(seen);
    assert.equal(seen.type, "tone.page");
    assert.equal(seen.tier, "public", "a tone page is not a transcript");
  });

  it("/lines still returns the v0 shape", async () => {
    await post("/ingest", { line: "📻 recent", type: "transcript" });
    const body = await (await get("/lines")).json();
    assert.ok(Array.isArray(body));
    const line = body.find((l) => l.text === "📻 recent");
    assert.ok(line, "the page polling /lines keeps working");
    assert.equal(line.type, "transcript");
    assert.ok(line.ts);
  });

  it("serves the page", async () => {
    const response = await get("/");
    assert.equal(response.status, 200);
    assert.match(response.headers.get("content-type"), /text\/html/);
    assert.match(await response.text(), /Scanner Feed/);
  });

  it("404s an unknown route", async () => {
    assert.equal((await get("/nope")).status, 404);
  });
});

// ---------------------------------------------------------------------------
// Review of 704beea. Several of these cover limits the local runtime does not
// enforce — miniflare accepts 500-key storage calls that real Durable Object
// storage rejects — so where a test cannot reach the failure it asserts the
// construction that avoids it instead.
// ---------------------------------------------------------------------------
describe("storage limits", () => {
  it("never builds a storage call larger than the API accepts", async () => {
    const { chunk, STORAGE_BATCH } = await import("../src/log.js");
    assert.equal(STORAGE_BATCH, 128, "the documented multi-key cap");
    for (const size of [0, 1, 127, 128, 129, 500, 1000]) {
      const slices = chunk(Array.from({ length: size }, (_, i) => i));
      assert.ok(
        slices.every((s) => s.length <= STORAGE_BATCH),
        `${size} items produced an oversized slice`
      );
      assert.equal(
        slices.reduce((n, s) => n + s.length, 0),
        size,
        "chunking must not lose or duplicate items"
      );
    }
  });

  it("accepts a batch far larger than one storage call", async () => {
    // miniflare will not reject the oversized get/put this used to make, so
    // this cannot prove the fix — but it does prove the batch still lands.
    const batch = Array.from({ length: 400 }, () => envelope());
    const body = await (await post("/v1/ingest", batch)).json();
    assert.equal(body.accepted, 400);

    const seen = new Set();
    let cursor = "";
    for (let page = 0; page < 10; page++) {
      const r = await (
        await get(`/v1/events?since=${cursor}&limit=500`, INGEST_TOKEN)
      ).json();
      for (const e of r.events) seen.add(e.id);
      cursor = r.cursor || cursor;
      if (!r.has_more) break;
    }
    for (const e of batch) assert.ok(seen.has(e.id), "every event is readable back");
  });
});

describe("limit handling", () => {
  it("rejects a limit storage cannot use, rather than 500ing", async () => {
    for (const bad of ["-1", "0", "1.5", "abc", "-999"]) {
      const response = await get(`/v1/events?limit=${bad}`, INGEST_TOKEN);
      assert.equal(response.status, 200, `limit=${bad} should not 500`);
      const body = await response.json();
      assert.ok(Array.isArray(body.events));
    }
  });

  it("clamps to the maximum", async () => {
    const { clampLimit } = await import("../src/log.js");
    assert.equal(clampLimit("99999"), 500);
    assert.equal(clampLimit("-1"), 100, "falls back rather than going negative");
    assert.equal(clampLimit("1.9"), 1, "floors rather than passing a fraction");
    assert.equal(clampLimit(null), 100);
  });
});

describe("malformed bodies", () => {
  it("a JSON null body is a 400, not a 500", async () => {
    // 5xx is the one class the scanner retries forever, so a malformed payload
    // answered with 500 is re-posted indefinitely.
    const response = await post("/v1/ingest", "null");
    assert.equal(response.status, 400);
  });

  it("a JSON string body is a 400", async () => {
    const response = await post("/v1/ingest", '"hello"');
    assert.equal(response.status, 400);
  });

  it("a JSON number body is a 400", async () => {
    assert.equal((await post("/v1/ingest", "42")).status, 400);
  });
});

describe("envelope ids", () => {
  it("rejects an id that is not a ULID", async () => {
    // The log keys by id and leans on ULID ordering for replay, cursors and
    // pruning. A non-ULID sorts outside that range: never pruned, never
    // reachable as a cursor.
    const response = await post("/v1/ingest", [envelope({ id: "abc123" })]);
    assert.equal(response.status, 400);
  });

  it("rejects an id containing a newline", async () => {
    // `id: ${event.id}\n` would otherwise inject arbitrary SSE fields into
    // every subscriber's stream.
    const response = await post("/v1/ingest", [
      envelope({ id: "01ARZ3NDEKTSV4RRFFQ69G5FAV\nevent: spoofed" }),
    ]);
    assert.equal(response.status, 400);
  });

  it("rejects a lowercase id", async () => {
    const response = await post("/v1/ingest", [
      envelope({ id: "01arz3ndektsv4rrffq69g5fav" }),
    ]);
    assert.equal(response.status, 400);
  });
});

describe("/lines returns the newest events", () => {
  it("shows recent lines once the log is larger than a page", async () => {
    // A forward scan returns the *first* keys in the log, so slicing its tail
    // gave the end of the oldest window. Past 500 events the page rendered a
    // weeks-old feed forever, which is a regression against the v0 ring buffer.
    const bulk = Array.from({ length: 300 }, () => publicEvent());
    for (let i = 0; i < bulk.length; i += 100) {
      await post("/v1/ingest", bulk.slice(i, i + 100));
    }
    const marker = publicEvent({
      render: { plain: "NEWEST LINE MARKER", discord: "x" },
    });
    await post("/v1/ingest", [marker]);

    const body = await (await get("/lines")).json();
    assert.ok(body.length > 0);
    assert.ok(
      body.some((l) => l.text === "NEWEST LINE MARKER"),
      "the most recent event must appear in /lines"
    );
    assert.equal(
      body[body.length - 1].text,
      "NEWEST LINE MARKER",
      "and it must be last, since /lines is chronological"
    );
  });

  it("caps at 50 lines", async () => {
    const body = await (await get("/lines")).json();
    assert.ok(body.length <= 50);
  });
});

describe("cursor precedence", () => {
  it("Last-Event-ID beats a stale ?since on the URL", async () => {
    // EventSource reconnects to the URL it was constructed with, so an initial
    // ?since is re-sent on every reconnect while the header carries where the
    // client actually reached. Preferring the URL replays the same backfill
    // after every drop, without bound.
    const first = envelope();
    await post("/v1/ingest", [first]);
    const second = envelope();
    await post("/v1/ingest", [second]);

    const all = await (await get("/v1/events?limit=500", INGEST_TOKEN)).json();
    const stale = all.events[all.events.length - 3];

    const response = await mf.dispatchFetch(
      url(`/v1/stream?since=${stale.id}`),
      {
        headers: {
          Authorization: `Bearer ${INGEST_TOKEN}`,
          "Last-Event-ID": second.id,
        },
      }
    );
    const text = await readFrames(response, 6, 2500);
    const ids = dataFrames(text).map((e) => e.id);
    assert.ok(
      !ids.includes(first.id),
      "the header cursor is newer, so the stale ?since must not replay"
    );
  });
});

describe("gap marking", () => {
  it("tells a subscriber when the backfill was truncated", async () => {
    // Going live after a truncated backfill drops the middle with no frame and
    // no error, and the client's cursor advances past it — making the gap
    // unrecoverable through /v1/events too.
    const { MAX_LIMIT } = await import("../src/log.js");
    const start = ulid(Date.now());
    await post("/v1/ingest", [envelope()]);

    const bulk = Array.from({ length: MAX_LIMIT + 20 }, () => publicEvent());
    for (let i = 0; i < bulk.length; i += 200) {
      await post("/v1/ingest", bulk.slice(i, i + 200));
    }

    const response = await get(`/v1/stream?since=${start}`, INGEST_TOKEN);
    const text = await readFrames(response, MAX_LIMIT + 3, 6000);
    assert.ok(text.includes("event: gap"), "a truncated backfill must say so");
    assert.ok(text.includes("/v1/events?since="), "and say how to recover");
  });

  it("does not mark a gap when the backfill was complete", async () => {
    const start = ulid(Date.now());
    const few = [envelope(), envelope()];
    await post("/v1/ingest", few);
    const response = await get(`/v1/stream?since=${start}`, INGEST_TOKEN);
    const text = await readFrames(response, 3, 2500);
    assert.ok(!text.includes("event: gap"));
  });
});
