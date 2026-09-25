// scanner-feed/src/log.js
//
// FeedLog — the append-only event log, as a Durable Object.
//
// This replaces a ring buffer kept in a single KV key. That shape had two
// problems a Durable Object simply does not have:
//
//   * Ingest was a read-modify-write on one key with no compare-and-swap, so
//     concurrent posts clobbered each other's appends. Every stream and poller
//     writes to the same feed, so that was the normal case, not the edge case.
//   * KV allows roughly one write per second per key. A busy incident exceeds
//     that easily, and the excess is silently dropped.
//
// A Durable Object serialises its own requests, so appends cannot race, and it
// can hold open connections — which is what makes a live tail possible at all.
//
// Events are keyed `e:<ulid>`. ULIDs sort lexicographically in time order, so a
// key range scan *is* the replay, and the cursor a consumer holds stays
// meaningful across capture hosts — which a counter minted here would not.

import { forReader } from "./tier.js";

// Events older than this are pruned. Matches the local store's retention.
export const RETENTION_MS = 30 * 24 * 60 * 60 * 1000;

// Most a single ingest may carry, and most a single read may return.
export const MAX_BATCH = 500;
export const MAX_LIMIT = 500;

// Durable Object storage caps multi-key get/put/delete at 128 items per call
// and throws past it. miniflare does not enforce that, so no test here can
// catch an oversized call — the limit has to be respected by construction.
export const STORAGE_BATCH = 128;

// Prune at most this many keys per ingest, so a long-idle feed catching up
// cannot spend its whole request budget deleting.
const PRUNE_BUDGET = 200;

/** Split `items` into slices no larger than the storage API accepts. */
export function chunk(items, size = STORAGE_BATCH) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

const ULID_RE = /^[0-9A-HJKMNP-TV-Z]{26}$/;
const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/** The lowest ULID that could have been minted at `ms`. Used as a prune
 *  boundary: everything below it is older than the retention window. */
export function ulidFloor(ms) {
  let out = "";
  let value = Math.max(0, Math.floor(ms));
  for (let i = 0; i < 10; i++) {
    out = CROCKFORD[value % 32] + out;
    value = Math.floor(value / 32);
  }
  return out + "0".repeat(16);
}

/** Milliseconds encoded in a ULID's timestamp prefix. */
export function ulidTime(id) {
  let ms = 0;
  for (let i = 0; i < 10; i++) ms = ms * 32 + CROCKFORD.indexOf(id[i]);
  return ms;
}

export function isUlid(value) {
  return typeof value === "string" && ULID_RE.test(value);
}

/** A whole number in [1, MAX_LIMIT]. Anything else — a negative, a fraction,
 *  a word — becomes the default rather than reaching storage.list(), which
 *  throws a RangeError on a limit it cannot use. */
export function clampLimit(raw, fallback = 100) {
  const n = Math.floor(Number(raw));
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.min(n, MAX_LIMIT);
}

const key = (id) => `e:${id}`;

export class FeedLog {
  constructor(state) {
    this.state = state;
    // Open SSE connections. Not persisted: a connection cannot survive the
    // object being evicted, and the client reconnects with Last-Event-ID.
    this.subscribers = new Set();
  }

  async fetch(request) {
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/append":
        return this.append(await request.json());
      case "/read":
        return this.read(url.searchParams);
      case "/latest":
        return this.latest(url.searchParams);
      case "/subscribe":
        return this.subscribe(url.searchParams);
      default:
        return new Response("not found", { status: 404 });
    }
  }

  /** Events after `since`, oldest first.
   *
   *  storage.list()'s `start` is inclusive, so the cursor itself is fetched and
   *  then dropped. Nudging the key with a sentinel byte would save one row and
   *  cost anyone reading this an explanation.
   */
  async after(since, limit) {
    const options = { prefix: "e:", limit: limit + (isUlid(since) ? 1 : 0) };
    if (isUlid(since)) options.start = key(since);
    const found = await this.state.storage.list(options);
    const events = [...found.values()];
    if (isUlid(since) && events.length && events[0].id === since) events.shift();
    return events.slice(0, limit);
  }

  // -- writing --------------------------------------------------------------
  async append(events) {
    const accepted = [];

    // Chunked: MAX_BATCH is 500 and the storage API takes 128 keys per call.
    // An unchunked get would throw before writing anything, the Worker would
    // return 500, and the scanner treats 5xx as retryable — so a catching-up
    // batch would be re-posted forever and never land.
    const stored = new Set();
    for (const slice of chunk(events.map((e) => key(e.id)))) {
      for (const found of (await this.state.storage.get(slice)).keys()) {
        stored.add(found);
      }
    }

    const writes = {};
    for (const event of events) {
      // Idempotent on the envelope's ULID: a retried batch re-sends events the
      // log already has, and re-broadcasting them would show subscribers the
      // same transmission twice.
      //
      // `stored` is a snapshot taken before this loop, so it cannot catch an id
      // repeated *within* this batch — writes would collapse to one key but
      // the event would be counted and broadcast twice. Checking the pending
      // writes as well closes that.
      if (stored.has(key(event.id)) || key(event.id) in writes) continue;
      writes[key(event.id)] = event;
      accepted.push(event);
    }

    if (accepted.length > 0) {
      for (const slice of chunk(Object.entries(writes))) {
        await this.state.storage.put(Object.fromEntries(slice));
      }
      this.broadcast(accepted);
    }

    const pruned = await this.prune();
    return Response.json({
      accepted: accepted.length,
      duplicates: events.length - accepted.length,
      pruned,
    });
  }

  async prune(now = Date.now()) {
    const stale = await this.state.storage.list({
      prefix: "e:",
      end: key(ulidFloor(now - RETENTION_MS)),
      limit: PRUNE_BUDGET,
    });
    if (stale.size === 0) return 0;
    for (const slice of chunk([...stale.keys()])) {
      await this.state.storage.delete(slice);
    }
    return stale.size;
  }

  // -- reading --------------------------------------------------------------
  async read(params) {
    const since = params.get("since");
    const limit = clampLimit(params.get("limit"));
    const events = await this.after(since, limit);
    return Response.json({
      events,
      // A consumer that reads a full page has more waiting; one that does not
      // is caught up. Saying so saves a round trip to discover it.
      has_more: events.length === limit,
      cursor: events.length ? events[events.length - 1].id : since || null,
    });
  }

  /** The newest `limit` events, oldest first.
   *
   *  Distinct from read() because "most recent" cannot be built from a forward
   *  scan: list() returns the *first* keys in ascending order, so taking the
   *  last slice of a forward page gives the tail of the oldest window, not the
   *  newest events. A reverse scan is the only way to reach the end of the log
   *  without walking all of it.
   */
  async latest(params) {
    const limit = clampLimit(params.get("limit"));
    const found = await this.state.storage.list({
      prefix: "e:",
      limit,
      reverse: true,
    });
    // reverse gives newest first; callers want chronological.
    const events = [...found.values()].reverse();
    return Response.json({ events });
  }

  // -- live tail ------------------------------------------------------------
  async subscribe(params) {
    const { readable, writable } = new TransformStream();

    // The tier check has to happen here, not only on the replay path: this is
    // the object that writes the frames, so an unfiltered subscriber would be
    // handed every transcript for as long as it held the stream open.
    const subscriber = {
      writer: writable.getWriter(),
      encoder: new TextEncoder(),
      authorised: params.get("authorised") === "1",
      // Every write goes through this chain. Two reasons: a writer cannot have
      // two writes in flight, and overlapping broadcasts would otherwise
      // interleave frames from different events.
      tail: Promise.resolve(),
    };

    // Read the backfill before publishing the subscriber, so events arriving
    // during the read are broadcast rather than missed — and queue the frames
    // rather than writing them, so they cannot overtake it.
    const since = params.get("since");
    const missed = isUlid(since) ? await this.after(since, MAX_LIMIT) : [];
    this.subscribers.add(subscriber);

    // A comment line opens the stream immediately, so a client behind a proxy
    // that buffers until first byte does not sit waiting for the first event.
    this.enqueue(subscriber, async () => {
      await subscriber.writer.write(subscriber.encoder.encode(": connected\n\n"));
    });
    for (const event of missed) this.queueSend(subscriber, event);

    if (missed.length === MAX_LIMIT) {
      // The backfill hit its ceiling, so there is more between the last
      // backfilled event and live traffic than this stream will carry. Going
      // straight to live here would drop the middle with no frame and no
      // error, and the client's cursor would advance past it — making the gap
      // unrecoverable through /v1/events too. Say so instead.
      const from = missed[missed.length - 1].id;
      this.enqueue(subscriber, async () => {
        await subscriber.writer.write(
          subscriber.encoder.encode(
            `event: gap\ndata: ${JSON.stringify({
              from,
              reason: "backfill truncated",
              hint: `replay with /v1/events?since=${from}`,
            })}\n\n`
          )
        );
      });
    }

    // Returned without awaiting any of the above. Awaiting a write before the
    // Response exists deadlocks: nothing is reading the readable end yet, the
    // write never settles, and workerd cancels the request as a hung Worker.
    return new Response(readable, {
      headers: {
        "Content-Type": "text/event-stream; charset=utf-8",
        "Cache-Control": "no-store",
        // Without this an nginx in front of the Worker buffers the whole
        // stream, which turns a live tail into a very slow download.
        "X-Accel-Buffering": "no",
      },
    });
  }

  /** Append work to a subscriber's write chain, dropping it on failure. */
  enqueue(subscriber, work) {
    subscriber.tail = subscriber.tail.then(work).catch(() => this.drop(subscriber));
    return subscriber.tail;
  }

  drop(subscriber) {
    this.subscribers.delete(subscriber);
    subscriber.writer.close().catch(() => {});
  }

  queueSend(subscriber, event) {
    this.enqueue(subscriber, async () => {
      const visible = forReader(event, { authorised: subscriber.authorised });
      if (!visible) {
        // Withheld — but the id still goes out, or a reconnecting client would
        // resume from before it and be handed the same withheld range forever.
        await subscriber.writer.write(
          subscriber.encoder.encode(`id: ${event.id}\n: withheld\n\n`)
        );
        return;
      }
      // The SSE id is the envelope's ULID, so a reconnecting browser's
      // Last-Event-ID header is already a valid cursor for /v1/events.
      const frame =
        `id: ${visible.id}\n` +
        `event: ${visible.type}\n` +
        `data: ${JSON.stringify(visible)}\n\n`;
      await subscriber.writer.write(subscriber.encoder.encode(frame));
    });
  }

  broadcast(events) {
    // Queued, never awaited: one wedged subscriber must not hold up the ingest
    // every other subscriber is waiting on.
    for (const subscriber of [...this.subscribers]) {
      for (const event of events) this.queueSend(subscriber, event);
    }
  }
}
