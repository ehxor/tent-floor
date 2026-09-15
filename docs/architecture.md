# Tent Floor event architecture

Status: accepted, in progress
Last updated: 2026-08-13

## Why

Tent Floor currently has three outputs — terminal, Discord, and the Cloudflare
Worker feed — and all three receive the same thing: a pre-rendered display
string. Every emit site flattens a structured event into text and throws the
structure away.

A tone page carries `tone_a`, `tone_b`, the lookup key, the matched unit, and
the stream it came from. What leaves the process is:

    📟 Cowichan Valley PAGE: 634.1/600.9 → Honeymoon Bay Fire Department

The pollers are worse, because they have more to lose: `pulsepoint_poller.py`
produces `incident_id`, `call_type_code`, `address`, and a full `unit_list`;
`bc_wildfire_poller.py` produces `latitude`, `longitude`, `size_ha`, `stage`,
and `nearest_town_km`. All of it collapses into one line, and the Worker keeps
the last 50 of them under a single KV key.

People have started building on top of Tent Floor, and the only surface
available to them is that display string — so they scrape the web UI and break
whenever the formatting changes. This document describes the surface they
should be building against instead.

## Shape

```
capture + enrich  →  local event log  →  output workers  →  edge log  →  consumers
    whisper            store.py           discord            DO         SSE
    tones              events              feed                         replay
    pollers            + state             mqtt                         mqtt
                       + outbox
```

The local SQLite store is the spine, not a side-car. Every event is written
there first, durably, and each output is a cursor reading forward from it.

Two properties follow from that, and they are the point of the whole exercise:

- The transcription path never blocks on a network call. Today
  `send_to_discord` and `send_to_feed` are synchronous `urlopen` calls with a
  5 second timeout, invoked inline from the consumer loop. Two slow outputs can
  stall that loop for 10 seconds while `work_queue` (maxsize 20) backs up and
  starts dropping transmissions — so a Discord hiccup costs you audio that was
  never transcribed. Both functions swallow the failure in a bare
  `except: pass`, so it is invisible when it happens.
- An output that is down falls behind instead of losing data. Its cursor stops
  advancing and catches up when the endpoint returns.

### Log, not bus

Subscribers need both liveness and history, and neither pub/sub nor polling
gives you both. MQTT alone means a subscriber that connects gets nothing until
the next event fires, and a subscriber that drops for a minute silently loses
that window. A feed with history but no push means everyone polls.

So the canonical form is an append-only log with monotonic IDs, reachable two
ways over the same cursor:

| Endpoint | Purpose |
|---|---|
| `GET /v1/events?since=<id>` | backfill and replay |
| `GET /v1/stream?since=<id>` | SSE live tail, resumes via `Last-Event-ID` |
| MQTT topics | optional mirror, documented as live-only and lossy |

Event IDs are therefore load-bearing: monotonic, stable across restarts, and
unique across capture hosts. ULID, plus a `source` field naming the host.

## Envelope

```json
{
  "v": 1,
  "id": "01K2ZQ8XJ4M7VN0C3R5T9WBFHD",
  "ts": "2026-08-13T20:57:03.412Z",
  "type": "tone.page",
  "source": "tentfloor-01",
  "group": "vancouver-island",
  "stream": "Cowichan Valley",
  "tier": "public",
  "data": {
    "tone_key": "634.1/600.9",
    "tone_a_hz": 634.1,
    "tone_b_hz": 600.9,
    "unit": "Honeymoon Bay Fire Department",
    "known": true
  },
  "render": {
    "plain": "📟 Cowichan Valley PAGE: 634.1/600.9 → Honeymoon Bay Fire Department",
    "discord": "📟 **Cowichan Valley** PAGE: 634.1/600.9 → Honeymoon Bay Fire Department"
  }
}
```

`render` exists so that Discord and the current web UI keep working unchanged.
It is **explicitly unstable** and documented as such: consumers that parse it
have reinvented the problem this whole design exists to solve.

### Event types

| Type | Source | Tier |
|---|---|---|
| `transcript.final` | whisper | sensitive |
| `tone.page` | tone detector | public |
| `cad.incident.new` | PulsePoint | public |
| `cad.incident.cleared` | PulsePoint | public |
| `cad.unit.added` | PulsePoint | public |
| `cad.unit.status` | PulsePoint | public |
| `cad.nfr.incident` | Nanaimo Fire | public |
| `wildfire.declared` | BC Wildfire | public |
| `wildfire.update` | BC Wildfire | public |
| `wildfire.removed` | BC Wildfire | public |
| `stream.up` / `stream.down` | stream watchdog | public |

### Cross-cutting rules

| Decision | Choice |
|---|---|
| Identity | ULID, plus `source` host field so a second capture box can join later |
| Mutability | Events are immutable. A re-transcription is a new event carrying `revises: <id>` |
| Provenance | Transcripts carry `engine`, `model`, and confidence where available |
| Time | RFC3339 UTC at the source. Local time is a display concern only |
| `render` | Present, unstable, never to be parsed |

Provenance matters because transcripts are machine guesses — the existence of
`hallucinations.txt` is an admission that they are sometimes fiction. A
consumer building alerting on this feed needs to know that.

## Access tiers

Content splits into two classes with very different risk profiles.

*Already public*: PulsePoint, BC Wildfire, and Nanaimo Fire come from public
APIs the agencies publish themselves. Restructuring them is not a new
disclosure.

*Newly created*: Whisper transcripts and audio clips. Radio is broadcast and
gone; nobody can search what was said on Mid Island at 3am last Tuesday.
Transcribing, storing, and indexing it creates a permanent machine-readable
record that did not previously exist.

The meaningful axis is therefore retention and searchability rather than
access. A live tail is roughly what a scanner in a kitchen already does. An
indexed archive is a searchable database of who had a medical emergency at
which address.

**Posture: open live, gated depth.**

| | Unauthenticated | Token |
|---|---|---|
| Live SSE tail | yes | yes |
| Recent window | last 1 hour | full retention |
| `public` tier events | yes | yes |
| `sensitive` tier beyond the window | no | yes |
| Audio clip URLs | stripped | signed, time-limited |

Retention: transcripts and poller events 30 days, audio clips 7 days.

The policy can move later; the mechanism cannot be retrofitted cheaply. Tier on
every event, tokens accepted from day one, retention configurable per class —
with those in place, loosening is a config change, while tightening means
breaking every consumer at once and asking Google to de-index you.

Two related obligations worth stating plainly: BC's PIPA governs personal
information handling, and transcripts of EMS dispatch can contain patient names
and addresses with none of the suppression PulsePoint applies to its own
medical call types. Separately, the premium Broadcastify feeds are redistributed
here in derived form, which is worth confirming against their terms before the
firehose is public.

## Audio

Clips are kept — a transcript of garbled radio is not verifiable without them,
and they are the raw material for tuning `jargon.txt` and `hallucinations.txt`.

- Encoded to Opus at ~16 kbps mono, which is transparent for voice and roughly
  60× smaller than the WAV currently built for whisper.
- Written **before** transcription, so the clip survives a whisper timeout or
  crash.
- Content-addressed filenames, tracked in a `clips` table with an expiry.
- Referenced from the envelope as
  `audio: { duration_s, url, expires_at }`; the URL is stripped for
  unauthenticated consumers.

## Relationship to PR #7

[PR #7](https://github.com/ehxor/tent-floor/pull/7) introduced a SQLite-backed
`store.py` with a generic `Reconciler`, replacing the per-poller in-memory
change detection. It is not being merged as-is, but its core is carried forward
here — the design below is a superset of it, and the implementation cherry-picks
from `cff2a1b`.

Kept:

- The `Reconciler` snapshot-diff algorithm and its
  appeared/changed/disappeared/reappeared vocabulary.
- Per-scope isolation. The shipped config polls PulsePoint agency EMS1201 twice
  with different unit prefixes and runs BC Wildfire in both groups; without
  scoping, each view reads the other's incidents as missing and the pair flaps
  between cleared and re-declared.
- Lifecycle columns (`first_seen`, `last_seen`, `gone_at`) and the retention
  sweep.
- `--seed`, so a first start against an empty database does not announce
  everything currently active.
- The equivalence-testing method: freeze the previous implementations in
  `tests/legacy_trackers.py` and assert the ported versions emit identical
  events.

Changed: PR #7 stores current state and returns `Change` objects to callers who
format strings. Here the store also owns the append-only event log and the
per-output cursors, so reconciliation appends envelopes to the log rather than
invoking a formatting callback. That reshapes the poller signatures and the
`store` config block PR #7 adds, which is why it is re-landed rather than merged
and then immediately rewritten.

## Phases

Phases 0–2 are internal. They change no external surface and can land while the
tiering details are still settling.

**Phase 0 — foundations.** RFC3339 UTC at every source. New `events.py` holding
the envelope, typed constructors per event type, and a renderer that reproduces
today's strings byte for byte. Emit sites build envelopes; outputs render from
them.

**Phase 1 — durable spine.** PR #7's store, plus an `events` table (monotonic
`seq`, ULID `id`, type, group, stream, tier, JSON payload) and an output cursor
table. Pollers ported onto `Reconciler`. Fixes both the restart replay storm and
the events-missed-during-downtime hole.

**Phase 2 — decouple outputs.** Each output becomes a worker thread reading
forward by cursor, with its own backoff and failure counter. The bare
`except: pass` handlers go away, and the dropped-transmission path with them.

**Phase 3 — audio clips.** Opus encoding, `clips` table, `audio` block in the
envelope, 7 day local sweep, R2 for the served copy.

**Phase 4 — edge becomes a log.** A Durable Object replaces the KV ring buffer.
The current `/ingest` does a read-modify-write against a single key
(`web/scanner-feed/src/index.js:53-66`) with no compare-and-swap, so concurrent
posts clobber each other's appends — and Cloudflare KV allows roughly one write
per second per key, which a busy incident exceeds easily. The DO fixes the race,
the rate limit, and gives somewhere to hold open subscriber connections.

`POST /v1/ingest` accepts batches and is idempotent on event ID. `/v1/events`
and `/v1/stream` are served from the DO with tier enforcement. `/lines` stays as
a deprecated shim so the current UI and existing scrapers survive the cutover.

**Phase 5 — MQTT mirror and docs.** Optional MQTT output on a topic tree shaped
like [trunk-recorder's MQTT plugin](https://github.com/TrunkRecorder/tr-plugin-mqtt),
so existing tooling can point at it:

```
tentfloor/<group>/<stream>/transcript
tentfloor/<group>/<stream>/tone
tentfloor/<group>/cad
tentfloor/<group>/wildfire
tentfloor/<group>/status          # retained
```

Retained `status` topics let a subscriber connecting cold immediately learn
which streams are live. Plus `docs/EVENTS.md` with the schema, the stability
policy, and a short reference SSE consumer — so people copy that instead of
parsing HTML.
