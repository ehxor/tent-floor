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
whisper hallucinates, so a consumer reading a transcript has no way to tell a
clean transcription from an invented one; the clip is the ground truth.

- Encoded to Opus at ~16 kbps mono, which is transparent for voice. Raw capture
  is 16 kHz 16-bit mono (256 kbps), so a clip is about **16× smaller** than the
  WAV already built for whisper — roughly 2 kB per second of speech. An hour of
  actual transmissions a day costs about 50 MB per stream across the 7 day
  retention; six hours a day, about 300 MB.
- Written **before** transcription, so the clip survives a whisper timeout or
  crash. That guarantee hinges on the caller being able to tell "nothing was
  said" from "whisper failed": `transcribe_chunk` returns `""` for the first
  and `None` for the second, and only the first deletes the clip. A wedged GPU,
  an OOM or a blown 60s budget leaves the audio on disk for the retention
  window — it is exactly the audio worth hearing.
- Content-addressed on the sha256 of the PCM, so identical audio is stored once
  and a retry cannot produce a second copy. Two-level directory fan-out, since
  a week of a busy scanner is a lot of files for one directory. A write that
  matches stored audio is flagged `reused` and extends the expiry, so the
  second transcript does not advertise the first one's deadline — and a reused
  clip is never deleted, because an earlier event still references it.
- Tracked in a `clips` table on its own retention clock, shorter than the log's.
  The events outlive the clips: an expired clip leaves the transcript intact
  with a dead reference, which is the intended shape rather than a bug.
- Swept **hourly**, not only at startup. The scanner is built to stay up for
  months — it restarts ffmpeg subprocesses, not the process — so a startup-only
  sweep is close to never, and clips would grow without bound against a
  retention figure that says otherwise. The same thread sweeps the event log.
- Referenced from the envelope as
  `audio: { id, codec, duration_s, bytes, expires_at, url }`. The local path is
  deliberately absent — it is meaningless off the host and would leak the
  filesystem layout to every subscriber. `url` stays `null` until there is
  somewhere to serve clips from, and is stripped for unauthenticated consumers
  once there is.
- Requires ffmpeg with libopus. If it is missing, clips are disabled with a
  message saying so rather than falling back to WAV, which would be 16× the
  bytes for the same week of retention.

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

One thing tightened on the way in. PR #7 scoped each reconciler by group and
poller type, which is sufficient for the shipped config — but only because its
two PulsePoint watchers and its two wildfire watchers happen to sit in different
groups. Since what actually distinguishes two watchers is their upstream filter,
the filter is now part of the scope (`poller_scope()`), and `check_scopes()`
refuses to start a config where two watchers would collide. The store itself
cannot make this check, because it never sees the filter.

The scope is a durable key, so it deliberately excludes anything that is not
part of a watcher's identity. Changing a filter does change the scope, and that
is correct: a different filter is a different view, and it should start from its
own state rather than inherit rows it never saw.

## Phases

Phases 0–2 are internal. They change no external surface and can land while the
tiering details are still settling.

**Phase 0 — foundations.** *Landed.* RFC3339 UTC at every source. `events.py`
holding the envelope, typed constructors per event type, and a renderer that
reproduces today's strings byte for byte. Emit sites build envelopes; outputs
render from them.

**Phase 1 — durable spine.** *Landed.* PR #7's store, plus an `events` table
(monotonic `seq`, ULID `id`, type, group, stream, tier, JSON payload) and an
`output_cursors` table. Pollers ported onto `Reconciler`. Fixes both the restart
replay storm and the events-missed-during-downtime hole.

Two properties the log commits to, because consumers will lean on them:

- **Append is idempotent on the envelope's ULID.** Replaying a batch returns the
  original `seq` rather than writing a second copy, so a retried send or a
  replayed fixture cannot duplicate.
- **A cursor only moves forward.** `advance()` takes the max, so a late or
  reordered acknowledgement cannot rewind an output and cause a re-send storm.
  Delivery is at-least-once; the stable ULID is what lets the far side
  de-duplicate.

Retention sweeps the log by age, which is a hard bound — a stuck output does not
get to grow the database without limit. But it reports how many unconsumed
events it dropped, because silently discarding a dead output's backlog is how
you find out about it six weeks later.

**Phase 2 — decouple outputs.** *Landed.* Each destination is an
`outputs.OutputWorker` thread reading forward from its own cursor
(`<group>:<kind>`), so emitting is an append and nothing more. One thread per
destination preserves ordering without coordination, and a stalled destination
stalls only itself.

Failures are split into the two kinds that call for different handling.
A `RetryableFailure` — connection refused, timeout, 5xx, or a 429 whose
`Retry-After` is honoured — is retried with backoff indefinitely, because the
whole point is to fall behind rather than lose data. A `PermanentFailure` — a
malformed payload, a deleted webhook — is logged loudly and skipped, because
retrying it forever would wedge every event queued behind it.

The recoverable client errors (401, 403, 408) sit between the two. A rotated
feed token is usually fixable, so discarding the stream the instant it happens
would defeat the point of keeping a cursor — but "usually" is not "always", and
an unbounded retry on a token that is genuinely dead wedges the destination on
one event forever while `emit()` keeps appending behind it and the retention
sweep quietly discards the backlog. So they are retried under a grace period
(`CLIENT_ERROR_GRACE_S`, one hour), and past it they are dropped like a
permanent failure, so the cursor moves and recovery is immediate when the
endpoint returns rather than needing a restart.

The grace measures a *continuous run of client errors*: any successful
delivery clears it, and so does any other kind of failure. A network outage in
the middle is not evidence that the credentials are dead, and letting it
accumulate would drop events for an auth blip that had only just started — the
feed host unreachable for an hour, then returning 401 briefly during a
rotation.

Failure reporting is loud on the way down and on recovery, and re-warns every
`FAILURE_REWARN_S` for as long as it keeps failing, with the pending count.
Warning once and then retrying in silence is the same invisibility that hid the
thread-death bug — it just hides a stuck thread instead of a dead one. The
throttle is checked before the message is built, because the pending count is a
store query taking the same lock the transcription thread needs to append.

A cursor that has never existed is not a cursor at 0. The log accumulates
independently of which destinations exist, so a new output name starting at 0
would re-deliver everything inside the retention window — up to 30 days of
transcripts into a Discord channel in one burst. `ensure_cursor()` seeds a new
cursor at its group's current head; backfilling is a deliberate act rather than
the default.

The worker thread survives anything a destination throws. An exception the HTTP
layer does not anticipate — a URL that lost its scheme raising `ValueError`, or
an `http.client.HTTPException`, which is not an `OSError` — used to unwind the
thread and leave that destination silently unserved for the life of the
process, with nothing watching it.

Backlog is measured against each group's own head. Against the global head it
never reaches zero once a second group exists, so every shutdown burns the full
drain timeout and then reports caught-up groups as behind; it also goes negative
after a retention sweep, since cursors keep their seq while `MAX(seq)` drops.

`wildfire.removed` is now recorded in the log and suppressed at the
destinations (`outputs.SUPPRESSED_TYPES`) rather than never being emitted. The
outward behaviour is unchanged; the log becomes a complete record of what was
seen. The suppression applies on the inline fallback path too, or `--no-store`
would start announcing it.

Without a store there is nowhere to queue, so `--no-store` keeps the old
blocking inline sends as an explicitly degraded path.

**Phase 3 — audio clips.** *Landed.* Opus encoding, `clips` table (migration
004), `audio` block in the envelope, 7 day local sweep including orphan
collection.

Not yet done: there is nowhere *public* to serve clips from, so `url` is always
`null` in the envelope. Wiring that up — R2 or otherwise — belongs with Phase 4,
since the tier enforcement that decides who may see a clip lives at the edge.

`admin_ui.py` is the local counterpart and deliberately not part of that: a
read-only, localhost-by-default page for searching transcripts and playing their
audio on the transcription machine. It is how a clip actually gets listened to
today, and it is the reason keeping the audio is useful before there is any
serving layer at all. It shares the store and the clips directory but nothing
else — no tiering, no cursors, no public surface.

**Phase 4 — edge becomes a log.** *Landed.* A Durable Object (`FeedLog`)
replaces the KV ring buffer. The old `/ingest` did a read-modify-write against
a single key with no compare-and-swap, so concurrent posts clobbered each
other's appends — and Cloudflare KV allows roughly one write per second per
key, which a busy incident exceeds. A DO serialises its own requests, and it
can hold open connections, which is what makes a live tail possible at all.

Events are keyed `e:<ulid>` in DO storage. ULIDs sort lexicographically in time
order, so a key range scan *is* the replay and the cursor a consumer holds stays
meaningful across capture hosts — which a counter minted at the edge would not
be. The SSE `id:` field is that same ULID, so a reconnecting browser's
`Last-Event-ID` is already a valid `since=` for `/v1/events`.

| Route | |
|---|---|
| `POST /v1/ingest` | a batch of envelopes, idempotent on ULID (`INGEST_TOKEN`) |
| `GET /v1/events?since=` | replay, oldest first, with `has_more` and a cursor |
| `GET /v1/stream?since=` | live tail, backfilling the gap before going live |
| `GET /lines` | deprecated v0 shim, still what the page polls |
| `POST /ingest` | deprecated v0 shim, one rendered line |

Tier enforcement lives in `tier.js` because *both* the replay path and the
Durable Object have to apply it. An earlier draft filtered only on replay,
which would have streamed every transcript to anyone holding the stream open —
the exact thing the tiers exist to prevent.

A withheld event still emits its `id` on the stream. Without that a reader
whose whole page is withheld resumes from before it and is handed the same
withheld range forever.

Clip URLs are stripped for unauthenticated readers while the rest of the
`audio` block stays: the clip's existence, length and id are not the secret,
fetching it is. `url` is still `null` in practice — nothing serves clips
publicly yet. `admin_ui.py` is how a clip gets listened to today.

The scanner sends envelopes to `/v1/ingest` and falls back to the v0 shape on a
404, so the Worker and the scanner can deploy in either order without a config
flag that has to be flipped at the right moment.

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
