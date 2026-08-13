"""
events.py — the structured event envelope.

Every emit site used to build a display string and throw the structure away.
A tone page knows its two frequencies, its lookup key, the matched unit and the
stream it came from; what left the process was "📟 Cowichan Valley PAGE:
634.1/600.9 → Honeymoon Bay Fire Department". Anyone building on Tent Floor had
to scrape that back apart.

This module is the surface they should build against instead. An event is a
dict with a fixed set of top-level fields and a per-type `data` payload:

    {
      "v": 1,
      "id": "01K2ZQ8XJ4M7VN0C3R5T9WBFHD",
      "ts": "2026-08-13T20:57:03.412Z",
      "type": "tone.page",
      "source": "tentfloor-01",
      "group": "vancouver-island",
      "stream": "Cowichan Valley",
      "tier": "public",
      "data": {...},
      "render": {"plain": "...", "discord": "..."}
    }

`render` carries the strings the terminal, Discord and the web UI already
display. It exists so those outputs keep working unchanged, and it is not part
of the stable contract — a consumer that parses it has reinvented the problem
this module exists to solve.

See docs/architecture.md.

Requirements:
    None (stdlib only)
"""

import os
import random
import socket
import threading
from datetime import datetime, timezone

SCHEMA_VERSION = 1

# ---------------------------------------------------------------------------
# Event types
# ---------------------------------------------------------------------------
TRANSCRIPT_FINAL = "transcript.final"
TONE_PAGE = "tone.page"

CAD_INCIDENT_NEW = "cad.incident.new"
CAD_INCIDENT_CLEARED = "cad.incident.cleared"
CAD_UNIT_ADDED = "cad.unit.added"
CAD_UNIT_STATUS = "cad.unit.status"
CAD_NFR_INCIDENT = "cad.nfr.incident"

WILDFIRE_DECLARED = "wildfire.declared"
WILDFIRE_UPDATE = "wildfire.update"
WILDFIRE_REMOVED = "wildfire.removed"

# ---------------------------------------------------------------------------
# Tiers
#
# Sensitivity class, not an access level. The edge combines it with event age:
# `public` is readable by anyone at any depth, `sensitive` is readable
# unauthenticated only within the live window and needs a token beyond it.
#
# CAD and wildfire events are `public` because they are restructured from APIs
# the agencies already publish. Transcripts are `sensitive` because they are
# newly created — radio is broadcast and gone until we write it down. Tone pages
# are unit dispatch identifiers rather than incident detail, so they stay public.
# ---------------------------------------------------------------------------
TIER_PUBLIC = "public"
TIER_SENSITIVE = "sensitive"

# Event type -> the `type` field the v0 Worker feed expects. The web UI keys its
# CSS and its label map off these, so they have to survive until /lines is
# retired. Nothing new should be added here.
LEGACY_LINE_TYPES = {
    TRANSCRIPT_FINAL: "transcript",
    TONE_PAGE: "tone",
    CAD_INCIDENT_NEW: "pulsepoint",
    CAD_INCIDENT_CLEARED: "pulsepoint",
    CAD_UNIT_ADDED: "pulsepoint",
    CAD_UNIT_STATUS: "pulsepoint",
    CAD_NFR_INCIDENT: "nanaimo_fire",
    WILDFIRE_DECLARED: "bc_wildfire",
    WILDFIRE_UPDATE: "bc_wildfire",
    WILDFIRE_REMOVED: "bc_wildfire",
}

# Poller `type` values -> envelope types. The pollers still speak their own
# vocabulary internally; Phase 1 moves them onto the reconciler and this
# translation moves with them.
POLLER_EVENT_TYPES = {
    "new_incident": CAD_INCIDENT_NEW,
    "incident_cleared": CAD_INCIDENT_CLEARED,
    "unit_added": CAD_UNIT_ADDED,
    "unit_status_change": CAD_UNIT_STATUS,
    "nanaimo_fire_incident": CAD_NFR_INCIDENT,
    "wildfire_declared": WILDFIRE_DECLARED,
    "wildfire_update": WILDFIRE_UPDATE,
    "wildfire_removed": WILDFIRE_REMOVED,
}


# ---------------------------------------------------------------------------
# Time
# ---------------------------------------------------------------------------
def utc_now():
    """RFC3339 UTC with milliseconds.

    Every timestamp in the system used to be datetime.now().strftime("%H:%M:%S")
    — local, no date, no zone. Consumers could not order events across midnight,
    survive the DST transition, or line events up against anything external.
    """
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def local_clock():
    """Wall-clock time for terminal output. Display only — never in an event."""
    return datetime.now().strftime("%H:%M:%S")


# ---------------------------------------------------------------------------
# Identity
#
# ULID: 48 bits of millisecond timestamp then 80 bits of randomness, Crockford
# base32. Lexicographic sort matches time order, which is what makes it usable
# as a replay cursor. Within a millisecond the random field is incremented
# rather than redrawn, so IDs minted in a tight loop still sort in emit order.
# ---------------------------------------------------------------------------
_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_ulid_lock = threading.Lock()
_ulid_last_ms = 0
_ulid_last_rand = 0


def _encode(value, length):
    out = []
    for _ in range(length):
        out.append(_CROCKFORD[value & 0x1F])
        value >>= 5
    return "".join(reversed(out))


def new_ulid():
    global _ulid_last_ms, _ulid_last_rand
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    with _ulid_lock:
        if now_ms == _ulid_last_ms:
            # Same millisecond: increment instead of redrawing so ordering holds.
            _ulid_last_rand = (_ulid_last_rand + 1) & ((1 << 80) - 1)
        else:
            _ulid_last_ms = now_ms
            _ulid_last_rand = random.getrandbits(80)
        ms, rand = _ulid_last_ms, _ulid_last_rand
    return _encode(ms, 10) + _encode(rand, 16)


_source_name = None


def source_name():
    """Identifies the capture host, so a second box can publish into one feed."""
    global _source_name
    if _source_name is None:
        _source_name = os.environ.get("TENTFLOOR_SOURCE") or socket.gethostname()
    return _source_name


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------
def envelope(event_type, data, group, stream=None, tier=TIER_PUBLIC,
             render_plain="", render_discord="", ts=None):
    """Build an event. `ts` defaults to now, and is passed in only when the
    producer already captured a more accurate time than the emit site."""
    return {
        "v": SCHEMA_VERSION,
        "id": new_ulid(),
        "ts": ts or utc_now(),
        "type": event_type,
        "source": source_name(),
        "group": group,
        "stream": stream,
        "tier": tier,
        "data": data,
        "render": {"plain": render_plain, "discord": render_discord},
    }


def legacy_line_type(event):
    """The v0 feed's `type` field. Unknown types degrade to "transcript", which
    is what the web UI already falls back to for anything it does not label."""
    return LEGACY_LINE_TYPES.get(event["type"], "transcript")


# ---------------------------------------------------------------------------
# Constructors
#
# The render strings below reproduce the previous output byte for byte. Changing
# them changes what Discord and the web UI display.
# ---------------------------------------------------------------------------
def transcript(group, stream, text, duration_s, engine="whisper.cpp",
               model=None, latency_s=None):
    data = {
        "text": text,
        "duration_s": round(duration_s, 3),
        "engine": engine,
        "model": model,
    }
    if latency_s is not None:
        # Not part of the event's meaning — kept for spotting a GPU falling behind.
        data["latency_s"] = round(latency_s, 3)
    return envelope(
        TRANSCRIPT_FINAL, data, group, stream=stream, tier=TIER_SENSITIVE,
        render_plain=f"📻 {stream} ({duration_s:.1f}s): {text}",
        render_discord=f"📻 **{stream}** ({duration_s:.1f}s): {text}",
    )


def tone_page(group, stream, tone_event):
    """`tone_event` is a TwoToneDetector event: tone_a, tone_b, key, unit."""
    unit = tone_event.get("unit")
    display = unit or "UNKNOWN (logged)"
    data = {
        "tone_key": tone_event["key"],
        "tone_a_hz": tone_event["tone_a"],
        "tone_b_hz": tone_event["tone_b"],
        "unit": unit,
        "known": unit is not None,
    }
    return envelope(
        TONE_PAGE, data, group, stream=stream, tier=TIER_PUBLIC,
        render_plain=f"📟 {stream} PAGE: {tone_event['key']} → {display}",
        render_discord=f"📟 **{stream}** PAGE: {tone_event['key']} → {display}",
    )


def poller(group, poller_event, render_plain, render_discord):
    """Wrap a poller event. The payload is carried through as-is minus its
    internal `type` tag, which the envelope's own type replaces.

    Phase 1 moves the pollers onto the shared reconciler and they will build
    envelopes directly; until then this preserves every field they already
    produce — incident ids, unit lists, coordinates, fire size — all of which
    the display string was discarding.
    """
    raw_type = poller_event.get("type", "")
    event_type = POLLER_EVENT_TYPES.get(raw_type)
    if event_type is None:
        raise ValueError(f"unmapped poller event type: {raw_type!r}")
    data = {k: v for k, v in poller_event.items() if k != "type"}
    return envelope(
        event_type, data, group, stream=None, tier=TIER_PUBLIC,
        render_plain=render_plain, render_discord=render_discord,
    )
