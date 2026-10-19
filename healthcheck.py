"""
healthcheck.py — one answer to "is the scanner working right now?"

`store.py --health` answers that for whoever thinks to run it. This is the
same answer in a form a monitor can poll: admin_ui.py serves it at /healthz,
200 when everything is working and 503 when something is not, with the reasons
in the body. It runs in admin_ui.py rather than in the scanner because the
failure most worth catching is the scanner process itself being gone — on
2026-10-18 it exited over one transmission and nothing noticed for a day.

It reads three things the scanner writes:

  * the heartbeat (kv `health/heartbeat`, every 30s): is the process alive,
    and is its transcription loop still coming round?
  * health events since that process started: is any stream stage or poller
    in an open failure run?
  * transcripts: has any stream been silent for longer than its normal lulls?
    This is the failure where ffmpeg is fine and nothing gets transcribed.

Requirements:
    None (stdlib only)
"""

import json
import time
from datetime import datetime

import events
from store import HEALTH_TYPES, _health_subject

# Missing this many heartbeats in a row means the process is gone or wedged.
HEARTBEAT_MISSED = 3
# The main loop comes round at least once a second while idle, and whisper
# gives up after 60s, so a loop this stale is stuck rather than busy.
LOOP_STALE_S = 180
# A failure younger than this is reported as pending, not failing. A single
# PulsePoint timeout or one full queue recovers within a poll or two; paging on
# those would teach you to ignore the page.
DEFAULT_GRACE_S = 120
# Health events are scanned back this far when there is no heartbeat to say
# when the current process started.
FALLBACK_WINDOW_S = 24 * 3600

_FAILING = (events.POLLER_ERROR, events.STREAM_ERROR)
_COMPONENT_TYPES = tuple(t for t in HEALTH_TYPES if not t.startswith("process."))


def _epoch(rfc3339):
    try:
        return datetime.fromisoformat(rfc3339.replace("Z", "+00:00")).timestamp()
    except (AttributeError, ValueError):
        return None


def _age(seconds):
    return events._duration(max(0, seconds))


def _heartbeat(conn):
    row = conn.execute(
        "SELECT value, updated_at FROM kv WHERE ns = 'health' AND key = 'heartbeat'"
    ).fetchone()
    if row is None:
        return None, None
    try:
        return json.loads(row[0]), row[1]
    except (TypeError, ValueError):
        return None, None


def _last_exit(conn, since):
    """The scanner's own account of why it stopped, if it gave one."""
    row = conn.execute(
        "SELECT body FROM events WHERE type = ? AND created_at >= ? "
        "ORDER BY seq DESC LIMIT 1", (events.PROCESS_EXITED, since)).fetchone()
    return json.loads(row[0]) if row else None


def evaluate(conn, now=None, grace_s=DEFAULT_GRACE_S):
    """The scanner's health from a read-only connection to its store.

    Returns a dict whose `status` is "ok" or "failing"; `problems` says why
    when it is failing, and `pending` lists failures still inside the grace
    period. Nothing here carries transcript text — this is safe to hand to a
    monitoring tool that keeps response bodies.
    """
    now = time.time() if now is None else now
    problems, pending = [], []
    result = {"checked_at": now, "scanner": None, "components": [], "streams": []}

    # -- the process ---------------------------------------------------------
    beat, beat_at = _heartbeat(conn)
    alive = False
    started = None
    if beat is None:
        problems.append("no scanner heartbeat recorded — it has not run since "
                        "monitoring was added, or the store is not the one "
                        "it writes to")
    else:
        started = _epoch(beat.get("started_at"))
        interval = beat.get("interval_s") or 30
        beat_age = now - beat_at
        loop_age = now - beat.get("loop_at", beat_at)
        result["scanner"] = {
            "pid": beat.get("pid"),
            "started_at": beat.get("started_at"),
            "heartbeat_age_s": round(beat_age, 1),
            "loop_age_s": round(loop_age, 1),
            "queue_depth": beat.get("queue_depth"),
        }
        if beat_age > interval * HEARTBEAT_MISSED:
            reason = f"scanner heartbeat is {_age(beat_age)} old"
            exited = _last_exit(conn, started or 0)
            if exited is not None:
                reason += f"; {exited['render']['plain']}"
            else:
                reason += "; it recorded no exit, so it was killed or hung"
            problems.append(reason)
        elif loop_age > LOOP_STALE_S:
            problems.append(f"transcription loop has not run for {_age(loop_age)}")
            alive = True
        else:
            alive = True

    # -- stream stages and pollers ------------------------------------------
    # Only since this process started: a Health run does not survive a
    # restart, so an error from before one would otherwise never clear.
    since = started if started is not None else now - FALLBACK_WINDOW_S
    types = list(_COMPONENT_TYPES)
    rows = conn.execute(
        f"SELECT body FROM events WHERE type IN ({', '.join('?' * len(types))}) "
        f"AND created_at >= ? ORDER BY seq DESC", (*types, since)).fetchall()
    latest = {}
    for (body,) in rows:            # newest first, so the first wins
        event = json.loads(body)
        label, key = _health_subject(event)
        latest.setdefault(key + (event.get("group") or "",), (label, event))

    for _key, (label, event) in sorted(latest.items()):
        if event.get("group"):
            label += f" [{event['group']}]"
        data = event.get("data", {})
        failing = event["type"] in _FAILING
        entry = {"subject": label, "state": "failing" if failing else "ok",
                 "ts": event["ts"], "detail": data.get("detail")}
        result["components"].append(entry)
        if not failing:
            continue
        failed_at = _epoch(data.get("since")) or _epoch(event["ts"]) or now
        entry["failing_for_s"] = round(now - failed_at, 1)
        message = (f"{label}: {data.get('detail')} "
                   f"(failing for {_age(now - failed_at)})")
        (problems if now - failed_at >= grace_s else pending).append(message)

    # -- silence -------------------------------------------------------------
    # Only while the process is up: a dead scanner makes every stream silent,
    # and eight extra lines would bury the one that says why.
    streams = (beat or {}).get("streams") or []
    if streams:
        # Twice the longest limit, so a stream with nothing in view is past
        # its limit rather than exactly on it.
        horizon = now - 2 * max(s.get("silence_alert_s") or 0 for s in streams)
        last = dict(conn.execute(
            "SELECT stream, MAX(created_at) FROM events "
            "WHERE type = ? AND created_at > ? GROUP BY stream",
            (events.TRANSCRIPT_FINAL, horizon)).fetchall())
        for s in streams:
            limit = s.get("silence_alert_s")
            heard = last.get(s["name"])
            # A stream cannot be blamed for silence before the process started,
            # and nothing older than the horizon was looked at, so with no
            # transcript in view the silence is only known to be at least that.
            reference = max(x for x in (heard, started, horizon) if x is not None)
            quiet = now - reference
            silent = bool(limit) and quiet > limit
            result["streams"].append({
                "name": s["name"], "group": s.get("group"),
                "last_transcript_age_s": round(now - heard, 1) if heard else None,
                "silence_alert_s": limit,
                "state": "silent" if silent else "ok",
            })
            if silent and alive:
                at_least = "" if heard else "at least "
                problems.append(f"{s['name']} [{s.get('group')}]: no transcript "
                                f"for {at_least}{_age(quiet)} "
                                f"(alert after {_age(limit)})")

    result["status"] = "failing" if problems else "ok"
    result["problems"] = problems
    result["pending"] = pending
    return result
