"""
tools/capture_fixtures.py — record real poll snapshots for equivalence testing.

The synthetic fixtures in tests/fixtures.py pin the reconciliation semantics,
but they were written from the field names the pollers read, not from live
data. This captures the real thing so the port can be validated against what
the APIs actually return.

Run it a few times, minutes apart, so consecutive snapshots contain real
transitions (a unit changing status, an incident clearing):

    python tools/capture_fixtures.py --source pulsepoint --agency EMS1201
    sleep 120
    python tools/capture_fixtures.py --source pulsepoint --agency EMS1201

Then replay the captures through both trackers and diff the events:

    python tools/capture_fixtures.py --replay fixtures/pulsepoint

Requirements:
    Network access to the upstream APIs, and `cryptography` for PulsePoint.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "tests"))

DEFAULT_DIR = "fixtures"


# ---------------------------------------------------------------------------
# Capture
# ---------------------------------------------------------------------------
def capture(source, out_dir, agency=None, fire_centre=None):
    """Fetch one snapshot and write it as a timestamped JSON file."""
    if source == "pulsepoint":
        from pulsepoint_poller import fetch_incidents
        if not agency:
            raise SystemExit("--agency is required for pulsepoint")
        snapshot, _ = fetch_incidents(agency)
    elif source == "nanaimo_fire":
        from nanaimo_fire_poller import fetch_incidents
        snapshot = fetch_incidents()
    elif source == "bc_wildfire":
        from bc_wildfire_poller import fetch_incidents
        snapshot = fetch_incidents(fire_centre)
    else:
        raise SystemExit(f"unknown source: {source}")

    if snapshot is None:
        print(f"[capture] {source}: no data returned "
              f"(PulsePoint alternates empty responses — try again)")
        return None

    target = os.path.join(out_dir, source)
    os.makedirs(target, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(target, f"{stamp}.json")
    with open(path, "w") as f:
        json.dump(snapshot, f, indent=2, default=str)
    print(f"[capture] {source}: {len(snapshot)} record(s) -> {path}")
    return path


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------
def replay(directory, unit_prefixes=None):
    """Feed captured snapshots through both trackers and diff the events.

    This is the real golden-output comparison: it proves the ported tracker
    emits exactly what the pre-store one did, on data the APIs actually sent.
    """
    import legacy_trackers as legacy
    from store import Store

    source = os.path.basename(os.path.normpath(directory))
    files = sorted(f for f in os.listdir(directory) if f.endswith(".json"))
    if len(files) < 2:
        print(f"[replay] {directory}: need at least 2 captures, found {len(files)}")
        return 1

    if source == "pulsepoint":
        import pulsepoint_poller as mod
        old = legacy.IncidentTracker(unit_prefixes=unit_prefixes)
        new = mod.IncidentTracker(unit_prefixes=unit_prefixes, store=Store(":memory:"))
    elif source == "nanaimo_fire":
        import nanaimo_fire_poller as mod
        old = legacy.NanaimoFireTracker()
        new = mod.NanaimoFireTracker(store=Store(":memory:"))
    elif source == "bc_wildfire":
        import bc_wildfire_poller as mod
        old = legacy.BCWildfireTracker()
        new = mod.BCWildfireTracker(store=Store(":memory:"))
    else:
        raise SystemExit(f"unknown source directory: {source}")

    def canonical(events):
        return sorted(json.dumps(e, sort_keys=True, default=str) for e in events)

    failures = 0
    for name in files:
        with open(os.path.join(directory, name)) as f:
            snapshot = json.load(f)

        expected = canonical(old.update(snapshot))
        actual = canonical(new.update(snapshot))

        if expected == actual:
            print(f"[replay] {name}: {len(expected)} event(s) match")
            continue

        failures += 1
        print(f"[replay] {name}: MISMATCH")
        for line in sorted(set(expected) - set(actual)):
            print(f"    only in legacy: {line}")
        for line in sorted(set(actual) - set(expected)):
            print(f"    only in ported: {line}")

        # Formatted output is what actually reaches Discord and the feed.
        for fmt_name in ("format_event", "format_event_discord"):
            fmt = getattr(mod, fmt_name)
            old_text = sorted(fmt(json.loads(e)) for e in expected)
            new_text = sorted(fmt(json.loads(e)) for e in actual)
            if old_text != new_text:
                print(f"    {fmt_name} differs")

    print(f"[replay] {len(files)} snapshot(s), {failures} mismatch(es)")
    return 1 if failures else 0


# ---------------------------------------------------------------------------
# Standalone mode
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Capture and replay real poller snapshots")
    parser.add_argument("--source", choices=["pulsepoint", "nanaimo_fire",
                                             "bc_wildfire"],
                        help="Source to capture from")
    parser.add_argument("--agency", help="PulsePoint agency ID (e.g. EMS1201)")
    parser.add_argument("--fire-centre", help="BC Wildfire fire centre code")
    parser.add_argument("--out", default=DEFAULT_DIR,
                        help=f"Capture directory (default: {DEFAULT_DIR})")
    parser.add_argument("--replay", metavar="DIR",
                        help="Replay a capture directory through both trackers")
    parser.add_argument("--unit-prefix", action="append",
                        help="Unit prefix filter to apply during replay (repeatable)")
    args = parser.parse_args()

    if args.replay:
        sys.exit(replay(args.replay, unit_prefixes=args.unit_prefix))

    if not args.source:
        parser.error("provide --source to capture, or --replay to compare")

    capture(args.source, args.out, agency=args.agency,
            fire_centre=args.fire_centre)


if __name__ == "__main__":
    main()
