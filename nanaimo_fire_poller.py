"""
nanaimo_fire_poller.py — Poll Nanaimo Fire Rescue's public incident feed.

Fetches GeoJSON incident data from the City of Nanaimo's open API,
tracks which incidents have already been seen, and emits events for
new incidents only.

Usage:
    Integrated into scanner_transcribe.py via --nanaimo-fire flag, or standalone:
        python nanaimo_fire_poller.py

Requirements:
    None (stdlib only)
"""

import json
import sys
import time
import threading
import urllib.request
from datetime import datetime

from store import Store

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
NANAIMO_FIRE_API = "https://www.nanaimo.ca/fire-rescue-incidents/api/data/json"
POLL_INTERVAL_S = 60  # Check every 60 seconds


# ---------------------------------------------------------------------------
# API fetch
# ---------------------------------------------------------------------------
def fetch_incidents():
    """Fetch current incidents from Nanaimo Fire Rescue API."""
    req = urllib.request.Request(NANAIMO_FIRE_API, headers={
        "User-Agent": "Mozilla/5.0",
    })
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        data = json.loads(resp.read().decode())
        return data.get("features", [])
    except Exception as e:
        print(f"[nanaimo-fire] Error fetching data: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Incident tracker
# ---------------------------------------------------------------------------
class NanaimoFireTracker:
    """Track Nanaimo Fire incidents and emit events for new ones.

    This feed is a rolling window of recent incidents, so the only question
    that matters is "have I announced this one already". Change detection is
    delegated to store.Reconciler with no tracked columns — presence only —
    so an incident whose units are updated mid-call does not re-announce.
    """

    def __init__(self, store=None, scope=""):
        """
        Args:
            store: a store.Store. Defaults to an in-memory one, which gives
                the pre-store behaviour of forgetting everything on exit.
            scope: isolates this tracker's rows from other watchers sharing
                the store.
        """
        self.store = store if store is not None else Store(":memory:")
        self.recon = self.store.reconciler("nanaimo_incidents",
                                           key=("incident_id",), scope=scope)

    def update(self, features):
        """Process features and return list of new incident events."""
        if features is None:
            return []

        rows = []
        by_id = {}
        for feature in features:
            props = feature.get("properties", {})
            incident_id = props.get("Id")
            # Note `is None`, not falsiness: an Id of 0 is a real incident.
            if incident_id is None:
                continue

            by_id[incident_id] = props
            rows.append({
                "incident_id": incident_id,
                "call_type": props.get("Type", "Unknown"),
                "location": props.get("Location", "Unknown"),
                "apparatuses": props.get("Apparatuses", ""),
                "timestamp": props.get("Timestamp", ""),
                "time": props.get("Time", ""),
                "date": props.get("Date", ""),
                "raw": json.dumps(feature, sort_keys=True, default=str),
            })

        events = []
        for change in self.recon.sync(rows):
            # Only genuinely new incidents announce. 'reappeared' is ignored
            # on purpose: an incident that drops out of the rolling window and
            # comes back is the same incident, and the pre-store seen_ids set
            # would never have announced it twice either.
            if change.kind != "appeared":
                continue
            props = by_id[change.key[0]]
            events.append({
                "type": "nanaimo_fire_incident",
                "incident_id": change.key[0],
                "call_type": props.get("Type", "Unknown"),
                "location": props.get("Location", "Unknown"),
                "apparatuses": props.get("Apparatuses", ""),
                "timestamp": props.get("Timestamp", ""),
                "time": props.get("Time", ""),
                "date": props.get("Date", ""),
            })

        return events


# ---------------------------------------------------------------------------
# Event formatting
# ---------------------------------------------------------------------------
def format_event(event):
    """Format for terminal output."""
    return (f"🔥 NANAIMO FIRE: {event['call_type']} — {event['location']} "
            f"— Units: {event['apparatuses']}")


def format_event_discord(event):
    """Format for Discord."""
    return (f"🔥 **NANAIMO FIRE: {event['call_type']}** — {event['location']} "
            f"— Units: {event['apparatuses']}")


# ---------------------------------------------------------------------------
# Background poller thread
# ---------------------------------------------------------------------------
class NanaimoFirePoller:
    """Background thread that polls Nanaimo Fire API and calls a callback."""

    def __init__(self, callback=None, poll_interval=POLL_INTERVAL_S, store=None,
                 scope=""):
        """
        Args:
            callback: function called with each event dict.
            poll_interval: seconds between polls.
            store: a store.Store for durable change detection. Without one,
                state is in-memory and restarts forget what was announced.
        """
        self.tracker = NanaimoFireTracker(store=store, scope=scope)
        self.callback = callback
        self.poll_interval = poll_interval
        self.stop_event = threading.Event()
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self._poll_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()

    def _poll_loop(self):
        while not self.stop_event.is_set():
            try:
                features = fetch_incidents()
                if features is not None:
                    # No first-poll suppression: the store already knows what
                    # was announced before this process started, so incidents
                    # that arrived during downtime are no longer swallowed.
                    for event in self.tracker.update(features):
                        if self.callback:
                            self.callback(event)
            except Exception as e:
                print(f"[nanaimo-fire] Poll error: {e}", file=sys.stderr)

            self.stop_event.wait(self.poll_interval)


# ---------------------------------------------------------------------------
# Standalone mode
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="Poll Nanaimo Fire Rescue incidents")
    parser.add_argument("--interval", "-i", type=int, default=POLL_INTERVAL_S,
                        help=f"Poll interval in seconds (default: {POLL_INTERVAL_S})")
    parser.add_argument("--dump", action="store_true",
                        help="Dump current incidents and exit")
    parser.add_argument("--no-initial-suppress", action="store_true",
                        help="Show existing incidents on first load (for testing)")
    args = parser.parse_args()

    if args.dump:
        print("[nanaimo-fire] Fetching current incidents...")
        features = fetch_incidents()
        if features:
            print(f"\n=== {len(features)} incidents ===\n")
            for f in features:
                p = f["properties"]
                print(f"  [{p['Time']}] {p['Type']:25} {p['Location']:45} Units: {p['Apparatuses']}")
        return

    print(f"[nanaimo-fire] Polling every {args.interval}s")
    print("-" * 60)

    tracker = NanaimoFireTracker()
    first = True

    try:
        while True:
            features = fetch_incidents()
            if features is not None:
                events = tracker.update(features)

                if first and not args.no_initial_suppress:
                    print(f"[nanaimo-fire] Loaded {len(events)} existing incidents, watching for new...")
                    first = False
                else:
                    for event in events:
                        ts = datetime.now().strftime("%H:%M:%S")
                        print(f"[{ts}] {format_event(event)}")
                    first = False

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n[exit] Stopped.")


if __name__ == "__main__":
    main()
