"""
bc_wildfire_poller.py -- Poll BC Wildfire Service for wildfire incidents.

Fetches published incident data from the BC Wildfire Service API,
filters by geographic polygon, tracks state changes (status, size,
fire-of-note, name), and emits events for integration with
scanner_transcribe.py.

Usage:
    Integrated into scanner_transcribe.py via config pollers, or standalone:
        python bc_wildfire_poller.py --polygon "lat1,lon1 lat2,lon2 ..."

Requirements:
    None (stdlib only)
"""

import json
import sys
import time
import threading
import urllib.request
from datetime import datetime

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BC_WILDFIRE_API = (
    "https://wildfiresituation.nrs.gov.bc.ca/wfnews-api/publicPublishedIncident"
    "?pageNumber=1&pageRowCount=100"
    "&stageOfControlList=OUT_CNTRL"
    "&stageOfControlList=HOLDING"
    "&stageOfControlList=UNDR_CNTRL"
    "&stageOfControlList=NEW"
    "&newFires=true"
    "&orderBy=lastUpdatedTimestamp%20DESC"
)
POLL_INTERVAL_S = 600  # 10 minutes

STAGE_LABELS = {
    "NEW": "New",
    "OUT_CNTRL": "Out of Control",
    "HOLDING": "Being Held",
    "UNDR_CNTRL": "Under Control",
    "OUT": "Out",
}

# Fields we track for changes
TRACKED_FIELDS = [
    "stageOfControlCode",
    "incidentSizeEstimatedHa",
    "fireOfNoteInd",
    "incidentName",
]


# ---------------------------------------------------------------------------
# Polygon point-in-polygon test (ray casting)
# ---------------------------------------------------------------------------
def point_in_polygon(lat, lon, polygon):
    """Check if a point (lat, lon) is inside a polygon.

    polygon is a list of (lat, lon) tuples.
    Uses the ray-casting algorithm.
    """
    n = len(polygon)
    inside = False
    j = n - 1
    for i in range(n):
        yi, xi = polygon[i]
        yj, xj = polygon[j]
        if ((yi > lat) != (yj > lat)) and \
           (lon < (xj - xi) * (lat - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def parse_polygon(polygon):
    """Parse a polygon into a list of (lat, lon) tuples.

    Accepts either:
      - A list of [lon, lat] pairs (GeoJSON standard, from JSON config)
      - A string like 'lon1,lat1 lon2,lat2 ...' (from CLI, GeoJSON order)
    """
    if isinstance(polygon, list):
        return [(float(p[1]), float(p[0])) for p in polygon]
    points = []
    for pair in polygon.strip().split():
        lon, lat = pair.split(",")
        points.append((float(lat), float(lon)))
    return points


# ---------------------------------------------------------------------------
# API fetch
# ---------------------------------------------------------------------------
def fetch_incidents(fire_centre_code=None):
    """Fetch current wildfire incidents from BC Wildfire Service API."""
    url = BC_WILDFIRE_API
    if fire_centre_code:
        url += f"&fireCentreCode={fire_centre_code}"

    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0",
    })
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        data = json.loads(resp.read().decode())
        return data.get("collection", [])
    except Exception as e:
        print(f"[bc-wildfire] Error fetching data: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Incident tracker
# ---------------------------------------------------------------------------
class BCWildfireTracker:
    """Track BC wildfire incidents within a polygon and emit change events."""

    def __init__(self, polygon=None):
        """
        Args:
            polygon: optional list of (lat, lon) tuples defining the area of
                interest. If None, all incidents are tracked.
        """
        self.polygon = polygon
        self.known_incidents = {}  # incidentGuid -> dict of tracked field values

    def _in_area(self, incident):
        """Check if incident location falls within our polygon."""
        if self.polygon is None:
            return True
        try:
            lat = float(incident.get("latitude", 0))
            lon = float(incident.get("longitude", 0))
        except (ValueError, TypeError):
            return False
        return point_in_polygon(lat, lon, self.polygon)

    def _tracked_snapshot(self, incident):
        """Extract the tracked fields from an incident."""
        return {f: incident.get(f) for f in TRACKED_FIELDS}

    def update(self, incidents):
        """Process incidents and return list of events."""
        if incidents is None:
            return []

        events = []
        current_ids = set()

        for incident in incidents:
            guid = incident.get("incidentGuid")
            if not guid:
                continue

            if not self._in_area(incident):
                continue

            current_ids.add(guid)
            snapshot = self._tracked_snapshot(incident)
            name = incident.get("incidentName", "Unknown")
            label = incident.get("incidentNumberLabel", "")
            fire_year = incident.get("fireYear")
            stage = STAGE_LABELS.get(
                incident.get("stageOfControlCode", ""),
                incident.get("stageOfControlCode", "Unknown")
            )
            size_ha = incident.get("incidentSizeEstimatedHa")
            fire_of_note = incident.get("fireOfNoteInd", False)
            fire_centre = incident.get("fireCentreName", "")

            if guid not in self.known_incidents:
                # Distinguish newly declared fires from fires we're
                # seeing for the first time that already have a status.
                is_new_fire = incident.get("stageOfControlCode") == "NEW"
                events.append({
                    "type": "wildfire_declared" if is_new_fire else "wildfire_new",
                    "guid": guid,
                    "name": name,
                    "label": label,
                    "fire_year": fire_year,
                    "stage": stage,
                    "size_ha": size_ha,
                    "fire_of_note": fire_of_note,
                    "fire_centre": fire_centre,
                    "latitude": incident.get("latitude"),
                    "longitude": incident.get("longitude"),
                })
                self.known_incidents[guid] = snapshot
                continue

            # Check for changes in tracked fields
            prev = self.known_incidents[guid]
            changes = {}
            for field in TRACKED_FIELDS:
                if snapshot[field] != prev[field]:
                    changes[field] = {
                        "old": prev[field],
                        "new": snapshot[field],
                    }

            if changes:
                events.append({
                    "type": "wildfire_update",
                    "guid": guid,
                    "name": name,
                    "label": label,
                    "fire_year": fire_year,
                    "stage": stage,
                    "size_ha": size_ha,
                    "fire_of_note": fire_of_note,
                    "fire_centre": fire_centre,
                    "changes": changes,
                })
                self.known_incidents[guid] = snapshot

        # Fires no longer in the feed (resolved / removed)
        gone = set(self.known_incidents.keys()) - current_ids
        for guid in gone:
            prev = self.known_incidents[guid]
            events.append({
                "type": "wildfire_removed",
                "guid": guid,
                "name": prev.get("incidentName", "Unknown"),
            })
            del self.known_incidents[guid]

        return events


# ---------------------------------------------------------------------------
# Event formatting
# ---------------------------------------------------------------------------
def _format_size(ha):
    if ha is None:
        return "unknown size"
    return f"{ha} ha"


def _format_changes(changes):
    parts = []
    for field, diff in changes.items():
        if field == "stageOfControlCode":
            old = STAGE_LABELS.get(diff["old"], diff["old"])
            new = STAGE_LABELS.get(diff["new"], diff["new"])
            parts.append(f"status: {old} -> {new}")
        elif field == "incidentSizeEstimatedHa":
            parts.append(f"size: {_format_size(diff['old'])} -> {_format_size(diff['new'])}")
        elif field == "fireOfNoteInd":
            if diff["new"]:
                parts.append("now Fire of Note")
            else:
                parts.append("no longer Fire of Note")
        elif field == "incidentName":
            parts.append(f"renamed: {diff['old']} -> {diff['new']}")
    return ", ".join(parts)


def format_event(event):
    """Format for terminal output."""
    t = event["type"]
    if t == "wildfire_declared":
        fon = " [FIRE OF NOTE]" if event.get("fire_of_note") else ""
        return (f"🚨🔥 NEW BC WILDFIRE DECLARED: {event['name']} ({event['label']}) -- "
                f"{_format_size(event['size_ha'])}, {event['fire_centre']}{fon}")
    elif t == "wildfire_new":
        fon = " [FIRE OF NOTE]" if event.get("fire_of_note") else ""
        return (f"🌲🔥 BC WILDFIRE: {event['name']} ({event['label']}) -- "
                f"{event['stage']}, {_format_size(event['size_ha'])}{fon}")
    elif t == "wildfire_update":
        return (f"🌲🔥 BC WILDFIRE UPDATE: {event['name']} ({event['label']}) -- "
                f"{_format_changes(event['changes'])}")
    elif t == "wildfire_removed":
        return f"🌲 BC WILDFIRE REMOVED: {event['name']} -- no longer in feed"
    return str(event)


def _incident_url(event):
    """Build a link to the BC Wildfire incident page, or None."""
    fire_year = event.get("fire_year")
    label = event.get("label")
    if fire_year and label:
        return (f"https://wildfiresituation.nrs.gov.bc.ca/incidents"
                f"?fireYear={fire_year}&incidentNumber={label}&source=map")
    return None


def format_event_discord(event):
    """Format for Discord (with markdown)."""
    t = event["type"]
    url = _incident_url(event)
    link = f"\n<{url}>" if url else ""
    if t == "wildfire_declared":
        fon = " **[FIRE OF NOTE]**" if event.get("fire_of_note") else ""
        return (f"🚨🔥 **NEW BC WILDFIRE DECLARED: {event['name']}** ({event['label']}) -- "
                f"{_format_size(event['size_ha'])}, {event['fire_centre']}{fon}{link}")
    elif t == "wildfire_new":
        fon = " **[FIRE OF NOTE]**" if event.get("fire_of_note") else ""
        return (f"🌲🔥 **BC WILDFIRE: {event['name']}** ({event['label']}) -- "
                f"{event['stage']}, {_format_size(event['size_ha'])}{fon}{link}")
    elif t == "wildfire_update":
        return (f"🌲🔥 **BC WILDFIRE UPDATE: {event['name']}** ({event['label']}) -- "
                f"{_format_changes(event['changes'])}{link}")
    elif t == "wildfire_removed":
        return f"🌲 **BC WILDFIRE REMOVED: {event['name']}** -- no longer in feed"
    return str(event)


# ---------------------------------------------------------------------------
# Background poller thread
# ---------------------------------------------------------------------------
class BCWildfirePoller:
    """Background thread that polls BC Wildfire API and calls a callback."""

    def __init__(self, polygon=None, fire_centre_code=None, callback=None,
                 poll_interval=POLL_INTERVAL_S):
        """
        Args:
            polygon: optional list of (lat, lon) tuples defining the area of
                interest. If None, all incidents returned by the API are tracked.
            fire_centre_code: optional fire centre code to filter API query.
            callback: function called with each event dict.
            poll_interval: seconds between polls (default 600 = 10 min).
        """
        self.tracker = BCWildfireTracker(polygon=polygon)
        self.fire_centre_code = fire_centre_code
        self.callback = callback
        self.poll_interval = poll_interval
        self.stop_event = threading.Event()
        self.thread = None
        self._initial_load = True

    def start(self):
        self.thread = threading.Thread(target=self._poll_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()

    def _poll_loop(self):
        while not self.stop_event.is_set():
            try:
                incidents = fetch_incidents(self.fire_centre_code)
                if incidents is not None:
                    events = self.tracker.update(incidents)

                    # On first load, silently absorb existing incidents
                    if self._initial_load:
                        self._initial_load = False
                        continue

                    for event in events:
                        if self.callback:
                            self.callback(event)
            except Exception as e:
                print(f"[bc-wildfire] Poll error: {e}", file=sys.stderr)

            self.stop_event.wait(self.poll_interval)


# ---------------------------------------------------------------------------
# Standalone mode
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="Poll BC Wildfire Service for incidents")
    parser.add_argument("--polygon", "-p",
                        help="Polygon as 'lon1,lat1 lon2,lat2 ...' (GeoJSON order, "
                             "min 3 points). If omitted, all incidents are tracked.")
    parser.add_argument("--fire-centre", "-f",
                        help="Fire centre code (e.g. 50 for Kamloops)")
    parser.add_argument("--interval", "-i", type=int, default=POLL_INTERVAL_S,
                        help=f"Poll interval in seconds (default: {POLL_INTERVAL_S})")
    parser.add_argument("--dump", action="store_true",
                        help="Dump matching incidents and exit")
    parser.add_argument("--no-initial-suppress", action="store_true",
                        help="Show existing incidents on first load (for testing)")
    args = parser.parse_args()

    polygon = None
    if args.polygon:
        polygon = parse_polygon(args.polygon)
        if len(polygon) < 3:
            print("[error] Polygon must have at least 3 points")
            sys.exit(1)

    if args.dump:
        print("[bc-wildfire] Fetching incidents...")
        incidents = fetch_incidents(args.fire_centre)
        if incidents:
            tracker = BCWildfireTracker(polygon=polygon)
            events = tracker.update(incidents)
            scope = "in polygon" if polygon else "total"
            print(f"\n=== {len(events)} incidents {scope} ===\n")
            for e in events:
                print(f"  {format_event(e)}")
            if polygon:
                print(f"\n({len(incidents)} total incidents from API, "
                      f"{len(events)} within polygon)")
        return

    print(f"[bc-wildfire] Polling every {args.interval}s")
    if polygon:
        print(f"[bc-wildfire] Polygon: {len(polygon)} points")
    else:
        print("[bc-wildfire] No polygon filter (tracking all returned incidents)")
    print("-" * 60)

    tracker = BCWildfireTracker(polygon=polygon)
    first = True

    try:
        while True:
            incidents = fetch_incidents(args.fire_centre)
            if incidents is not None:
                events = tracker.update(incidents)

                if first and not args.no_initial_suppress:
                    print(f"[bc-wildfire] Loaded {len(events)} existing fires in area, "
                          f"watching for changes...")
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
