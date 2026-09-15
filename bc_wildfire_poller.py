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
import math
import os
import sys
import time
import threading
import urllib.request
from datetime import datetime

from store import Store

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BC_WILDFIRE_API_BASE = (
    "https://wildfiresituation.nrs.gov.bc.ca/wfnews-api/publicPublishedIncident"
    "?pageNumber=1&pageRowCount=100"
    "&orderBy=lastUpdatedTimestamp%20DESC"
)
BC_WILDFIRE_ACTIVE_FIRES = (
    BC_WILDFIRE_API_BASE
    + "&stageOfControlList=OUT_CNTRL"
    "&stageOfControlList=HOLDING"
    "&stageOfControlList=UNDR_CNTRL"
    "&stageOfControlList=NEW"
)
BC_WILDFIRE_FIRES_OF_NOTE = (
    BC_WILDFIRE_API_BASE
    + "&stageOfControlList=OUT_CNTRL"
    "&stageOfControlList=HOLDING"
    "&stageOfControlList=UNDR_CNTRL"
    "&stageOfControlList=NEW"
    "&fireOfNoteInd=true"
)
POLL_INTERVAL_S = 600  # 10 minutes

STAGE_LABELS = {
    "NEW": "New",
    "OUT_CNTRL": "Out of Control",
    "HOLDING": "Being Held",
    "UNDR_CNTRL": "Under Control",
    "OUT": "Out",
}

# Fields we track for changes, in the order they appear in update events.
TRACKED_FIELDS = [
    "stageOfControlCode",
    "incidentSizeEstimatedHa",
    "fireOfNoteInd",
    "incidentName",
]

# API field name -> column in the `wildfires` table. Events keep the API
# names so the formatters below are unaffected by the storage schema.
TRACKED_COLUMNS = {
    "stageOfControlCode": "stage_code",
    "incidentSizeEstimatedHa": "size_ha",
    "fireOfNoteInd": "fire_of_note",
    "incidentName": "name",
}


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
# Nearest-town lookup
# ---------------------------------------------------------------------------
_TOWNS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bc_towns.txt")


def _load_towns(path=_TOWNS_FILE):
    """Load towns from TSV file. Returns list of (name, lat, lon)."""
    towns = []
    with open(path, encoding="utf-8") as f:
        next(f)  # skip header
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 3:
                name, lat, lon = parts[0], float(parts[1]), float(parts[2])
                towns.append((name, lat, lon))
    return towns


def _haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance between two points in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# Pre-load towns at import time
_TOWNS = _load_towns()


def nearest_town(lat, lon):
    """Return (name, distance_km) of the nearest town to the given point."""
    best_name = None
    best_dist = float("inf")
    for name, tlat, tlon in _TOWNS:
        d = _haversine_km(lat, lon, tlat, tlon)
        if d < best_dist:
            best_dist = d
            best_name = name
    if best_name is None:
        return None, None
    return best_name, round(best_dist, 1)


# ---------------------------------------------------------------------------
# API fetch
# ---------------------------------------------------------------------------
def _fetch_url(url):
    """Fetch a single API URL and return the collection list, or None on error."""
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


def fetch_incidents(fire_centre_code=None):
    """Fetch current wildfire incidents from BC Wildfire Service API.

    Makes two queries -- one for new fires and one for fires of note --
    and merges the results so we track both.
    """
    suffix = f"&fireCentreCode={fire_centre_code}" if fire_centre_code else ""

    new_fires = _fetch_url(BC_WILDFIRE_ACTIVE_FIRES + suffix)
    fon_fires = _fetch_url(BC_WILDFIRE_FIRES_OF_NOTE + suffix)

    if new_fires is None and fon_fires is None:
        return None

    # Merge by guid, fires-of-note take precedence (more up-to-date fields)
    merged = {}
    for incident in (new_fires or []):
        guid = incident.get("incidentGuid")
        if guid:
            merged[guid] = incident
    for incident in (fon_fires or []):
        guid = incident.get("incidentGuid")
        if guid:
            merged[guid] = incident

    return list(merged.values())


# ---------------------------------------------------------------------------
# Incident tracker
# ---------------------------------------------------------------------------
class BCWildfireTracker:
    """Track BC wildfire incidents within a polygon and emit change events.

    Change detection is delegated to store.Reconciler against the `wildfires`
    table, so tracked state survives a restart. Event payloads are still built
    from the incoming incident rather than from storage, which keeps the exact
    values the API sent (an integer hectare figure stays an integer).
    """

    def __init__(self, polygon=None, store=None, scope=""):
        """
        Args:
            polygon: optional list of (lat, lon) tuples defining the area of
                interest. If None, all incidents are tracked.
            store: a store.Store. Defaults to an in-memory one, which gives
                the pre-store behaviour of forgetting everything on exit —
                handy for --dump and tests.
            scope: isolates this tracker's rows from other wildfire trackers
                sharing the store. Two groups watching different fire centres
                must not see each other's fires as missing.
        """
        self.polygon = polygon
        self.store = store if store is not None else Store(":memory:")
        self.recon = self.store.reconciler(
            "wildfires",
            key=("guid",),
            tracked=tuple(TRACKED_COLUMNS[f] for f in TRACKED_FIELDS),
            scope=scope,
        )

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

    def _to_row(self, incident):
        """Map an API incident onto the `wildfires` columns.

        Tracked columns hold the raw API values (no defaults applied), so the
        change comparison sees exactly what the old snapshot dict saw.
        """
        return {
            "guid": incident.get("incidentGuid"),
            "label": incident.get("incidentNumberLabel", ""),
            "name": incident.get("incidentName"),
            "fire_year": incident.get("fireYear"),
            "stage_code": incident.get("stageOfControlCode"),
            "size_ha": incident.get("incidentSizeEstimatedHa"),
            "fire_of_note": incident.get("fireOfNoteInd"),
            "fire_centre": incident.get("fireCentreName", ""),
            "lat": incident.get("latitude"),
            "lon": incident.get("longitude"),
            "raw": json.dumps(incident, sort_keys=True, default=str),
        }

    def _base_event(self, incident):
        """The fields shared by declared and update events."""
        fire_lat = incident.get("latitude")
        fire_lon = incident.get("longitude")
        try:
            town_name, town_dist = nearest_town(float(fire_lat), float(fire_lon))
        except (TypeError, ValueError):
            town_name, town_dist = None, None

        return {
            "guid": incident.get("incidentGuid"),
            "name": incident.get("incidentName", "Unknown"),
            "label": incident.get("incidentNumberLabel", ""),
            "fire_year": incident.get("fireYear"),
            "stage": STAGE_LABELS.get(
                incident.get("stageOfControlCode", ""),
                incident.get("stageOfControlCode", "Unknown")
            ),
            "size_ha": incident.get("incidentSizeEstimatedHa"),
            "fire_of_note": incident.get("fireOfNoteInd", False),
            "fire_centre": incident.get("fireCentreName", ""),
            "nearest_town": town_name,
            "nearest_town_km": town_dist,
        }

    @staticmethod
    def _changes_to_api_fields(changed_columns):
        """Translate reconciler column diffs back to API field names.

        fireOfNoteInd round-trips through an INTEGER column, so restore the
        bool — consumers of the event should not have to know the schema.
        """
        changes = {}
        for field in TRACKED_FIELDS:
            column = TRACKED_COLUMNS[field]
            if column not in changed_columns:
                continue
            diff = changed_columns[column]
            if field == "fireOfNoteInd":
                changes[field] = {
                    "old": None if diff["old"] is None else bool(diff["old"]),
                    "new": None if diff["new"] is None else bool(diff["new"]),
                }
            else:
                changes[field] = {"old": diff["old"], "new": diff["new"]}
        return changes

    def update(self, incidents):
        """Process incidents and return list of events."""
        if incidents is None:
            return []

        rows = []
        by_guid = {}
        for incident in incidents:
            guid = incident.get("incidentGuid")
            if not guid:
                continue
            if not self._in_area(incident):
                continue
            by_guid[guid] = incident
            rows.append(self._to_row(incident))

        events = []
        for change in self.recon.sync(rows):
            guid = change.key[0]

            # A fire that dropped out of the feed and came back is a genuine
            # new declaration — the pre-store tracker forgot it on removal.
            if change.kind in ("appeared", "reappeared"):
                event = self._base_event(by_guid[guid])
                event["type"] = "wildfire_declared"
                event["latitude"] = by_guid[guid].get("latitude")
                event["longitude"] = by_guid[guid].get("longitude")
                events.append(event)

            elif change.kind == "changed":
                event = self._base_event(by_guid[guid])
                event["type"] = "wildfire_update"
                event["changes"] = self._changes_to_api_fields(change.changed_fields)
                events.append(event)

            elif change.kind == "disappeared":
                events.append({
                    "type": "wildfire_removed",
                    "guid": guid,
                    "name": change.before.get("name"),
                })

        return events


# ---------------------------------------------------------------------------
# Event formatting
# ---------------------------------------------------------------------------
def _format_size(ha):
    if ha is None:
        return "unknown size"
    return f"{ha:,} ha"


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


def _format_nearby(event):
    """Format nearest-town suffix, e.g. ', 4.2 km from Kamloops'."""
    town = event.get("nearest_town")
    dist = event.get("nearest_town_km")
    if town and dist is not None:
        return f", {dist} km from {town}"
    return ""


def format_event(event):
    """Format for terminal output."""
    t = event["type"]
    nearby = _format_nearby(event)
    if t == "wildfire_declared":
        fon = " [FIRE OF NOTE]" if event.get("fire_of_note") else ""
        return (f"🚨🔥 NEW BC WILDFIRE DECLARED: {event['name']} ({event['label']}) -- "
                f"{event['stage']}, {_format_size(event['size_ha'])}, {event['fire_centre']}{nearby}{fon}")
    elif t == "wildfire_update":
        return (f"🌲🔥 BC WILDFIRE UPDATE: {event['name']} ({event['label']}) -- "
                f"{_format_changes(event['changes'])}{nearby}")
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
    nearby = _format_nearby(event)
    if t == "wildfire_declared":
        fon = " **[FIRE OF NOTE]**" if event.get("fire_of_note") else ""
        return (f"🚨🔥 **NEW BC WILDFIRE DECLARED: {event['name']}** ({event['label']}) -- "
                f"{event['stage']}, {_format_size(event['size_ha'])}, {event['fire_centre']}{nearby}{fon}{link}")
    elif t == "wildfire_update":
        return (f"🌲🔥 **BC WILDFIRE UPDATE: {event['name']}** ({event['label']}) -- "
                f"{_format_changes(event['changes'])}{nearby}{link}")
    elif t == "wildfire_removed":
        return f"🌲 **BC WILDFIRE REMOVED: {event['name']}** -- no longer in feed"
    return str(event)


# ---------------------------------------------------------------------------
# Background poller thread
# ---------------------------------------------------------------------------
class BCWildfirePoller:
    """Background thread that polls BC Wildfire API and calls a callback."""

    def __init__(self, polygon=None, fire_centre_code=None, callback=None,
                 poll_interval=POLL_INTERVAL_S, store=None, scope=""):
        """
        Args:
            polygon: optional list of (lat, lon) tuples defining the area of
                interest. If None, all incidents returned by the API are tracked.
            fire_centre_code: optional fire centre code to filter API query.
            callback: function called with each event dict.
            poll_interval: seconds between polls (default 600 = 10 min).
            store: a store.Store for durable change detection. Without one,
                state is in-memory and every restart re-declares active fires.
        """
        self.tracker = BCWildfireTracker(polygon=polygon, store=store, scope=scope)
        self.fire_centre_code = fire_centre_code
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
                incidents = fetch_incidents(self.fire_centre_code)
                if incidents is not None:
                    # No first-poll suppression: the store already knows which
                    # fires were seen before this process started, so anything
                    # reported here is genuinely new.
                    for event in self.tracker.update(incidents):
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
