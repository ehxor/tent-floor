"""
tests/legacy_trackers.py — frozen copies of the pre-store tracker classes.

Verbatim extracts of IncidentTracker, NanaimoFireTracker and BCWildfireTracker
as they existed before the store.py port, kept only so the equivalence tests
can prove the ported pollers emit identical events for identical input.

Do not fix bugs here. The point is that this behaves exactly like the old code,
warts included. Delete this file once the port has been validated against real
captured fixtures.

Generated mechanically via inspect.getsource — do not hand-edit.
"""

from bc_wildfire_poller import (
    STAGE_LABELS,
    TRACKED_FIELDS,
    nearest_town,
    point_in_polygon,
)
from pulsepoint_poller import CALL_TYPES, _status_label



# --- from pulsepoint_poller.py ---
class IncidentTracker:
    """Track PulsePoint incidents and emit events on state changes."""

    def __init__(self, unit_prefixes=None):
        """
        Args:
            unit_prefixes: List of prefixes to match against unit IDs.
                          Matching is done against the unit ID directly.
                          e.g., ["1"] matches "140A1D", "105A1D", "1Q3", etc.
                          None means track all incidents.
        """
        self.unit_prefixes = unit_prefixes
        self.known_incidents = {}  # incident_id -> last known incident dict
        self.known_units = {}     # incident_id -> {unit_id: last_status_code}

    def _unit_matches(self, unit_id):
        """Check if a unit ID matches any of our prefixes."""
        if not self.unit_prefixes:
            return True
        return any(unit_id.startswith(p) for p in self.unit_prefixes)

    def update(self, active_incidents):
        """Process active incidents and return list of events."""
        if active_incidents is None:
            return []

        events = []
        current_ids = set()

        for incident in active_incidents:
            incident_id = incident.get("ID")
            if not incident_id:
                continue

            # Extract units
            units = incident.get("Unit", [])
            unit_list = []
            for u in units:
                uid = u.get("UnitID", "")
                status = u.get("PulsePointDispatchStatus", "")
                unit_list.append({"id": uid, "status": status})

            # Filter by unit prefix
            if self.unit_prefixes:
                matching = [u for u in unit_list if self._unit_matches(u["id"])]
                if not matching:
                    continue

            current_ids.add(incident_id)

            call_type_code = incident.get("PulsePointIncidentCallType", "UNK")
            call_type = CALL_TYPES.get(call_type_code, call_type_code)
            address = incident.get("FullDisplayAddress", "Unknown location")
            call_time = incident.get("CallReceivedDateTime", "")
            unit_ids = [u["id"] for u in unit_list]
            unit_summary = ", ".join(unit_ids) if unit_ids else "No units"

            # New incident?
            if incident_id not in self.known_incidents:
                events.append({
                    "type": "new_incident",
                    "incident_id": incident_id,
                    "call_type": call_type,
                    "call_type_code": call_type_code,
                    "address": address,
                    "call_time": call_time,
                    "units": unit_summary,
                    "unit_list": unit_list,
                })
                self.known_incidents[incident_id] = incident
                self.known_units[incident_id] = {u["id"]: u["status"] for u in unit_list}
                continue

            # Existing incident — check for changes
            prev_units = self.known_units.get(incident_id, {})
            for unit in unit_list:
                uid = unit["id"]
                new_status = unit["status"]
                old_status = prev_units.get(uid)

                if old_status is None:
                    events.append({
                        "type": "unit_added",
                        "incident_id": incident_id,
                        "call_type": call_type,
                        "address": address,
                        "unit_id": uid,
                        "status": _status_label(new_status),
                    })
                elif new_status != old_status:
                    events.append({
                        "type": "unit_status_change",
                        "incident_id": incident_id,
                        "call_type": call_type,
                        "address": address,
                        "unit_id": uid,
                        "old_status": _status_label(old_status),
                        "new_status": _status_label(new_status),
                    })

            self.known_incidents[incident_id] = incident
            self.known_units[incident_id] = {u["id"]: u["status"] for u in unit_list}

        # Cleared incidents
        cleared = set(self.known_incidents.keys()) - current_ids
        for incident_id in cleared:
            old = self.known_incidents[incident_id]
            call_type_code = old.get("PulsePointIncidentCallType", "UNK")
            events.append({
                "type": "incident_cleared",
                "incident_id": incident_id,
                "call_type": CALL_TYPES.get(call_type_code, call_type_code),
                "address": old.get("FullDisplayAddress", "Unknown"),
            })
            del self.known_incidents[incident_id]
            del self.known_units[incident_id]

        return events


# --- from nanaimo_fire_poller.py ---
class NanaimoFireTracker:
    """Track Nanaimo Fire incidents and emit events for new ones."""

    def __init__(self):
        self.seen_ids = set()

    def update(self, features):
        """Process features and return list of new incident events."""
        if features is None:
            return []

        events = []
        for feature in features:
            props = feature.get("properties", {})
            incident_id = props.get("Id")
            if incident_id is None:
                continue

            if incident_id in self.seen_ids:
                continue

            self.seen_ids.add(incident_id)

            events.append({
                "type": "nanaimo_fire_incident",
                "incident_id": incident_id,
                "call_type": props.get("Type", "Unknown"),
                "location": props.get("Location", "Unknown"),
                "apparatuses": props.get("Apparatuses", ""),
                "timestamp": props.get("Timestamp", ""),
                "time": props.get("Time", ""),
                "date": props.get("Date", ""),
            })

        return events


# --- from bc_wildfire_poller.py ---
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

            fire_lat = incident.get("latitude")
            fire_lon = incident.get("longitude")
            try:
                town_name, town_dist = nearest_town(float(fire_lat), float(fire_lon))
            except (TypeError, ValueError):
                town_name, town_dist = None, None

            if guid not in self.known_incidents:
                events.append({
                    "type": "wildfire_declared",
                    "guid": guid,
                    "name": name,
                    "label": label,
                    "fire_year": fire_year,
                    "stage": stage,
                    "size_ha": size_ha,
                    "fire_of_note": fire_of_note,
                    "fire_centre": fire_centre,
                    "latitude": fire_lat,
                    "longitude": fire_lon,
                    "nearest_town": town_name,
                    "nearest_town_km": town_dist,
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
                    "nearest_town": town_name,
                    "nearest_town_km": town_dist,
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
