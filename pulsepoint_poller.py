"""
pulsepoint_poller.py — Poll PulsePoint for live CAD incident data.

Fetches encrypted incident data from PulsePoint's web API, decrypts it,
tracks state changes (new incidents, unit status updates, incidents cleared),
and emits events for integration with scanner_transcribe.py.

Usage:
    Integrated into scanner_transcribe.py via --pulsepoint flag, or standalone:
        python pulsepoint_poller.py --agency EMS1201 --unit-prefix 1

    Standalone mode prints events to the console for testing.

Requirements:
    pip install cryptography

The agency ID is visible in the PulsePoint web URL:
    https://web.pulsepoint.org/?agencies=EMS1201
                                         ^^^^^^^
"""

import base64
import hashlib
import json
import sys
import time
import threading
import urllib.request
from datetime import datetime

try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.backends import default_backend
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

# ---------------------------------------------------------------------------
# PulsePoint API
# ---------------------------------------------------------------------------
PULSEPOINT_API_URL = "https://api.pulsepoint.org/v1/webapp?resource=incidents&agencyid={agency_id}"
POLL_INTERVAL_S = 30

# ---------------------------------------------------------------------------
# Incident type codes
# ---------------------------------------------------------------------------
CALL_TYPES = {
    "AA": "Auto Aid", "MU": "Mutual Aid", "ST": "Strike Team",
    "AC": "Aircraft Crash", "AE": "Aircraft Emergency", "AES": "Aircraft Standby",
    "LZ": "Landing Zone", "AED": "AED Alarm", "OA": "Alarm",
    "CMA": "Carbon Monoxide", "FA": "Fire Alarm", "MA": "Manual Alarm",
    "SD": "Smoke Detector", "TRBL": "Trouble Alarm", "WFA": "Waterflow Alarm",
    "FL": "Flooding", "LR": "Ladder Request", "LA": "Lift Assist",
    "PA": "Police Assist", "PS": "Public Service", "SH": "Sheared Hydrant",
    "EX": "Explosion", "PE": "Pipeline Emergency", "TE": "Transformer Explosion",
    "AF": "Appliance Fire", "CHIM": "Chimney Fire", "CF": "Commercial Fire",
    "WSF": "Confirmed Structure Fire", "WVEG": "Confirmed Vegetation Fire",
    "CB": "Controlled Burn", "ELF": "Electrical Fire", "EF": "Extinguished Fire",
    "FIRE": "Fire", "FULL": "Full Assignment", "IF": "Illegal Fire",
    "MF": "Marine Fire", "OF": "Outside Fire", "PF": "Pole Fire",
    "GF": "Refuse/Garbage Fire", "RF": "Residential Fire", "SF": "Structure Fire",
    "VEG": "Vegetation Fire", "VF": "Vehicle Fire",
    "WCF": "Working Commercial Fire", "WRF": "Working Residential Fire",
    "BT": "Bomb Threat", "EE": "Electrical Emergency", "EM": "Emergency",
    "ER": "Emergency Response", "GAS": "Gas Leak", "HC": "Hazardous Condition",
    "HMR": "Hazmat Response", "TD": "Tree Down", "WE": "Water Emergency",
    "AI": "Arson Investigation", "HMI": "Hazmat Investigation",
    "INV": "Investigation", "OI": "Odor Investigation", "SI": "Smoke Investigation",
    "LO": "Lockout", "CL": "Commercial Lockout", "RL": "Residential Lockout",
    "VL": "Vehicle Lockout", "IFT": "Interfacility Transfer",
    "ME": "Medical Emergency", "MCI": "Multi Casualty",
    "EQ": "Earthquake", "FLW": "Flood Warning", "TOW": "Tornado Warning",
    "TSW": "Tsunami Warning", "CA": "Community Activity", "FW": "Fire Watch",
    "NO": "Notification", "STBY": "Standby", "TEST": "Test", "TRNG": "Training",
    "UNK": "Unknown", "AR": "Animal Rescue", "CR": "Cliff Rescue",
    "CSR": "Confined Space", "ELR": "Elevator Rescue", "RES": "Rescue",
    "RR": "Rope Rescue", "TR": "Technical Rescue", "TNR": "Trench Rescue",
    "USAR": "Urban Search & Rescue", "VS": "Vessel Sinking", "WR": "Water Rescue",
    "TCE": "Expanded Traffic Collision", "RTE": "Railroad/Train Emergency",
    "TC": "Traffic Collision", "TCS": "Traffic Collision w/ Structure",
    "TCT": "Traffic Collision w/ Train", "WA": "Wires Arcing", "WD": "Wires Down",
}

# PulsePoint dispatch status codes (from actual API responses)
DISPATCH_STATUSES = {
    "AQ": "dispatched",       # Awaiting/Queued
    "AR": "arrived",          # Arrived
    "DP": "dispatched",       # Dispatched
    "ER": "enroute",          # Enroute
    "OS": "on scene",         # On Scene
    "TR": "transporting",     # Transport
    "TA": "at hospital",      # Transport Arrived
    "CL": "cleared",          # Cleared
    "AOS": "avail on scene",  # Available On Scene
}


# ---------------------------------------------------------------------------
# Decryption
# ---------------------------------------------------------------------------
def _build_password():
    """Reconstruct the hardcoded decryption password from PulsePoint's JS."""
    e = "CommonIncidents"
    return e[13] + e[1] + e[2] + "brady" + "5" + "r" + e.lower()[6] + e[5] + "gs"


def _derive_key(password, salt):
    """Derive AES-256 key using MD5 (OpenSSL EVP_BytesToKey compat)."""
    key = b''
    block = None
    while len(key) < 32:
        hasher = hashlib.md5()
        if block:
            hasher.update(block)
        hasher.update(password.encode())
        hasher.update(salt)
        block = hasher.digest()
        key += block
    return key[:32]


def decrypt_pulsepoint(data):
    """Decrypt PulsePoint's AES-CBC encrypted JSON response."""
    if not HAS_CRYPTO:
        raise ImportError("cryptography package required: pip install cryptography")

    ct = base64.b64decode(data["ct"])
    iv = bytes.fromhex(data["iv"])
    salt = bytes.fromhex(data["s"])

    password = _build_password()
    key = _derive_key(password, salt)

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    out = decryptor.update(ct) + decryptor.finalize()

    # Strip padding and wrapper quotes
    out = out[1:out.rindex(b'"')].decode()
    out = out.replace(r'\"', r'"')

    return json.loads(out)


# ---------------------------------------------------------------------------
# API fetch
# ---------------------------------------------------------------------------
def fetch_incidents(agency_id):
    """Fetch and decrypt active incidents for an agency."""
    url = PULSEPOINT_API_URL.format(agency_id=agency_id)
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://web.pulsepoint.org",
        "Referer": "https://web.pulsepoint.org/",
    })

    try:
        resp = urllib.request.urlopen(req, timeout=15)
        raw_text = resp.read().decode()
        if not raw_text or len(raw_text) < 10:
            return None, None  # Empty response (alternating poll pattern)

        raw = json.loads(raw_text)
        data = decrypt_pulsepoint(raw)

        active = data.get("incidents", {}).get("active", [])
        recent = data.get("incidents", {}).get("recent", [])
        return active, recent
    except json.JSONDecodeError:
        return None, None  # PulsePoint sometimes returns empty on alternate polls
    except Exception as e:
        print(f"[pulsepoint] Error fetching data: {e}", file=sys.stderr)
        return None, None


# ---------------------------------------------------------------------------
# Incident tracker
# ---------------------------------------------------------------------------
def _status_label(code):
    """Convert a dispatch status code to human-readable label."""
    return DISPATCH_STATUSES.get(code, code)


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


# ---------------------------------------------------------------------------
# Event formatting
# ---------------------------------------------------------------------------
def format_event(event):
    """Format for terminal output."""
    t = event["type"]
    if t == "new_incident":
        return (f"🚒 NEW: {event['call_type']} — {event['address']} "
                f"— Units: {event['units']}")
    elif t == "unit_added":
        return (f"🚒 +UNIT: {event['unit_id']} {event['status']} "
                f"— {event['call_type']} @ {event['address']}")
    elif t == "unit_status_change":
        return (f"🚒 UPDATE: {event['unit_id']} "
                f"{event['old_status']} → {event['new_status']} "
                f"— {event['call_type']} @ {event['address']}")
    elif t == "incident_cleared":
        return f"🚒 CLEARED: {event['call_type']} — {event['address']}"
    return str(event)


def format_event_discord(event):
    """Format for Discord (with markdown)."""
    t = event["type"]
    if t == "new_incident":
        return (f"🚒 **NEW: {event['call_type']}** — {event['address']} "
                f"— Units: {event['units']}")
    elif t == "unit_added":
        return (f"🚒 **+UNIT:** {event['unit_id']} {event['status']} "
                f"— {event['call_type']} @ {event['address']}")
    elif t == "unit_status_change":
        return (f"🚒 **{event['unit_id']}** "
                f"{event['old_status']} → {event['new_status']} "
                f"— {event['call_type']} @ {event['address']}")
    elif t == "incident_cleared":
        return f"🚒 **CLEARED:** {event['call_type']} — {event['address']}"
    return str(event)


# ---------------------------------------------------------------------------
# Background poller thread
# ---------------------------------------------------------------------------
class PulsePointPoller:
    """Background thread that polls PulsePoint and calls a callback with events."""

    def __init__(self, agency_id, unit_prefixes=None, callback=None,
                 poll_interval=POLL_INTERVAL_S):
        self.agency_id = agency_id
        self.tracker = IncidentTracker(unit_prefixes=unit_prefixes)
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
                active, recent = fetch_incidents(self.agency_id)
                if active is not None:
                    events = self.tracker.update(active)
                    for event in events:
                        if self.callback:
                            self.callback(event)
            except Exception as e:
                print(f"[pulsepoint] Poll error: {e}", file=sys.stderr)

            self.stop_event.wait(self.poll_interval)


# ---------------------------------------------------------------------------
# Standalone mode
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="Poll PulsePoint for live incident data")
    parser.add_argument("--agency", "-a", required=True,
                        help="PulsePoint agency ID (e.g., EMS1201)")
    parser.add_argument("--unit-prefix", "-u", action="append",
                        help="Filter units starting with this prefix (repeatable)")
    parser.add_argument("--interval", "-i", type=int, default=POLL_INTERVAL_S,
                        help=f"Poll interval in seconds (default: {POLL_INTERVAL_S})")
    parser.add_argument("--dump", action="store_true",
                        help="Dump raw incident data and exit")
    args = parser.parse_args()

    if not HAS_CRYPTO:
        print("[error] cryptography package required: pip install cryptography")
        sys.exit(1)

    if args.dump:
        print(f"[pulsepoint] Fetching incidents for agency {args.agency}...")
        active, recent = fetch_incidents(args.agency)
        if active is not None:
            print(f"\n=== {len(active)} active incidents ===\n")
            print(json.dumps(active, indent=2))
            print(f"\n=== {len(recent)} recent incidents ===\n")
            if recent:
                print(json.dumps(recent[:3], indent=2))
                print(f"... ({len(recent) - 3} more)")
        else:
            print("[pulsepoint] No data returned (try again — PulsePoint alternates responses)")
        return

    print(f"[pulsepoint] Polling agency {args.agency} every {args.interval}s")
    if args.unit_prefix:
        print(f"[pulsepoint] Filtering units starting with: {', '.join(args.unit_prefix)}")
    print("-" * 60)

    tracker = IncidentTracker(unit_prefixes=args.unit_prefix)

    try:
        while True:
            active, recent = fetch_incidents(args.agency)
            if active is not None:
                events = tracker.update(active)
                for event in events:
                    ts = datetime.now().strftime("%H:%M:%S")
                    print(f"[{ts}] {format_event(event)}")

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n[exit] Stopped.")


if __name__ == "__main__":
    main()
