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
import urllib.error
import urllib.request
from datetime import datetime

import events
from store import Store

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
#
# Every way this can fail used to collapse into `return None, None`, and two
# of the three paths did it without printing anything. The caller could not
# tell an agency with no active calls from an unreachable API, and the one
# message it did produce — "no data returned (try again — PulsePoint
# alternates responses)" — told an operator to retry something that would
# never succeed. PulsePoint moved behind AWS WAF in September 2026 and spent
# eight days answering every poll with an empty 202 challenge under exactly
# that message.
#
# So a failed fetch now names its own cause. The kinds:
#
#   waf_challenge  — AWS WAF wants a JS challenge solved. Retrying the same
#                    request never works; this needs a token from a browser
#                    session, so it is flagged non-retryable.
#   http_error     — upstream answered with a 4xx/5xx.
#   network        — DNS, TLS, connection, timeout. Ordinary transient.
#   empty_response — 200 with nothing in it. PulsePoint genuinely does this
#                    on alternate polls, which is why it stays distinct from
#                    waf_challenge rather than being assumed to be one.
#   malformed      — the body was not the JSON envelope we expect.
#   decrypt_failed — the envelope decrypted to something unusable, which is
#                    how a change to their key derivation would show up.
# ---------------------------------------------------------------------------
WAF_ACTION_HEADER = "x-amzn-waf-action"


class FetchError:
    """Why a poll produced no incidents.

    `retryable` is about this specific request shape, not about whether the
    poller should keep running — it always keeps running. False means a plain
    retry is known to be pointless and something has to change first.
    """

    __slots__ = ("kind", "detail", "retryable")

    def __init__(self, kind, detail, retryable=True):
        self.kind = kind
        self.detail = detail
        self.retryable = retryable

    def __repr__(self):
        return f"FetchError(kind={self.kind!r}, detail={self.detail!r})"

    def __str__(self):
        return f"{self.kind}: {self.detail}"


class FetchResult:
    """The outcome of one fetch: incidents, or a reason there are none.

    `active` is None on failure and a list (possibly empty) on success, so
    `if result.active is not None` still distinguishes "nothing is happening"
    from "we could not look" the way the old tuple did.
    """

    __slots__ = ("active", "recent", "error")

    def __init__(self, active=None, recent=None, error=None):
        self.active = active
        self.recent = recent
        self.error = error

    @property
    def ok(self):
        return self.error is None

    def __repr__(self):
        if self.error is not None:
            return f"FetchResult(error={self.error!r})"
        return (f"FetchResult(active={len(self.active)}, "
                f"recent={len(self.recent)})")


def fetch_incidents(agency_id, token=None):
    """Fetch and decrypt active incidents for an agency. Returns a FetchResult.

    `token` is an AWS WAF token from waf_token.py. Without one every request
    comes back challenged; see that module for where it comes from and why it
    only lasts five minutes.

    Never raises: a poll loop that dies on a transient DNS failure is worse
    than one that reports it.
    """
    url = PULSEPOINT_API_URL.format(agency_id=agency_id)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://web.pulsepoint.org",
        "Referer": "https://web.pulsepoint.org/",
    }
    if token:
        # The cookie form works too, but a header is the right shape for a
        # server-side client and does not need a cookie jar.
        headers["x-aws-waf-token"] = token
    req = urllib.request.Request(url, headers=headers)

    try:
        resp = urllib.request.urlopen(req, timeout=15)
        status = getattr(resp, "status", None) or resp.getcode()
        waf = resp.headers.get(WAF_ACTION_HEADER)
        raw_text = resp.read().decode()
    except urllib.error.HTTPError as e:
        # A challenge can arrive as an error status too, so check it first.
        waf = e.headers.get(WAF_ACTION_HEADER) if e.headers else None
        if waf:
            return FetchResult(error=FetchError(
                "waf_challenge",
                f"HTTP {e.code} with {WAF_ACTION_HEADER}: {waf}",
                retryable=False))
        return FetchResult(error=FetchError(
            "http_error", f"HTTP {e.code} {e.reason}"))
    except urllib.error.URLError as e:
        return FetchResult(error=FetchError(
            "network", f"{type(e.reason).__name__}: {e.reason}"))
    except Exception as e:
        return FetchResult(error=FetchError(
            "network", f"{type(e).__name__}: {e}"))

    if waf:
        # 202 + an empty body + this header is AWS WAF asking for a JS
        # challenge to be solved. urllib cannot, so this recurs forever.
        return FetchResult(error=FetchError(
            "waf_challenge",
            f"HTTP {status} with {WAF_ACTION_HEADER}: {waf} "
            f"(blocked as a bot; needs a browser-issued token)",
            retryable=False))

    if not raw_text or len(raw_text) < 10:
        return FetchResult(error=FetchError(
            "empty_response",
            f"HTTP {status} with {len(raw_text)} byte(s)"))

    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError as e:
        return FetchResult(error=FetchError(
            "malformed", f"body is not JSON ({e}): {raw_text[:120]!r}"))

    try:
        data = decrypt_pulsepoint(raw)
    except Exception as e:
        return FetchResult(error=FetchError(
            "decrypt_failed", f"{type(e).__name__}: {e}", retryable=False))

    incidents = data.get("incidents")
    if not isinstance(incidents, dict):
        return FetchResult(error=FetchError(
            "decrypt_failed",
            f"decrypted payload has no incidents object: {sorted(data)[:8]}",
            retryable=False))

    return FetchResult(active=incidents.get("active", []),
                       recent=incidents.get("recent", []))


# ---------------------------------------------------------------------------
# Incident tracker
# ---------------------------------------------------------------------------
def _status_label(code):
    """Convert a dispatch status code to human-readable label."""
    return DISPATCH_STATUSES.get(code, code)


class IncidentTracker:
    """Track PulsePoint incidents and emit events on state changes.

    Two levels of reconciliation against the store: incidents drive
    new_incident and incident_cleared, and the units on those incidents drive
    unit_added and unit_status_change. Both survive a restart, so coming back
    up no longer re-announces every active incident.
    """

    def __init__(self, unit_prefixes=None, store=None, scope="", agency=None):
        """
        Args:
            unit_prefixes: List of prefixes to match against unit IDs.
                          Matching is done against the unit ID directly.
                          e.g., ["1"] matches "140A1D", "105A1D", "1Q3", etc.
                          None means track all incidents.
            store: a store.Store. Defaults to an in-memory one, which gives
                the pre-store behaviour of forgetting everything on exit.
            scope: isolates this tracker's rows. The shipped config polls one
                agency twice with different prefixes, and those two views must
                not treat each other's incidents as cleared.
            agency: recorded on incident rows; not used for matching.
        """
        self.unit_prefixes = unit_prefixes
        self.agency = agency
        self.store = store if store is not None else Store(":memory:")
        self.incidents = self.store.reconciler(
            "pulsepoint_incidents", key=("incident_id",), scope=scope)
        self.units = self.store.reconciler(
            "pulsepoint_units", key=("incident_id", "unit_id"),
            tracked=("status_code",), scope=scope)

    def _unit_matches(self, unit_id):
        """Check if a unit ID matches any of our prefixes."""
        if not self.unit_prefixes:
            return True
        return any(unit_id.startswith(p) for p in self.unit_prefixes)

    @staticmethod
    def _unit_list(incident):
        return [{"id": u.get("UnitID", ""),
                 "status": u.get("PulsePointDispatchStatus", "")}
                for u in incident.get("Unit", [])]

    def _to_row(self, incident, incident_id):
        """Map an incident onto pulsepoint_incidents columns.

        call_type_code and address hold the raw API values, with no defaults
        applied. The defaults differ between a live event ("Unknown location")
        and a cleared one ("Unknown"), so applying them here would change the
        text of cleared events.
        """
        code = incident.get("PulsePointIncidentCallType")
        return {
            "incident_id": incident_id,
            "agency": self.agency,
            "call_type_code": code,
            "call_type": CALL_TYPES.get(code, code),
            "address": incident.get("FullDisplayAddress"),
            "call_time": incident.get("CallReceivedDateTime"),
            "raw": json.dumps(incident, sort_keys=True, default=str),
        }

    def update(self, active_incidents):
        """Process active incidents and return list of events."""
        if active_incidents is None:
            return []

        incident_rows = []
        unit_rows = []
        snapshot = {}
        order = []

        for incident in active_incidents:
            incident_id = incident.get("ID")
            if not incident_id:
                continue

            unit_list = self._unit_list(incident)

            # An incident with no matching unit is skipped entirely, so it
            # falls out of the snapshot and reads as cleared. Pre-existing
            # behaviour, preserved.
            if self.unit_prefixes and not any(self._unit_matches(u["id"])
                                              for u in unit_list):
                continue

            snapshot[incident_id] = (incident, unit_list)
            order.append(incident_id)
            incident_rows.append(self._to_row(incident, incident_id))
            for unit in unit_list:
                unit_rows.append({
                    "incident_id": incident_id,
                    "unit_id": unit["id"],
                    "status_code": unit["status"],
                    "status_label": _status_label(unit["status"]),
                })

        incident_changes = self.incidents.sync(incident_rows)
        new_ids = {c.key[0] for c in incident_changes
                   if c.kind in ("appeared", "reappeared")}
        cleared = [c for c in incident_changes if c.kind == "disappeared"]

        # Confine unit disappearance detection to the incidents in this
        # snapshot — a unit is not gone just because its incident was not
        # part of this poll's matching set.
        if order:
            marks = ", ".join("?" * len(order))
            unit_scope = (f"incident_id IN ({marks})", order)
        else:
            unit_scope = ("0 = 1", [])
        unit_changes = self.units.sync(unit_rows, within=unit_scope)

        # Units of a cleared incident go with it, so that if the incident
        # comes back its units read as freshly added.
        for change in cleared:
            self.units.mark_gone_where("incident_id = ?", [change.key[0]])

        by_incident = {}
        for change in unit_changes:
            by_incident.setdefault(change.key[0], []).append(change)

        # Emit in the order the old tracker did: per incident in snapshot
        # order, then clears.
        events = []
        for incident_id in order:
            incident, unit_list = snapshot[incident_id]
            call_type_code = incident.get("PulsePointIncidentCallType", "UNK")
            call_type = CALL_TYPES.get(call_type_code, call_type_code)
            address = incident.get("FullDisplayAddress", "Unknown location")

            if incident_id in new_ids:
                unit_ids = [u["id"] for u in unit_list]
                events.append({
                    "type": "new_incident",
                    "incident_id": incident_id,
                    "call_type": call_type,
                    "call_type_code": call_type_code,
                    "address": address,
                    "call_time": incident.get("CallReceivedDateTime", ""),
                    "units": ", ".join(unit_ids) if unit_ids else "No units",
                    "unit_list": unit_list,
                })
                continue

            for change in by_incident.get(incident_id, []):
                if change.kind in ("appeared", "reappeared"):
                    events.append({
                        "type": "unit_added",
                        "incident_id": incident_id,
                        "call_type": call_type,
                        "address": address,
                        "unit_id": change.key[1],
                        "status": _status_label(change.after["status_code"]),
                    })
                elif change.kind == "changed":
                    diff = change.changed_fields["status_code"]
                    events.append({
                        "type": "unit_status_change",
                        "incident_id": incident_id,
                        "call_type": call_type,
                        "address": address,
                        "unit_id": change.key[1],
                        "old_status": _status_label(diff["old"]),
                        "new_status": _status_label(diff["new"]),
                    })
                # A unit dropping off an incident emits nothing, as before.

        for change in cleared:
            before = change.before
            code = before["call_type_code"]
            code = "UNK" if code is None else code
            address = before["address"]
            events.append({
                "type": "incident_cleared",
                "incident_id": change.key[0],
                "call_type": CALL_TYPES.get(code, code),
                "address": "Unknown" if address is None else address,
            })

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
                 poll_interval=POLL_INTERVAL_S, store=None, scope="",
                 group=None, tokens=None):
        """
        Args:
            agency_id: PulsePoint agency ID (e.g. EMS1201).
            unit_prefixes: optional list of unit ID prefixes to filter on.
            callback: function called with each event dict.
            poll_interval: seconds between polls.
            store: a store.Store for durable change detection. Without one,
                every restart re-announces all active incidents. Its event log
                is also where this poller records its own health, so without
                one a failing poller is once again only visible on stderr.
            scope: isolates this poller's rows. Two pollers on the same agency
                with different prefixes need different scopes.
            group: the group these events belong to, recorded on the health
                events so an operator can tell which poller went dark.
            tokens: a waf_token.TokenCache. The API is behind AWS WAF and
                returns an empty challenge to every request without a token.
                None keeps the pre-WAF behaviour, which is to be challenged
                and to say so.
        """
        self.agency_id = agency_id
        self.tracker = IncidentTracker(unit_prefixes=unit_prefixes, store=store,
                                       scope=scope, agency=agency_id)
        self.callback = callback
        self.poll_interval = poll_interval
        self.stop_event = threading.Event()
        self.thread = None
        self.tokens = tokens
        self.health = events.PollerHealth(
            "pulsepoint", target=agency_id, group=group,
            emit=store.events.append if store is not None else None)

    def start(self):
        self.thread = threading.Thread(target=self._poll_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()

    def _fetch(self):
        """One fetch, re-minting the token once if the API rejects it.

        The cached token's expiry is a guess; this response is the fact. A
        single retry covers the ordinary case of a token ageing out between
        polls without turning a genuine block into a mint loop.
        """
        token = self.tokens.get() if self.tokens is not None else None
        result = fetch_incidents(self.agency_id, token=token)
        if (not result.ok and result.error.kind == "waf_challenge"
                and self.tokens is not None and token is not None):
            self.tokens.invalidate()
            result = fetch_incidents(self.agency_id, token=self.tokens.get())
        return result

    def _poll_loop(self):
        while not self.stop_event.is_set():
            try:
                result = self._fetch()
                if result.ok:
                    self._report(self.health.ok())
                    for event in self.tracker.update(result.active):
                        if self.callback:
                            self.callback(event)
                else:
                    detail = result.error.detail
                    if (result.error.kind == "waf_challenge"
                            and self.tokens is not None
                            and self.tokens.last_error):
                        # Still challenged after a re-mint: the browser side is
                        # what is broken, and that is the actionable fact.
                        detail = f"{detail} -- mint failed: {self.tokens.last_error}"
                    self._report(self.health.failed(
                        result.error.kind, detail,
                        retryable=result.error.retryable))
            except Exception as e:
                # The fetch does not raise, so anything here came from the
                # tracker or the callback. It is still this poller's health.
                self._report(self.health.failed(
                    "internal", f"{type(e).__name__}: {e}"))

            self.stop_event.wait(self.poll_interval)

    @staticmethod
    def _report(health_event):
        """Mirror a health event to stderr. The event log is the record; this
        is for whoever happens to be watching the terminal."""
        if health_event is not None:
            print(f"[pulsepoint] {health_event['render']['plain']}",
                  file=sys.stderr)


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
        result = fetch_incidents(args.agency)
        if not result.ok:
            print(f"[pulsepoint] FAILED ({result.error.kind}): "
                  f"{result.error.detail}", file=sys.stderr)
            if not result.error.retryable:
                print("[pulsepoint] Retrying this request will not help.",
                      file=sys.stderr)
            sys.exit(1)
        active, recent = result.active, result.recent
        print(f"\n=== {len(active)} active incidents ===\n")
        print(json.dumps(active, indent=2))
        print(f"\n=== {len(recent)} recent incidents ===\n")
        if recent:
            print(json.dumps(recent[:3], indent=2))
            print(f"... ({len(recent) - 3} more)")
        return

    print(f"[pulsepoint] Polling agency {args.agency} every {args.interval}s")
    if args.unit_prefix:
        print(f"[pulsepoint] Filtering units starting with: {', '.join(args.unit_prefix)}")
    print("-" * 60)

    tracker = IncidentTracker(unit_prefixes=args.unit_prefix)
    # No store in standalone mode, so nothing to append to — the health
    # object is here for its throttling, and the render strings go to stderr.
    health = events.PollerHealth("pulsepoint", target=args.agency)

    try:
        while True:
            result = fetch_incidents(args.agency)
            if result.ok:
                recovered = health.ok()
                if recovered is not None:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                          f"{recovered['render']['plain']}", file=sys.stderr)
                for event in tracker.update(result.active):
                    ts = datetime.now().strftime("%H:%M:%S")
                    print(f"[{ts}] {format_event(event)}")
            else:
                failure = health.failed(result.error.kind, result.error.detail,
                                        retryable=result.error.retryable)
                if failure is not None:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                          f"{failure['render']['plain']}", file=sys.stderr)

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n[exit] Stopped.")


if __name__ == "__main__":
    main()
