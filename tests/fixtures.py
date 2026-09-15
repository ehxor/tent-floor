"""
tests/fixtures.py — synthetic poll snapshots for the equivalence tests.

These are hand-built from the field names each poller actually reads, not
captured from the live APIs (the sandbox this was written in cannot reach
them). They are good enough to pin the reconciliation semantics, but they are
not a substitute for replaying real captures — see tools/capture_fixtures.py
and the README section on validating the port.

Each SCENARIO is a list of snapshots, one per poll, covering the transitions
that matter: something appears, changes a tracked field, changes an untracked
field, disappears, and comes back.
"""


# ---------------------------------------------------------------------------
# BC Wildfire
# ---------------------------------------------------------------------------
def wildfire(guid, name="Test Creek", label="V70001", stage="OUT_CNTRL",
             size_ha=12.5, fire_of_note=False, lat=49.16, lon=-123.94,
             fire_centre="Coastal Fire Centre", fire_year=2026, **extra):
    incident = {
        "incidentGuid": guid,
        "incidentName": name,
        "incidentNumberLabel": label,
        "fireYear": fire_year,
        "stageOfControlCode": stage,
        "incidentSizeEstimatedHa": size_ha,
        "fireOfNoteInd": fire_of_note,
        "fireCentreName": fire_centre,
        "latitude": lat,
        "longitude": lon,
        "lastUpdatedTimestamp": "2026-08-10T00:00:00Z",
    }
    incident.update(extra)
    return incident


WILDFIRE_SCENARIO = [
    # 1. Two fires appear.
    [wildfire("g1"), wildfire("g2", name="Cedar Ridge", label="V70002",
                              stage="HOLDING", size_ha=3)],
    # 2. No change at all.
    [wildfire("g1"), wildfire("g2", name="Cedar Ridge", label="V70002",
                              stage="HOLDING", size_ha=3)],
    # 3. g1 grows and goes fire-of-note; g2 only changes an untracked field.
    [wildfire("g1", size_ha=48.0, fire_of_note=True),
     wildfire("g2", name="Cedar Ridge", label="V70002", stage="HOLDING",
              size_ha=3, lastUpdatedTimestamp="2026-08-10T02:00:00Z")],
    # 4. g1 renamed and brought under control; g2 drops out of the feed.
    [wildfire("g1", name="Test Creek North", size_ha=48.0, fire_of_note=True,
              stage="UNDR_CNTRL")],
    # 5. g2 comes back — a genuine re-declaration.
    [wildfire("g1", name="Test Creek North", size_ha=48.0, fire_of_note=True,
              stage="UNDR_CNTRL"),
     wildfire("g2", name="Cedar Ridge", label="V70002", stage="OUT_CNTRL",
              size_ha=9)],
    # 6. Integer hectare figure — must not become 5.0 in the output.
    [wildfire("g1", name="Test Creek North", size_ha=5, fire_of_note=True,
              stage="UNDR_CNTRL"),
     wildfire("g2", name="Cedar Ridge", label="V70002", stage="OUT_CNTRL",
              size_ha=9)],
    # 7. Everything clears.
    [],
]


# ---------------------------------------------------------------------------
# Nanaimo Fire
# ---------------------------------------------------------------------------
def nanaimo(incident_id, call_type="Structure Fire", location="100 Commercial St",
            apparatuses="E1, L1", time_str="14:32", date="2026-08-10",
            lat=49.1659, lon=-123.9401):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": {
            "Id": incident_id,
            "Type": call_type,
            "Location": location,
            "Apparatuses": apparatuses,
            "Timestamp": f"{date}T{time_str}:00",
            "Time": time_str,
            "Date": date,
        },
    }


NANAIMO_SCENARIO = [
    # 1. Two incidents appear.
    [nanaimo("n1"), nanaimo("n2", call_type="Medical", location="1 Terminal Ave")],
    # 2. Unchanged.
    [nanaimo("n1"), nanaimo("n2", call_type="Medical", location="1 Terminal Ave")],
    # 3. A third appears; n1's units change (untracked — must not re-announce).
    [nanaimo("n1", apparatuses="E1, L1, R1"),
     nanaimo("n2", call_type="Medical", location="1 Terminal Ave"),
     nanaimo("n3", call_type="Alarm Activation", location="55 Victoria Rd")],
    # 4. n1 and n2 age out of the feed.
    [nanaimo("n3", call_type="Alarm Activation", location="55 Victoria Rd")],
    # 5. n1 reappears — the same incident, so it must not re-announce.
    [nanaimo("n1", apparatuses="E1, L1, R1"),
     nanaimo("n3", call_type="Alarm Activation", location="55 Victoria Rd")],
    # 6. Missing Id — skipped entirely.
    [nanaimo("n3", call_type="Alarm Activation", location="55 Victoria Rd"),
     {"type": "Feature", "properties": {"Type": "Unknown"}}],
]


# ---------------------------------------------------------------------------
# PulsePoint
# ---------------------------------------------------------------------------
def pp_incident(incident_id, call_type="ME", address="100 Commercial St",
                units=(), call_time="2026-08-10T14:32:00Z"):
    return {
        "ID": incident_id,
        "PulsePointIncidentCallType": call_type,
        "FullDisplayAddress": address,
        "CallReceivedDateTime": call_time,
        "Unit": [{"UnitID": uid, "PulsePointDispatchStatus": status}
                 for uid, status in units],
    }


PULSEPOINT_SCENARIO = [
    # 1. One matching incident with two units, one non-matching incident.
    [pp_incident("i1", units=[("130A2D", "DP"), ("130A1D", "DP")]),
     pp_incident("i2", call_type="TC", address="Island Hwy",
                 units=[("9Q9", "DP")])],
    # 2. Units go enroute.
    [pp_incident("i1", units=[("130A2D", "ER"), ("130A1D", "ER")]),
     pp_incident("i2", call_type="TC", address="Island Hwy",
                 units=[("9Q9", "ER")])],
    # 3. One unit on scene, a third unit joins the incident.
    [pp_incident("i1", units=[("130A2D", "OS"), ("130A1D", "ER"),
                              ("173A1D", "DP")]),
     pp_incident("i2", call_type="TC", address="Island Hwy",
                 units=[("9Q9", "ER")])],
    # 4. A second matching incident appears.
    [pp_incident("i1", units=[("130A2D", "TR"), ("130A1D", "AQ"),
                              ("173A1D", "OS")]),
     pp_incident("i3", call_type="TC", address="Metral Dr",
                 units=[("120A3D", "DP")])],
    # 5. i1 clears; a unit drops off i3 (emits nothing, matching old code).
    [pp_incident("i3", call_type="TC", address="Metral Dr",
                 units=[("120A3D", "OS"), ("122BD", "DP")])],
    [pp_incident("i3", call_type="TC", address="Metral Dr",
                 units=[("120A3D", "OS")])],
    # 6. Everything clears.
    [],
]
