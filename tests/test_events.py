"""Phase 0 is meant to be invisible from the outside: the same bytes reach
Discord and the web feed, they are just built from a structured event now.

These tests pin that down the way PR #7 pinned the poller port — by freezing
the previous implementation and asserting the new one still agrees with it.
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import events


# ---------------------------------------------------------------------------
# The emit sites as they were before events.py, copied verbatim from
# scanner_transcribe_gpu.py. Do not tidy these — their value is being frozen.
# ---------------------------------------------------------------------------
def legacy_transcript(stream_name, text, duration):
    return (f"📻 {stream_name} ({duration:.1f}s): {text}",
            f"📻 **{stream_name}** ({duration:.1f}s): {text}")


def legacy_tone(stream_name, event):
    unit = event["unit"] or "UNKNOWN (logged)"
    return (f"📟 {stream_name} PAGE: {event['key']} → {unit}",
            f"📟 **{stream_name}** PAGE: {event['key']} → {unit}")


class RenderEquivalence(unittest.TestCase):
    def test_transcript_matches_legacy(self):
        cases = [
            ("Mid Island", "Engine three responding", 4.25),
            ("Cowichan Valley", "", 1.0),
            ("North Island", "Code 3 to 1200 block Bowen Road", 12.04),
            # Rounding boundary: .95 must format the same in both paths.
            ("Comox Valley", "copy that", 3.95),
        ]
        for stream, text, duration in cases:
            with self.subTest(stream=stream):
                want = legacy_transcript(stream, text, duration)
                ev = events.transcript("vancouver-island", stream, text, duration)
                self.assertEqual(ev["render"]["plain"], want[0])
                self.assertEqual(ev["render"]["discord"], want[1])

    def test_tone_matches_legacy(self):
        known = {"tone_a": 634.1, "tone_b": 600.9, "key": "634.1/600.9",
                 "unit": "Honeymoon Bay Fire Department"}
        unknown = {"tone_a": 1210.0, "tone_b": 1277.5, "key": "1210.0/1277.5",
                   "unit": None}
        for tone in (known, unknown):
            with self.subTest(unit=tone["unit"]):
                want = legacy_tone("Cowichan Valley", tone)
                ev = events.tone_page("vancouver-island", "Cowichan Valley", tone)
                self.assertEqual(ev["render"]["plain"], want[0])
                self.assertEqual(ev["render"]["discord"], want[1])

    def test_unknown_tone_is_flagged_without_losing_the_key(self):
        ev = events.tone_page("vancouver-island", "Cowichan Valley",
                              {"tone_a": 1210.0, "tone_b": 1277.5,
                               "key": "1210.0/1277.5", "unit": None})
        self.assertFalse(ev["data"]["known"])
        self.assertIsNone(ev["data"]["unit"])
        self.assertEqual(ev["data"]["tone_a_hz"], 1210.0)


class LegacyLineTypes(unittest.TestCase):
    """The web UI keys its CSS and labels off these, so the mapping is a
    compatibility surface until /lines is retired."""

    def test_every_event_type_maps(self):
        for event_type in events.LEGACY_LINE_TYPES:
            self.assertIn(events.LEGACY_LINE_TYPES[event_type],
                          {"transcript", "tone", "pulsepoint",
                           "nanaimo_fire", "bc_wildfire"})

    def test_every_poller_type_has_a_line_type(self):
        for envelope_type in events.POLLER_EVENT_TYPES.values():
            self.assertIn(envelope_type, events.LEGACY_LINE_TYPES,
                          f"{envelope_type} would silently fall back to transcript")

    def test_transcript_and_tone_land_where_the_ui_expects(self):
        ev = events.transcript("g", "s", "hello", 1.0)
        self.assertEqual(events.legacy_line_type(ev), "transcript")
        ev = events.tone_page("g", "s", {"tone_a": 1.0, "tone_b": 2.0,
                                         "key": "1.0/2.0", "unit": "X"})
        self.assertEqual(events.legacy_line_type(ev), "tone")


class PollerWrapping(unittest.TestCase):
    def test_structure_survives_that_the_string_was_dropping(self):
        raw = {
            "type": "new_incident",
            "incident_id": "abc123",
            "call_type": "Medical Emergency",
            "call_type_code": "ME",
            "address": "1200 Bowen Rd",
            "units": "M1, E3",
            "unit_list": [{"id": "M1", "status": "DP"},
                          {"id": "E3", "status": "AK"}],
        }
        ev = events.poller("vancouver-island", raw, "plain", "discord")
        self.assertEqual(ev["type"], events.CAD_INCIDENT_NEW)
        self.assertEqual(events.legacy_line_type(ev), "pulsepoint")
        # Everything but the internal tag carries through.
        self.assertNotIn("type", ev["data"])
        for key in ("incident_id", "call_type_code", "unit_list"):
            self.assertEqual(ev["data"][key], raw[key])

    def test_wildfire_coordinates_survive(self):
        raw = {"type": "wildfire_declared", "guid": "g1", "name": "K52121",
               "latitude": 50.1, "longitude": -120.2, "size_ha": 12.5,
               "stage": "Out of Control"}
        ev = events.poller("interior", raw, "p", "d")
        self.assertEqual(ev["type"], events.WILDFIRE_DECLARED)
        self.assertEqual(ev["data"]["latitude"], 50.1)
        self.assertEqual(ev["data"]["size_ha"], 12.5)

    def test_unmapped_type_is_loud(self):
        with self.assertRaises(ValueError):
            events.poller("g", {"type": "something_new"}, "p", "d")


class Envelope(unittest.TestCase):
    def test_required_fields_present_and_serializable(self):
        ev = events.transcript("vancouver-island", "Mid Island", "test", 2.0)
        for field in ("v", "id", "ts", "type", "source", "group", "stream",
                      "tier", "data", "render"):
            self.assertIn(field, ev)
        self.assertEqual(ev["v"], events.SCHEMA_VERSION)
        json.dumps(ev)  # must survive the wire

    def test_tiers(self):
        transcript = events.transcript("g", "s", "t", 1.0)
        self.assertEqual(transcript["tier"], events.TIER_SENSITIVE)
        cad = events.poller("g", {"type": "new_incident"}, "p", "d")
        self.assertEqual(cad["tier"], events.TIER_PUBLIC)

    def test_transcript_carries_provenance(self):
        ev = events.transcript("g", "s", "t", 1.0, model="large-v3")
        self.assertEqual(ev["data"]["engine"], "whisper.cpp")
        self.assertEqual(ev["data"]["model"], "large-v3")

    def test_poller_events_have_no_stream(self):
        ev = events.poller("g", {"type": "wildfire_update", "guid": "x"}, "p", "d")
        self.assertIsNone(ev["stream"])


class Identity(unittest.TestCase):
    def test_ulids_are_unique_and_sort_in_emit_order(self):
        ids = [events.new_ulid() for _ in range(5000)]
        self.assertEqual(len(set(ids)), len(ids))
        self.assertEqual(ids, sorted(ids),
                         "lexicographic order is the replay cursor")

    def test_ulid_is_crockford_base32_of_the_right_length(self):
        ulid = events.new_ulid()
        self.assertEqual(len(ulid), 26)
        self.assertTrue(set(ulid) <= set(events._CROCKFORD))

    def test_ulids_from_threads_stay_unique(self):
        import threading
        out = []
        lock = threading.Lock()

        def work():
            mine = [events.new_ulid() for _ in range(500)]
            with lock:
                out.extend(mine)

        threads = [threading.Thread(target=work) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(set(out)), len(out))


class Time(unittest.TestCase):
    def test_utc_now_is_rfc3339_zulu(self):
        from datetime import datetime, timezone
        ts = events.utc_now()
        self.assertTrue(ts.endswith("Z"), ts)
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        self.assertEqual(parsed.tzinfo, timezone.utc)

    def test_event_timestamps_are_utc_not_local(self):
        ev = events.transcript("g", "s", "t", 1.0)
        self.assertTrue(ev["ts"].endswith("Z"))
        self.assertIn("T", ev["ts"])


if __name__ == "__main__":
    unittest.main()
