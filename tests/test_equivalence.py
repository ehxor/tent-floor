"""
tests/test_equivalence.py — the ported pollers must emit what the old ones did.

Each test replays a sequence of poll snapshots through both the frozen
pre-store tracker (tests/legacy_trackers.py) and the store-backed one, and
asserts the emitted events match step for step.

Two deliberate divergences are asserted separately in test_restart.py rather
than here, because they are the point of the change:
  * a restart no longer replays or swallows anything
  * removal/re-appearance is now explicit per source

Run:  python -m unittest discover tests
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import bc_wildfire_poller as wf  # noqa: E402
import fixtures  # noqa: E402
import legacy_trackers as legacy  # noqa: E402
import nanaimo_fire_poller as nf  # noqa: E402
import pulsepoint_poller as pp  # noqa: E402
from store import Store  # noqa: E402


def sort_events(events):
    """Order-insensitive comparison key.

    The old trackers emitted removals by iterating a set, so their order was
    arbitrary; the reconciler sorts them. Comparing as sorted lists keeps the
    test honest about content without pinning an order the old code never
    actually guaranteed.
    """
    return sorted(events, key=lambda e: json.dumps(e, sort_keys=True, default=str))


class EquivalenceMixin:
    """Replays a scenario through both trackers and compares each step."""

    def assert_same(self, legacy_tracker, new_tracker, scenario, formatters=()):
        for step, snapshot in enumerate(scenario, start=1):
            expected = legacy_tracker.update(snapshot)
            actual = new_tracker.update(snapshot)

            self.assertEqual(
                sort_events(expected), sort_events(actual),
                f"event mismatch at poll {step}\n"
                f"  legacy: {json.dumps(sort_events(expected), indent=2, default=str)}\n"
                f"  ported: {json.dumps(sort_events(actual), indent=2, default=str)}")

            # Formatted output is what actually reaches Discord and the feed,
            # so pin it too — this is what catches 5 quietly becoming 5.0.
            for fmt in formatters:
                self.assertEqual(
                    [fmt(e) for e in sort_events(expected)],
                    [fmt(e) for e in sort_events(actual)],
                    f"formatted output mismatch at poll {step} via {fmt.__name__}")


class TestWildfireEquivalence(unittest.TestCase, EquivalenceMixin):
    def test_scenario_matches_legacy(self):
        self.assert_same(
            legacy.BCWildfireTracker(),
            wf.BCWildfireTracker(store=Store(":memory:")),
            fixtures.WILDFIRE_SCENARIO,
            formatters=(wf.format_event, wf.format_event_discord),
        )

    def test_integer_size_is_not_coerced_to_float(self):
        """A REAL column would turn 5 into 5.0 and print '5.0 ha'."""
        tracker = wf.BCWildfireTracker(store=Store(":memory:"))

        # The declared event prints the size directly.
        declared = tracker.update([fixtures.wildfire("g9", size_ha=5)])[0]
        self.assertEqual(declared["size_ha"], 5)
        self.assertIn("5 ha", wf.format_event(declared))
        self.assertNotIn("5.0 ha", wf.format_event(declared))

        # The update event prints old -> new, and 'old' comes back out of
        # the column — which is where a REAL affinity would show up.
        changed = tracker.update([fixtures.wildfire("g9", size_ha=6)])[0]
        self.assertEqual(changed["changes"]["incidentSizeEstimatedHa"],
                         {"old": 5, "new": 6})
        self.assertIn("size: 5 ha -> 6 ha", wf.format_event(changed))

    def test_fire_of_note_stays_a_bool_in_changes(self):
        """It round-trips through an INTEGER column; consumers see a bool."""
        tracker = wf.BCWildfireTracker(store=Store(":memory:"))
        tracker.update([fixtures.wildfire("g9", fire_of_note=False)])
        events = tracker.update([fixtures.wildfire("g9", fire_of_note=True)])
        diff = events[0]["changes"]["fireOfNoteInd"]
        self.assertIs(diff["old"], False)
        self.assertIs(diff["new"], True)

    def test_polygon_filter_still_applies(self):
        # A polygon around Nanaimo; the fire is in the Interior.
        polygon = [(49.0, -124.2), (49.4, -124.2), (49.4, -123.7), (49.0, -123.7)]
        tracker = wf.BCWildfireTracker(polygon=polygon, store=Store(":memory:"))
        self.assertEqual(tracker.update([fixtures.wildfire("g1", lat=50.7, lon=-120.3)]), [])
        self.assertEqual(len(tracker.update([fixtures.wildfire("g1", lat=49.2, lon=-123.9)])), 1)


class TestNanaimoEquivalence(unittest.TestCase, EquivalenceMixin):
    def test_scenario_matches_legacy(self):
        self.assert_same(
            legacy.NanaimoFireTracker(),
            nf.NanaimoFireTracker(store=Store(":memory:")),
            fixtures.NANAIMO_SCENARIO,
            formatters=(nf.format_event, nf.format_event_discord),
        )


class TestPulsePointEquivalence(unittest.TestCase, EquivalenceMixin):
    def test_scenario_matches_legacy_unfiltered(self):
        self.assert_same(
            legacy.IncidentTracker(),
            pp.IncidentTracker(store=Store(":memory:")),
            fixtures.PULSEPOINT_SCENARIO,
            formatters=(pp.format_event, pp.format_event_discord),
        )

    def test_scenario_matches_legacy_with_unit_prefix(self):
        self.assert_same(
            legacy.IncidentTracker(unit_prefixes=["1"]),
            pp.IncidentTracker(unit_prefixes=["1"], store=Store(":memory:")),
            fixtures.PULSEPOINT_SCENARIO,
            formatters=(pp.format_event, pp.format_event_discord),
        )

    def test_incident_losing_its_matching_units_reads_as_cleared(self):
        """Pre-existing behaviour: the prefix filter skips the whole incident,
        so it falls out of the snapshot and is reported cleared."""
        for tracker in (legacy.IncidentTracker(unit_prefixes=["1"]),
                        pp.IncidentTracker(unit_prefixes=["1"],
                                           store=Store(":memory:"))):
            tracker.update([fixtures.pp_incident("i1", units=[("130A2D", "DP")])])
            events = tracker.update([fixtures.pp_incident("i1", units=[("9Q9", "DP")])])
            self.assertEqual([e["type"] for e in events], ["incident_cleared"])


if __name__ == "__main__":
    unittest.main()
