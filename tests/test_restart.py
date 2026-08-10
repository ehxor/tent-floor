"""
tests/test_restart.py — the behaviour this whole change exists for.

Before the store, each poller had to pick one of two wrong restart
behaviours. PulsePoint replayed: no first-load guard, so coming back up
re-announced every active incident. Nanaimo Fire and BC Wildfire had amnesia:
both swallowed their entire first poll, so anything that appeared while the
process was down was never reported.

These tests assert the third option, which is the only correct one — nothing
is replayed, and nothing that happened during downtime is missed.

Run:  python -m unittest discover tests
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import bc_wildfire_poller as wf  # noqa: E402
import fixtures  # noqa: E402
import nanaimo_fire_poller as nf  # noqa: E402
import pulsepoint_poller as pp  # noqa: E402
from store import Store  # noqa: E402


class RestartCase(unittest.TestCase):
    """Gives each test a real on-disk database it can reopen."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "test.db")

    def tearDown(self):
        self.tmp.cleanup()

    def store(self):
        return Store(self.db)


class TestWildfireRestart(RestartCase):
    def test_no_replay_and_no_missed_incidents(self):
        store = self.store()
        tracker = wf.BCWildfireTracker(store=store)
        first = tracker.update([fixtures.wildfire("g1"), fixtures.wildfire("g2")])
        self.assertEqual(len(first), 2)
        store.close()

        # Restart. g2 is gone, g3 appeared while we were down.
        store = self.store()
        tracker = wf.BCWildfireTracker(store=store)
        events = tracker.update([fixtures.wildfire("g1"), fixtures.wildfire("g3")])

        kinds = sorted((e["type"], e["guid"]) for e in events)
        self.assertEqual(kinds, [("wildfire_declared", "g3"),
                                 ("wildfire_removed", "g2")])
        store.close()

    def test_tracked_change_during_downtime_is_reported(self):
        store = self.store()
        wf.BCWildfireTracker(store=store).update(
            [fixtures.wildfire("g1", stage="OUT_CNTRL", size_ha=10)])
        store.close()

        store = self.store()
        events = wf.BCWildfireTracker(store=store).update(
            [fixtures.wildfire("g1", stage="UNDR_CNTRL", size_ha=10)])
        self.assertEqual([e["type"] for e in events], ["wildfire_update"])
        self.assertEqual(events[0]["changes"]["stageOfControlCode"],
                         {"old": "OUT_CNTRL", "new": "UNDR_CNTRL"})
        store.close()


class TestNanaimoRestart(RestartCase):
    def test_incident_during_downtime_is_not_swallowed(self):
        store = self.store()
        first = nf.NanaimoFireTracker(store=store).update([fixtures.nanaimo("n1")])
        self.assertEqual(len(first), 1)
        store.close()

        store = self.store()
        events = nf.NanaimoFireTracker(store=store).update(
            [fixtures.nanaimo("n1"), fixtures.nanaimo("n2")])
        self.assertEqual([e["incident_id"] for e in events], ["n2"])
        store.close()

    def test_integer_ids_survive_the_round_trip(self):
        """A TEXT key column would coerce 1 to "1", miss on the next poll,
        and re-announce the same incident forever."""
        store = self.store()
        self.assertEqual(len(nf.NanaimoFireTracker(store=store).update(
            [fixtures.nanaimo(1), fixtures.nanaimo(2)])), 2)
        store.close()

        store = self.store()
        self.assertEqual(nf.NanaimoFireTracker(store=store).update(
            [fixtures.nanaimo(1), fixtures.nanaimo(2)]), [])
        store.close()


class TestPulsePointRestart(RestartCase):
    def test_active_incidents_are_not_re_announced(self):
        """This is the restart storm: the old tracker had no first-load guard,
        so every watchdog restart replayed the whole active list."""
        snapshot = [fixtures.pp_incident("i1", units=[("130A2D", "ER")]),
                    fixtures.pp_incident("i2", units=[("120A3D", "OS")])]

        store = self.store()
        self.assertEqual(len(pp.IncidentTracker(store=store).update(snapshot)), 2)
        store.close()

        store = self.store()
        self.assertEqual(pp.IncidentTracker(store=store).update(snapshot), [])
        store.close()

    def test_status_change_during_downtime_is_reported(self):
        store = self.store()
        pp.IncidentTracker(store=store).update(
            [fixtures.pp_incident("i1", units=[("130A2D", "ER")])])
        store.close()

        store = self.store()
        events = pp.IncidentTracker(store=store).update(
            [fixtures.pp_incident("i1", units=[("130A2D", "TR")])])
        self.assertEqual([e["type"] for e in events], ["unit_status_change"])
        self.assertEqual(events[0]["old_status"], "enroute")
        self.assertEqual(events[0]["new_status"], "transporting")
        store.close()


class TestScopeIsolation(RestartCase):
    """The shipped config runs the same source twice with different filters.

    Sharing rows between those two views would make each read the other's
    incidents as missing from its own snapshot, flapping between cleared and
    re-declared on every poll.
    """

    def test_two_pulsepoint_views_of_one_agency_do_not_interfere(self):
        store = self.store()
        island = pp.IncidentTracker(unit_prefixes=["1"], store=store,
                                    scope="vancouver-island")
        interior = pp.IncidentTracker(unit_prefixes=["3"], store=store,
                                      scope="interior")

        snapshot = [fixtures.pp_incident("i1", units=[("130A2D", "ER")]),
                    fixtures.pp_incident("i2", units=[("330A1D", "ER")])]

        self.assertEqual([e["incident_id"] for e in island.update(snapshot)], ["i1"])
        self.assertEqual([e["incident_id"] for e in interior.update(snapshot)], ["i2"])

        # Neither view should now see anything change.
        self.assertEqual(island.update(snapshot), [])
        self.assertEqual(interior.update(snapshot), [])
        store.close()

    def test_two_wildfire_regions_do_not_interfere(self):
        store = self.store()
        coastal = wf.BCWildfireTracker(store=store, scope="vancouver-island")
        interior = wf.BCWildfireTracker(store=store, scope="interior")

        self.assertEqual(len(coastal.update([fixtures.wildfire("g1")])), 1)
        self.assertEqual(len(interior.update([fixtures.wildfire("g2")])), 1)

        # The interior poll must not have cleared the coastal fire.
        self.assertEqual(coastal.update([fixtures.wildfire("g1")]), [])
        self.assertEqual(interior.update([fixtures.wildfire("g2")]), [])
        store.close()


if __name__ == "__main__":
    unittest.main()
