"""
tests/test_store.py — unit tests for the shared reconciliation machinery.

Written once against the real tables, covering all four call sites.

Run:  python -m unittest discover tests
"""

import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from store import Store  # noqa: E402


def kinds(changes):
    return [(c.kind, c.key) for c in changes]


class TestReconciler(unittest.TestCase):
    def setUp(self):
        self.store = Store(":memory:")
        self.recon = self.store.reconciler(
            "wildfires", key=("guid",), tracked=("stage_code", "size_ha"))

    def tearDown(self):
        self.store.close()

    def row(self, guid, stage="NEW", size=1.0, name="X"):
        return {"guid": guid, "stage_code": stage, "size_ha": size, "name": name}

    def test_appeared(self):
        changes = self.recon.sync([self.row("a"), self.row("b")])
        self.assertEqual(kinds(changes), [("appeared", ("a",)), ("appeared", ("b",))])

    def test_unchanged_snapshot_yields_nothing(self):
        self.recon.sync([self.row("a")])
        self.assertEqual(self.recon.sync([self.row("a")]), [])

    def test_changed_reports_only_tracked_columns(self):
        self.recon.sync([self.row("a", stage="NEW", name="X")])
        changes = self.recon.sync([self.row("a", stage="OUT", name="Y")])
        self.assertEqual(kinds(changes), [("changed", ("a",))])
        self.assertEqual(changes[0].changed_fields,
                         {"stage_code": {"old": "NEW", "new": "OUT"}})

    def test_untracked_column_change_is_stored_but_not_reported(self):
        self.recon.sync([self.row("a", name="X")])
        self.assertEqual(self.recon.sync([self.row("a", name="Y")]), [])
        with self.store.lock:
            stored = self.store.conn.execute(
                "SELECT name FROM wildfires WHERE guid = 'a'").fetchone()[0]
        self.assertEqual(stored, "Y")

    def test_disappeared_then_reappeared(self):
        self.recon.sync([self.row("a")])
        self.assertEqual(kinds(self.recon.sync([])), [("disappeared", ("a",))])
        self.assertEqual(kinds(self.recon.sync([self.row("a")])),
                         [("reappeared", ("a",))])

    def test_disappearing_twice_only_reports_once(self):
        self.recon.sync([self.row("a")])
        self.recon.sync([])
        self.assertEqual(self.recon.sync([]), [])

    def test_presence_only_tracking_never_reports_changed(self):
        recon = self.store.reconciler("nanaimo_incidents", key=("incident_id",))
        recon.sync([{"incident_id": "n1", "call_type": "Fire"}])
        self.assertEqual(recon.sync([{"incident_id": "n1", "call_type": "Medical"}]), [])

    def test_composite_key(self):
        recon = self.store.reconciler(
            "pulsepoint_units", key=("incident_id", "unit_id"),
            tracked=("status_code",))
        changes = recon.sync([
            {"incident_id": "i1", "unit_id": "u1", "status_code": "DP"},
            {"incident_id": "i1", "unit_id": "u2", "status_code": "DP"},
        ])
        self.assertEqual(kinds(changes),
                         [("appeared", ("i1", "u1")), ("appeared", ("i1", "u2"))])

        changes = recon.sync([
            {"incident_id": "i1", "unit_id": "u1", "status_code": "ER"},
            {"incident_id": "i1", "unit_id": "u2", "status_code": "DP"},
        ])
        self.assertEqual(kinds(changes), [("changed", ("i1", "u1"))])

    def test_scope_limits_disappearance_detection(self):
        recon = self.store.reconciler(
            "pulsepoint_units", key=("incident_id", "unit_id"),
            tracked=("status_code",))
        recon.sync([{"incident_id": "i1", "unit_id": "u1", "status_code": "DP"},
                    {"incident_id": "i2", "unit_id": "u2", "status_code": "DP"}])

        # A snapshot covering only i1 must not touch i2's units.
        changes = recon.sync([{"incident_id": "i1", "unit_id": "u1",
                               "status_code": "ER"}],
                             within=("incident_id IN (?)", ["i1"]))
        self.assertEqual(kinds(changes), [("changed", ("i1", "u1"))])

    def test_detect_disappeared_false_leaves_rows_alone(self):
        self.recon.sync([self.row("a")])
        self.assertEqual(self.recon.sync([], detect_disappeared=False), [])
        self.assertEqual(self.recon.live_keys(), [("a",)])

    def test_mark_gone_where(self):
        recon = self.store.reconciler(
            "pulsepoint_units", key=("incident_id", "unit_id"))
        recon.sync([{"incident_id": "i1", "unit_id": "u1"},
                    {"incident_id": "i1", "unit_id": "u2"},
                    {"incident_id": "i2", "unit_id": "u3"}])
        self.assertEqual(recon.mark_gone_where("incident_id = ?", ["i1"]), 2)
        self.assertEqual(recon.live_keys(), [("i2", "u3")])

    def test_booleans_round_trip_without_spurious_changes(self):
        recon = self.store.reconciler("wildfires", key=("guid",),
                                      tracked=("fire_of_note",))
        recon.sync([{"guid": "a", "fire_of_note": False}])
        self.assertEqual(recon.sync([{"guid": "a", "fire_of_note": False}]), [])
        changes = recon.sync([{"guid": "a", "fire_of_note": True}])
        self.assertEqual(changes[0].changed_fields,
                         {"fire_of_note": {"old": 0, "new": 1}})

    def test_int_and_float_compare_equal_without_churn(self):
        self.recon.sync([self.row("a", size=5)])
        self.assertEqual(self.recon.sync([self.row("a", size=5.0)]), [])

    def test_unknown_column_is_rejected(self):
        with self.assertRaises(ValueError):
            self.store.reconciler("wildfires", key=("guid",), tracked=("nope",))

    def test_scopes_are_isolated(self):
        a = self.store.reconciler("wildfires", key=("guid",), scope="a")
        b = self.store.reconciler("wildfires", key=("guid",), scope="b")
        self.assertEqual(kinds(a.sync([self.row("g")])), [("appeared", ("g",))])
        self.assertEqual(kinds(b.sync([self.row("g")])), [("appeared", ("g",))])
        # Neither should see the other's row vanish.
        self.assertEqual(a.sync([self.row("g")]), [])
        self.assertEqual(b.sync([self.row("g")]), [])


class TestRetention(unittest.TestCase):
    def test_sweep_drops_only_long_gone_rows(self):
        store = Store(":memory:", retention_days=30)
        recon = store.reconciler("wildfires", key=("guid",))
        recon.sync([{"guid": "old"}, {"guid": "recent"}, {"guid": "live"}])

        now = 1_000_000.0
        recon.sync([{"guid": "live"}], now=now - 40 * 86400)   # old, recent -> gone
        recon.sync([{"guid": "live"}, {"guid": "recent"}], now=now - 40 * 86400)
        recon.sync([{"guid": "live"}], now=now - 1 * 86400)    # recent -> gone again

        self.assertEqual(store.sweep(now=now), 1)
        with store.lock:
            remaining = {r[0] for r in
                         store.conn.execute("SELECT guid FROM wildfires")}
        self.assertEqual(remaining, {"live", "recent"})
        store.close()


class TestKV(unittest.TestCase):
    def setUp(self):
        self.store = Store(":memory:")

    def tearDown(self):
        self.store.close()

    def test_roundtrip_and_default(self):
        self.assertIsNone(self.store.kv.get("ns", "missing"))
        self.assertEqual(self.store.kv.get("ns", "missing", "fallback"), "fallback")
        self.store.kv.set("ns", "k", {"status_id": "12345"})
        self.assertEqual(self.store.kv.get("ns", "k"), {"status_id": "12345"})

    def test_overwrite_and_delete(self):
        self.store.kv.set("ns", "k", 1)
        self.store.kv.set("ns", "k", 2)
        self.assertEqual(self.store.kv.get("ns", "k"), 2)
        self.store.kv.delete("ns", "k")
        self.assertIsNone(self.store.kv.get("ns", "k"))

    def test_namespaces_are_separate(self):
        self.store.kv.set("a", "k", 1)
        self.store.kv.set("b", "k", 2)
        self.assertEqual(self.store.kv.all("a"), {"k": 1})
        self.assertEqual(self.store.kv.all("b"), {"k": 2})


class TestMigrations(unittest.TestCase):
    def test_reopening_does_not_re_run_migrations(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "m.db")
            store = Store(path)
            store.kv.set("ns", "k", "v")
            store.close()

            store = Store(path)  # would raise "table already exists" if re-run
            self.assertEqual(store.kv.get("ns", "k"), "v")
            with store.lock:
                version = store.conn.execute("PRAGMA user_version").fetchone()[0]
            self.assertGreater(version, 0)
            store.close()


class TestConcurrency(unittest.TestCase):
    def test_parallel_syncs_do_not_lock_up(self):
        """Poller threads sync different tables at the same time."""
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(os.path.join(tmp, "c.db"))
            errors = []

            def hammer(table, key, prefix):
                try:
                    recon = store.reconciler(table, key=key, scope=prefix)
                    for i in range(50):
                        recon.sync([{key[0]: f"{prefix}-{i}"}])
                except Exception as exc:  # pragma: no cover - failure path
                    errors.append(exc)

            threads = [
                threading.Thread(target=hammer, args=("wildfires", ("guid",), "w")),
                threading.Thread(target=hammer, args=("nanaimo_incidents",
                                                      ("incident_id",), "n")),
                threading.Thread(target=hammer, args=("pulsepoint_incidents",
                                                      ("incident_id",), "p")),
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)

            self.assertEqual(errors, [])
            self.assertFalse([t for t in threads if t.is_alive()])
            store.close()


if __name__ == "__main__":
    unittest.main()
