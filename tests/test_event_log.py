"""The event log is the seam between capture and presentation, so the
properties that matter are the ones consumers will lean on: append never loses
an event, replay never duplicates one, and a cursor only moves forward.
"""

import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import events
from store import Store


def sample(text="engine three responding", group="vancouver-island"):
    return events.transcript(group, "Mid Island", text, 4.2, model="large-v3")


class Append(unittest.TestCase):
    def setUp(self):
        self.store = Store(":memory:")
        self.log = self.store.events

    def tearDown(self):
        self.store.close()

    def test_seq_increases_and_starts_at_one(self):
        first = self.log.append(sample("one"))
        second = self.log.append(sample("two"))
        self.assertEqual(first, 1)
        self.assertEqual(second, 2)
        self.assertEqual(self.log.latest_seq(), 2)

    def test_replaying_the_same_event_is_a_no_op(self):
        event = sample()
        first = self.log.append(event)
        again = self.log.append(event)
        self.assertEqual(first, again)
        self.assertEqual(self.log.count(), 1,
                         "a retried batch must not duplicate")

    def test_envelope_round_trips_unchanged(self):
        event = sample()
        self.log.append(event)
        (_, restored), = self.log.read_after(0)
        self.assertEqual(restored, event)

    def test_structured_payload_survives_the_database(self):
        raw = {"type": "wildfire_declared", "guid": "g1", "latitude": 50.1,
               "longitude": -120.2, "size_ha": 12.5, "fire_of_note": True}
        self.log.append(events.poller("interior", raw, "plain", "discord"))
        (_, restored), = self.log.read_after(0)
        self.assertEqual(restored["data"]["latitude"], 50.1)
        self.assertEqual(restored["data"]["size_ha"], 12.5)
        self.assertIs(restored["data"]["fire_of_note"], True)

    def test_latest_seq_on_empty_log_is_zero(self):
        self.assertEqual(self.log.latest_seq(), 0)


class Reading(unittest.TestCase):
    def setUp(self):
        self.store = Store(":memory:")
        self.log = self.store.events
        self.seqs = [self.log.append(sample(f"line {i}")) for i in range(5)]

    def tearDown(self):
        self.store.close()

    def test_read_after_is_exclusive_and_ordered(self):
        got = self.log.read_after(0)
        self.assertEqual([seq for seq, _ in got], self.seqs)
        got = self.log.read_after(self.seqs[1])
        self.assertEqual([seq for seq, _ in got], self.seqs[2:])

    def test_read_after_the_end_returns_nothing(self):
        self.assertEqual(self.log.read_after(self.log.latest_seq()), [])

    def test_limit_bounds_the_batch(self):
        self.assertEqual(len(self.log.read_after(0, limit=2)), 2)

    def test_tier_filter_excludes_sensitive_bodies(self):
        self.log.append(events.poller("g", {"type": "new_incident"}, "p", "d"))
        public = self.log.read_after(0, tiers=[events.TIER_PUBLIC])
        self.assertEqual(len(public), 1)
        self.assertEqual(public[0][1]["tier"], events.TIER_PUBLIC)
        # Transcripts are sensitive, so an unauthenticated reader sees none.
        self.assertTrue(all(e["type"] != events.TRANSCRIPT_FINAL
                            for _, e in public))


class Cursors(unittest.TestCase):
    def setUp(self):
        self.store = Store(":memory:")
        self.log = self.store.events

    def tearDown(self):
        self.store.close()

    def test_unknown_output_starts_at_zero(self):
        self.assertEqual(self.log.cursor("discord"), 0)

    def test_advance_is_durable_and_per_output(self):
        for i in range(3):
            self.log.append(sample(f"m{i}"))
        self.log.advance("discord", 2)
        self.assertEqual(self.log.cursor("discord"), 2)
        self.assertEqual(self.log.cursor("feed"), 0,
                         "outputs must not share a position")
        self.assertEqual([s for s, _ in self.log.read_after(self.log.cursor("discord"))],
                         [3])

    def test_cursor_never_moves_backwards(self):
        self.log.advance("discord", 5)
        self.log.advance("discord", 2)
        self.assertEqual(self.log.cursor("discord"), 5,
                         "a late or reordered ack must not rewind the cursor")

    def test_a_stalled_output_falls_behind_rather_than_losing_events(self):
        for i in range(10):
            self.log.append(sample(f"m{i}"))
        self.log.advance("feed", 10)
        # Discord was down for the whole batch; everything is still there.
        pending = self.log.read_after(self.log.cursor("discord"), limit=100)
        self.assertEqual(len(pending), 10)

    def test_cursors_reports_every_output(self):
        self.log.advance("discord", 3)
        self.log.advance("feed", 7)
        self.assertEqual(self.log.cursors(), {"discord": 3, "feed": 7})


class Retention(unittest.TestCase):
    def setUp(self):
        self.store = Store(":memory:", retention_days=30)
        self.log = self.store.events

    def tearDown(self):
        self.store.close()

    def test_sweep_drops_only_events_past_the_window(self):
        now = time.time()
        self.log.append(sample("old"), now=now - 40 * 86400)
        self.log.append(sample("recent"), now=now - 1 * 86400)
        removed, unconsumed = self.log.sweep(cutoff=now - 30 * 86400)
        self.assertEqual(removed, 1)
        self.assertEqual(unconsumed, 0)
        self.assertEqual(self.log.count(), 1)

    def test_sweep_reports_a_backlog_it_had_to_discard(self):
        now = time.time()
        stale = self.log.append(sample("old"), now=now - 40 * 86400)
        self.log.append(sample("recent"), now=now - 1 * 86400)
        # An output that never got past the start of the window.
        self.log.advance("discord", stale - 1)
        removed, unconsumed = self.log.sweep(cutoff=now - 30 * 86400)
        self.assertEqual(removed, 1)
        self.assertEqual(unconsumed, 1,
                         "dropping an unread backlog silently is how you find "
                         "out about a dead output six weeks later")

    def test_store_sweep_covers_rows_and_events(self):
        now = time.time()
        self.log.append(sample("old"), now=now - 40 * 86400)
        rows, dropped, unconsumed = self.store.sweep(now=now)
        self.assertEqual(rows, 0)
        self.assertEqual(dropped, 1)
        self.assertEqual(unconsumed, 0)


class Schema(unittest.TestCase):
    def test_the_per_group_read_uses_an_index(self):
        """Without one, a worker for a quiet group walks the whole tail of the
        log on every pass — under the same lock the transcription thread needs
        to append. LIMIT bounds rows returned, not rows scanned."""
        store = Store(":memory:")
        with store.lock:
            plan = " ".join(
                str(row[-1]) for row in store.conn.execute(
                    "EXPLAIN QUERY PLAN SELECT seq, body FROM events "
                    "WHERE seq > ? AND group_name = ? ORDER BY seq LIMIT ?",
                    (0, "g", 50)))
        store.close()
        self.assertIn("events_group_seq", plan,
                      f"per-group read is not using the index: {plan}")

    def test_migrations_run_in_order_to_head(self):
        import store as store_module
        store = Store(":memory:")
        with store.lock:
            version = store.conn.execute("PRAGMA user_version").fetchone()[0]
        self.assertEqual(version, len(store_module.MIGRATIONS))
        store.close()

    def test_reopening_a_database_does_not_remigrate(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "t.db")
            first = Store(path)
            seq = first.events.append(sample())
            first.close()

            second = Store(path)
            self.assertEqual(second.events.latest_seq(), seq,
                             "reopening must preserve the log, not reset it")
            self.assertEqual(second.events.count(), 1)
            second.close()


if __name__ == "__main__":
    unittest.main()
