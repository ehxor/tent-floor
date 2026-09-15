"""The point of Phase 2 is that a broken destination can no longer reach back
and hurt capture. These tests exercise the failure modes that used to be
invisible: an endpoint that is down, one that will never accept the event, and
a shutdown with deliveries still in flight.
"""

import sys
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import events
import outputs
from store import Store


class FakeDestination:
    """Records what it received and fails on command."""

    kind = "fake"

    def __init__(self, fail_times=0, error=None):
        self.received = []
        self.attempts = 0
        self.fail_times = fail_times
        self.error = error or outputs.RetryableFailure("down")
        self.lock = threading.Lock()

    def deliver(self, event):
        with self.lock:
            self.attempts += 1
            if self.attempts <= self.fail_times:
                raise self.error
            self.received.append(event)

    def texts(self):
        with self.lock:
            return [e["render"]["plain"] for e in self.received]


def transcript(text, group="vancouver-island"):
    return events.transcript(group, "Mid Island", text, 1.0)


class WorkerBase(unittest.TestCase):
    def setUp(self):
        self.store = Store(":memory:")
        self.log = self.store.events
        self.stop = threading.Event()
        # Tests assert retry *behaviour*, not the production pacing. Without
        # this the suite spends most of its time asleep in backoff.
        self._saved = (outputs.BACKOFF_S, outputs.IDLE_POLL_S)
        outputs.BACKOFF_S = (0.01,)
        outputs.IDLE_POLL_S = 0.02

    def tearDown(self):
        self.stop.set()
        outputs.BACKOFF_S, outputs.IDLE_POLL_S = self._saved
        self.store.close()

    def run_worker(self, destination, group="vancouver-island", until=None,
                   timeout=5.0):
        """Run a worker until `until()` is true, then stop it."""
        worker = outputs.OutputWorker(self.log, destination, group, self.stop)
        worker.start()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if until is None or until():
                break
            time.sleep(0.01)
        self.stop.set()
        worker.notify()   # wake it out of an idle wait, as OutputManager.stop does
        worker.join(timeout=5.0)
        self.assertFalse(worker.is_alive(), "worker did not stop")
        return worker


class Delivery(WorkerBase):
    def test_events_are_delivered_in_order(self):
        for i in range(5):
            self.log.append(transcript(f"line {i}"))
        dest = FakeDestination()
        self.run_worker(dest, until=lambda: len(dest.received) == 5)
        self.assertEqual([t.split(": ")[-1] for t in dest.texts()],
                         [f"line {i}" for i in range(5)])

    def test_cursor_advances_so_a_restart_does_not_resend(self):
        self.log.append(transcript("once"))
        dest = FakeDestination()
        self.run_worker(dest, until=lambda: len(dest.received) == 1)
        self.assertEqual(self.log.cursor("vancouver-island:fake"), 1)

        # A second worker on the same cursor has nothing to do.
        self.stop = threading.Event()
        again = FakeDestination()
        self.run_worker(again, until=lambda: False, timeout=0.5)
        self.assertEqual(again.received, [])

    def test_a_worker_only_sees_its_own_group(self):
        self.log.append(transcript("island", group="vancouver-island"))
        self.log.append(transcript("interior", group="interior"))
        dest = FakeDestination()
        self.run_worker(dest, group="interior",
                        until=lambda: len(dest.received) == 1, timeout=2.0)
        self.assertEqual(len(dest.received), 1)
        self.assertEqual(dest.received[0]["group"], "interior")

    def test_suppressed_types_are_recorded_but_not_delivered(self):
        self.log.append(events.poller(
            "vancouver-island", {"type": "wildfire_removed", "guid": "g"}, "p", "d"))
        self.log.append(transcript("after"))
        dest = FakeDestination()
        self.run_worker(dest, until=lambda: len(dest.received) == 1)
        self.assertEqual(len(dest.received), 1,
                         "wildfire.removed must not be announced")
        self.assertEqual(self.log.count(), 2, "but it must still be recorded")
        self.assertEqual(self.log.cursor("vancouver-island:fake"), 2,
                         "a suppressed event must not wedge the cursor")


class Failure(WorkerBase):
    def test_a_retryable_failure_is_retried_until_it_lands(self):
        self.log.append(transcript("eventually"))
        dest = FakeDestination(fail_times=2)
        worker = self.run_worker(dest, until=lambda: len(dest.received) == 1,
                                 timeout=10.0)
        self.assertEqual(dest.texts()[0].endswith("eventually"), True)
        self.assertEqual(worker.consecutive_failures, 0, "recovery resets the count")

    def test_a_down_destination_does_not_lose_events(self):
        for i in range(3):
            self.log.append(transcript(f"line {i}"))
        dest = FakeDestination(fail_times=10_000)   # never recovers
        worker = self.run_worker(dest, until=lambda: dest.attempts >= 2,
                                 timeout=5.0)
        self.assertEqual(dest.received, [])
        self.assertEqual(self.log.cursor("vancouver-island:fake"), 0,
                         "nothing may be acknowledged that was not delivered")
        self.assertEqual(self.log.count(), 3, "the events are queued, not dropped")
        self.assertGreater(worker.consecutive_failures, 0)

    def test_a_permanent_failure_is_skipped_so_it_cannot_wedge_the_queue(self):
        self.log.append(transcript("poison"))
        self.log.append(transcript("good"))
        dest = FakeDestination(fail_times=1,
                               error=outputs.PermanentFailure("HTTP 404 Not Found"))
        worker = self.run_worker(dest, until=lambda: len(dest.received) == 1,
                                 timeout=5.0)
        self.assertEqual(len(dest.received), 1)
        self.assertTrue(dest.texts()[0].endswith("good"),
                        "the event behind the poison one must still go out")
        self.assertEqual(worker.skipped, 1)
        self.assertEqual(self.log.cursor("vancouver-island:fake"), 2)

    def test_stopping_mid_retry_leaves_the_event_for_next_start(self):
        self.log.append(transcript("unsent"))
        dest = FakeDestination(fail_times=10_000)
        self.run_worker(dest, until=lambda: dest.attempts >= 1, timeout=5.0)
        self.assertEqual(self.log.cursor("vancouver-island:fake"), 0)
        # A later run with a working destination picks it up.
        self.stop = threading.Event()
        healthy = FakeDestination()
        self.run_worker(healthy, until=lambda: len(healthy.received) == 1)
        self.assertTrue(healthy.texts()[0].endswith("unsent"))


class Classification(unittest.TestCase):
    """_post turns transport errors into the two kinds the worker acts on."""

    def test_rate_limit_is_retryable_and_honours_retry_after(self):
        import urllib.error
        err = urllib.error.HTTPError(
            "http://x", 429, "Too Many Requests", {"Retry-After": "2.5"}, None)
        with self.assertRaises(outputs.RetryableFailure) as caught:
            outputs._raise_for_http_error(err)
        self.assertEqual(caught.exception.retry_after, 2.5)

    def test_client_errors_are_permanent(self):
        import urllib.error
        for code in (400, 401, 404):
            err = urllib.error.HTTPError("http://x", code, "nope", {}, None)
            with self.assertRaises(outputs.PermanentFailure):
                outputs._raise_for_http_error(err)

    def test_server_errors_are_retryable(self):
        import urllib.error
        err = urllib.error.HTTPError("http://x", 503, "Unavailable", {}, None)
        with self.assertRaises(outputs.RetryableFailure):
            outputs._raise_for_http_error(err)

    def test_a_malformed_retry_after_still_retries(self):
        import urllib.error
        err = urllib.error.HTTPError(
            "http://x", 429, "Too Many", {"Retry-After": "soon"}, None)
        with self.assertRaises(outputs.RetryableFailure) as caught:
            outputs._raise_for_http_error(err)
        self.assertIsNone(caught.exception.retry_after,
                          "an unparseable header falls back to normal backoff")


class Manager(unittest.TestCase):
    def setUp(self):
        self.store = Store(":memory:")
        self.stop = threading.Event()
        self.manager = outputs.OutputManager(self.store.events, self.stop)

    def tearDown(self):
        self.stop.set()
        self.store.close()

    def test_only_configured_destinations_get_workers(self):
        self.manager.add_group("vancouver-island", discord_webhook="https://d")
        self.manager.add_group("interior", feed_url="https://f", feed_token="t")
        self.assertEqual([w.cursor_name for w in self.manager.workers],
                         ["vancouver-island:discord", "interior:feed"])

    def test_a_group_with_no_destinations_gets_no_workers(self):
        self.manager.add_group("quiet")
        self.assertEqual(self.manager.workers, [])

    def test_backlog_reports_how_far_behind_each_worker_is(self):
        self.manager.add_group("vancouver-island", discord_webhook="https://d")
        for i in range(3):
            self.store.events.append(transcript(f"m{i}"))
        self.assertEqual(self.manager.backlog(), {"vancouver-island:discord": 3})
        self.store.events.advance("vancouver-island:discord", 3)
        self.assertEqual(self.manager.backlog(), {"vancouver-island:discord": 0})

    def test_notify_does_not_raise_without_workers(self):
        self.manager.notify()

    def test_stop_wakes_an_idle_worker_promptly(self):
        saved = outputs.IDLE_POLL_S
        outputs.IDLE_POLL_S = 30.0   # far longer than the join below tolerates
        try:
            worker = outputs.OutputWorker(
                self.store.events, FakeDestination(), "vancouver-island", self.stop)
            self.manager.workers.append(worker)
            self.manager.start()
            time.sleep(0.05)         # let it reach the idle wait
            self.manager.stop()
            worker.join(timeout=2.0)
            self.assertFalse(worker.is_alive(),
                             "stop must wake an idle worker, not wait out the poll")
        finally:
            outputs.IDLE_POLL_S = saved


if __name__ == "__main__":
    unittest.main()
