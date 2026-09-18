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

    def existing_cursor(self, group="vancouver-island", kind="fake"):
        """Register a cursor at 0, standing in for a destination that has been
        running since before these events.

        Without this a new worker seeds at the head and delivers nothing, which
        is the point of ensure_cursor — so a test that pre-loads the log has to
        say which of the two situations it is exercising.
        """
        self.log.advance(f"{group}:{kind}", 0)

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
        self.existing_cursor()
        for i in range(5):
            self.log.append(transcript(f"line {i}"))
        dest = FakeDestination()
        self.run_worker(dest, until=lambda: len(dest.received) == 5)
        self.assertEqual([t.split(": ")[-1] for t in dest.texts()],
                         [f"line {i}" for i in range(5)])

    def test_cursor_advances_so_a_restart_does_not_resend(self):
        self.existing_cursor()
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
        self.existing_cursor(group="interior")
        self.log.append(transcript("island", group="vancouver-island"))
        self.log.append(transcript("interior", group="interior"))
        dest = FakeDestination()
        self.run_worker(dest, group="interior",
                        until=lambda: len(dest.received) == 1, timeout=2.0)
        self.assertEqual(len(dest.received), 1)
        self.assertEqual(dest.received[0]["group"], "interior")

    def test_suppressed_types_are_recorded_but_not_delivered(self):
        self.existing_cursor()
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
        self.existing_cursor()
        self.log.append(transcript("eventually"))
        dest = FakeDestination(fail_times=2)
        worker = self.run_worker(dest, until=lambda: len(dest.received) == 1,
                                 timeout=10.0)
        self.assertEqual(dest.texts()[0].endswith("eventually"), True)
        self.assertEqual(worker.consecutive_failures, 0, "recovery resets the count")

    def test_a_down_destination_does_not_lose_events(self):
        self.existing_cursor()
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
        self.existing_cursor()
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
        self.existing_cursor()
        self.log.append(transcript("unsent"))
        dest = FakeDestination(fail_times=10_000)
        self.run_worker(dest, until=lambda: dest.attempts >= 1, timeout=5.0)
        self.assertEqual(self.log.cursor("vancouver-island:fake"), 0)
        # A later run with a working destination picks it up.
        self.stop = threading.Event()
        healthy = FakeDestination()
        self.run_worker(healthy, until=lambda: len(healthy.received) == 1)
        self.assertTrue(healthy.texts()[0].endswith("unsent"))


class CursorSeeding(WorkerBase):
    """A new destination must not replay the log it was never subscribed to.

    The log accumulates independently of which destinations exist, so a cursor
    that has never existed starting at 0 means the first deploy of a new output
    re-delivers everything inside the retention window — up to 30 days of
    transcripts into a Discord channel in one burst.
    """

    def test_a_brand_new_cursor_starts_at_the_head(self):
        for i in range(5):
            self.log.append(transcript(f"history {i}"))
        dest = FakeDestination()
        self.run_worker(dest, until=lambda: False, timeout=0.5)
        self.assertEqual(dest.received, [],
                         "a new destination must not replay the existing log")
        self.assertEqual(self.log.cursor("vancouver-island:fake"), 5)

    def test_events_after_the_seed_are_delivered(self):
        self.log.append(transcript("before"))
        worker = outputs.OutputWorker(
            self.log, (dest := FakeDestination()), "vancouver-island", self.stop)
        worker.start()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and self.log.cursor("vancouver-island:fake") == 0:
            time.sleep(0.01)          # wait for the seed to be written
        self.log.append(transcript("after"))
        worker.notify()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and not dest.received:
            time.sleep(0.01)
        self.stop.set(); worker.notify(); worker.join(timeout=5.0)
        self.assertEqual(len(dest.received), 1)
        self.assertTrue(dest.texts()[0].endswith("after"))

    def test_seeding_never_moves_an_existing_cursor(self):
        self.log.append(transcript("queued"))
        self.existing_cursor()        # destination already exists, at 0
        dest = FakeDestination()
        self.run_worker(dest, until=lambda: len(dest.received) == 1)
        self.assertTrue(dest.texts()[0].endswith("queued"),
                        "an established cursor must not be fast-forwarded")

    def test_ensure_cursor_is_idempotent(self):
        self.assertEqual(self.log.ensure_cursor("x", 7), 7)
        self.assertEqual(self.log.ensure_cursor("x", 99), 7,
                         "a second call must not move the cursor")


class ThreadSurvival(WorkerBase):
    """An exception _post did not anticipate must not kill the thread.

    urllib.request.Request raises ValueError on a URL that lost its scheme, and
    http.client.HTTPException is not an OSError, so both escape the handlers in
    _post. A dead worker leaves its destination silently unserved for the life
    of the process — nothing polls status(), and backlog() only runs at exit.
    """

    def test_an_unexpected_exception_does_not_kill_the_worker(self):
        self.existing_cursor()
        self.log.append(transcript("boom"))

        class Exploding:
            kind = "fake"
            def __init__(self): self.attempts = 0
            def deliver(self, event):
                self.attempts += 1
                raise ValueError("unknown url type: 'myhost/ingest'")

        dest = Exploding()
        worker = self.run_worker(dest, until=lambda: dest.attempts >= 3,
                                 timeout=5.0)
        self.assertGreaterEqual(dest.attempts, 3, "it must keep retrying")
        self.assertEqual(self.log.cursor("vancouver-island:fake"), 0,
                         "an undelivered event must not be acknowledged")
        self.assertGreater(worker.consecutive_failures, 0)

    def test_the_worker_recovers_once_the_error_stops(self):
        self.existing_cursor()
        self.log.append(transcript("eventually"))

        class FlakyThenFine:
            kind = "fake"
            def __init__(self): self.attempts = 0; self.received = []
            def deliver(self, event):
                self.attempts += 1
                if self.attempts <= 2:
                    raise RuntimeError("something unforeseen")
                self.received.append(event)

        dest = FlakyThenFine()
        self.run_worker(dest, until=lambda: len(dest.received) == 1, timeout=5.0)
        self.assertEqual(len(dest.received), 1)
        self.assertEqual(self.log.cursor("vancouver-island:fake"), 1)


class Backlog(unittest.TestCase):
    """Backlog is measured per group. Against the global head it never reaches
    zero once a second group exists, so drain() burns its whole timeout at every
    shutdown and then reports groups that are fully caught up as behind."""

    def setUp(self):
        self.store = Store(":memory:")
        self.stop = threading.Event()
        self.manager = outputs.OutputManager(self.store.events, self.stop)
        self.manager.add_group("vi", feed_url="https://a", feed_token="t")
        self.manager.add_group("interior", feed_url="https://b", feed_token="t")

    def tearDown(self):
        self.stop.set()
        self.store.close()

    def test_a_caught_up_group_reads_zero_while_another_group_appends(self):
        self.store.events.append(transcript("a", group="vi"))         # seq 1
        self.store.events.append(transcript("b", group="interior"))   # seq 2
        self.store.events.advance("vi:feed", 1)
        self.store.events.advance("interior:feed", 2)
        self.assertEqual(self.manager.backlog(),
                         {"vi:feed": 0, "interior:feed": 0})

    def test_drain_returns_immediately_when_every_group_is_caught_up(self):
        self.store.events.append(transcript("a", group="vi"))
        self.store.events.append(transcript("b", group="interior"))
        self.store.events.advance("vi:feed", 1)
        self.store.events.advance("interior:feed", 2)
        started = time.monotonic()
        self.assertTrue(self.manager.drain(timeout=2.0))
        self.assertLess(time.monotonic() - started, 1.0,
                        "drain must not burn its timeout on a caught-up manager")

    def test_backlog_counts_only_a_workers_own_group(self):
        for _ in range(3):
            self.store.events.append(transcript("x", group="interior"))
        self.assertEqual(self.manager.backlog()["vi:feed"], 0)
        self.assertEqual(self.manager.backlog()["interior:feed"], 3)

    def test_backlog_cannot_go_negative_after_a_sweep(self):
        self.store.events.append(transcript("old", group="vi"))
        self.store.events.advance("vi:feed", 1)
        self.store.events.sweep(cutoff=time.time() + 60)   # removes everything
        self.assertEqual(self.store.events.count(), 0)
        self.assertEqual(self.manager.backlog()["vi:feed"], 0,
                         "a cursor ahead of an emptied log is not a backlog")

    def test_notify_only_wakes_its_own_group(self):
        by_name = {w.cursor_name: w for w in self.manager.workers}
        self.manager.notify("interior")
        self.assertTrue(by_name["interior:feed"].wake.is_set())
        self.assertFalse(by_name["vi:feed"].wake.is_set(),
                         "a busy group must not wake every other group's workers")

    def test_notify_without_a_group_wakes_everything(self):
        self.manager.notify()
        self.assertTrue(all(w.wake.is_set() for w in self.manager.workers))


class InlineFallback(unittest.TestCase):
    """--no-store, a store that fails to open, and a failed append all fall back
    to inline sends. That path must suppress what the workers suppress, or those
    situations start announcing events that have never been announced."""

    def setUp(self):
        import scanner_transcribe_gpu as stg   # pulls numpy
        self.stg = stg
        self.posted = []
        self._original = stg._post_inline
        stg._post_inline = lambda url, payload, headers: self.posted.append(payload)
        # No event_log, so emit() takes the degraded path.
        self.out = stg.GroupOutputs(discord_webhook="https://example.invalid/hook")

    def tearDown(self):
        self.stg._post_inline = self._original

    def test_ordinary_events_are_sent_inline(self):
        self.out.emit(transcript("ordinary"))
        self.assertEqual(len(self.posted), 1)
        self.assertTrue(self.posted[0]["content"].endswith("ordinary"))

    def test_suppressed_types_are_not_sent_inline(self):
        self.out.emit(events.poller(
            "vancouver-island", {"type": "wildfire_removed", "guid": "x"}, "p", "d"))
        self.assertEqual(self.posted, [],
                         "wildfire.removed must not be announced inline either")

    def test_suppression_does_not_block_what_follows(self):
        self.out.emit(events.poller(
            "vancouver-island", {"type": "wildfire_removed", "guid": "x"}, "p", "d"))
        self.out.emit(transcript("after"))
        self.assertEqual(len(self.posted), 1)


class ClientErrorGrace(WorkerBase):
    """401/403 are retryable because a rotated token is usually fixable — but
    "usually" is not "always", and an unbounded retry wedges the destination on
    one event forever while emit() keeps appending behind it and the retention
    sweep discards the backlog. After the grace, the cursor has to move again.
    """

    def setUp(self):
        super().setUp()
        self._grace = outputs.CLIENT_ERROR_GRACE_S

    def tearDown(self):
        outputs.CLIENT_ERROR_GRACE_S = self._grace
        super().tearDown()

    def bounded(self, message="HTTP 401 Unauthorized"):
        return outputs.RetryableFailure(message, bounded=True)

    def test_within_the_grace_the_event_is_retried_not_dropped(self):
        outputs.CLIENT_ERROR_GRACE_S = 3600
        self.existing_cursor()
        self.log.append(transcript("queued"))
        dest = FakeDestination(fail_times=10_000, error=self.bounded())
        worker = self.run_worker(dest, until=lambda: dest.attempts >= 3, timeout=5.0)
        self.assertEqual(self.log.cursor("vancouver-island:fake"), 0,
                         "inside the grace the event stays queued")
        self.assertEqual(worker.skipped, 0)

    def test_past_the_grace_the_stream_flows_again(self):
        outputs.CLIENT_ERROR_GRACE_S = 0   # grace already expired
        self.existing_cursor()
        for i in range(3):
            self.log.append(transcript(f"line {i}"))
        dest = FakeDestination(fail_times=10_000, error=self.bounded())
        worker = self.run_worker(
            dest, until=lambda: self.log.cursor("vancouver-island:fake") == 3,
            timeout=5.0)
        self.assertEqual(self.log.cursor("vancouver-island:fake"), 3,
                         "a permanently dead endpoint must not wedge the cursor")
        self.assertEqual(worker.skipped, 3)

    def test_a_success_resets_the_grace_clock(self):
        outputs.CLIENT_ERROR_GRACE_S = 3600
        self.existing_cursor()
        self.log.append(transcript("first"))
        dest = FakeDestination(fail_times=2, error=self.bounded())
        worker = self.run_worker(dest, until=lambda: len(dest.received) == 1,
                                 timeout=5.0)
        self.assertIsNone(worker.client_error_since,
                          "a delivery that lands clears the failing run")
        self.assertEqual(worker.skipped, 0)

    def test_rate_limiting_is_never_bounded(self):
        """429 means 'slow down', not 'your credentials are wrong'. Dropping
        events after an hour of heavy traffic would be the wrong response."""
        import urllib.error
        err = urllib.error.HTTPError(
            "http://x", 429, "Too Many Requests", {"Retry-After": "1"}, None)
        with self.assertRaises(outputs.RetryableFailure) as caught:
            outputs._raise_for_http_error(err)
        self.assertFalse(caught.exception.bounded)

    def test_server_errors_are_never_bounded(self):
        import urllib.error
        err = urllib.error.HTTPError("http://x", 503, "Unavailable", {}, None)
        with self.assertRaises(outputs.RetryableFailure) as caught:
            outputs._raise_for_http_error(err)
        self.assertFalse(caught.exception.bounded)

    def test_an_unbounded_failure_is_never_dropped_by_the_grace(self):
        outputs.CLIENT_ERROR_GRACE_S = 0
        self.existing_cursor()
        self.log.append(transcript("kept"))
        dest = FakeDestination(fail_times=10_000)   # plain retryable, unbounded
        worker = self.run_worker(dest, until=lambda: dest.attempts >= 3, timeout=5.0)
        self.assertEqual(self.log.cursor("vancouver-island:fake"), 0,
                         "a network outage must still queue indefinitely")
        self.assertEqual(worker.skipped, 0)

    def test_auth_codes_are_marked_bounded(self):
        import urllib.error
        for code in (401, 403, 408):
            err = urllib.error.HTTPError("http://x", code, "nope", {}, None)
            with self.subTest(code=code):
                with self.assertRaises(outputs.RetryableFailure) as caught:
                    outputs._raise_for_http_error(err)
                self.assertTrue(caught.exception.bounded)

    def test_an_unrelated_outage_does_not_count_toward_the_grace(self):
        """The grace measures a continuous run of client errors. A network
        outage in the middle is not evidence the credentials are dead, and
        letting it accumulate drops events for an auth blip that had only just
        started — e.g. the feed host is unreachable for an hour, comes back, and
        returns 401 briefly during a token rotation."""
        outputs.CLIENT_ERROR_GRACE_S = 0.5
        self.existing_cursor()
        self.log.append(transcript("survives"))

        class Sequence:
            kind = "fake"
            def __init__(self): self.n = 0; self.received = []
            def deliver(self, event):
                self.n += 1
                if self.n == 1:
                    raise outputs.RetryableFailure("HTTP 401", bounded=True)
                if self.n <= 100:      # unrelated outage, longer than the grace
                    raise outputs.RetryableFailure("connection refused")
                if self.n <= 110:      # auth again, well inside the grace
                    raise outputs.RetryableFailure("HTTP 401", bounded=True)
                self.received.append(event)

        dest = Sequence()
        worker = self.run_worker(dest, until=lambda: bool(dest.received),
                                 timeout=8.0)
        self.assertEqual(worker.skipped, 0,
                         "the auth error was never continuous past the grace")
        self.assertEqual(len(dest.received), 1)

    def test_an_unexpected_error_also_clears_the_grace_clock(self):
        """An unexpected exception is no more an auth failure than a 5xx is."""
        outputs.CLIENT_ERROR_GRACE_S = 3600
        self.existing_cursor()
        self.log.append(transcript("x"))

        class AuthThenSomethingElse:
            kind = "fake"
            def __init__(self): self.n = 0
            def deliver(self, event):
                self.n += 1
                if self.n == 1:
                    raise outputs.RetryableFailure("HTTP 401", bounded=True)
                raise ValueError("unknown url type")

        dest = AuthThenSomethingElse()
        worker = self.run_worker(dest, until=lambda: dest.n >= 4, timeout=5.0)
        self.assertIsNone(worker.client_error_since)

    def test_a_non_bounded_failure_restarts_the_clock(self):
        outputs.CLIENT_ERROR_GRACE_S = 3600
        self.existing_cursor()
        self.log.append(transcript("x"))

        class AuthThenOutage:
            kind = "fake"
            def __init__(self): self.n = 0
            def deliver(self, event):
                self.n += 1
                if self.n == 1:
                    raise outputs.RetryableFailure("HTTP 401", bounded=True)
                raise outputs.RetryableFailure("connection refused")

        dest = AuthThenOutage()
        worker = self.run_worker(dest, until=lambda: dest.n >= 4, timeout=5.0)
        self.assertIsNone(worker.client_error_since,
                          "a non-client failure clears the client-error run")


class FailureVisibility(WorkerBase):
    """A worker that warns once and then retries in silence for a month is the
    same invisibility that hid the thread-death bug — it just hides a stuck
    thread instead of a dead one."""

    def setUp(self):
        super().setUp()
        self._rewarn = outputs.FAILURE_REWARN_S

    def tearDown(self):
        outputs.FAILURE_REWARN_S = self._rewarn
        super().tearDown()

    def run_capturing(self, rewarn_s, attempts):
        import contextlib, io
        outputs.FAILURE_REWARN_S = rewarn_s
        self.existing_cursor()
        self.log.append(transcript("stuck"))
        dest = FakeDestination(fail_times=10_000)
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            self.run_worker(dest, until=lambda: dest.attempts >= attempts,
                            timeout=5.0)
        return [line for line in buffer.getvalue().splitlines() if "[warn]" in line]

    def test_a_stuck_worker_keeps_reporting(self):
        warnings = self.run_capturing(rewarn_s=0, attempts=5)
        self.assertGreater(len(warnings), 1,
                           "a destination stuck for a month must say so more "
                           "than once at the start of it")

    def test_it_does_not_warn_on_every_single_retry(self):
        warnings = self.run_capturing(rewarn_s=3600, attempts=5)
        self.assertEqual(len(warnings), 1,
                         "loud on the way down, quiet while it stays down")

    def test_the_warning_says_how_much_is_queued(self):
        warnings = self.run_capturing(rewarn_s=0, attempts=3)
        self.assertIn("pending", warnings[0])

    def test_pending_counts_only_this_group(self):
        self.existing_cursor()
        self.log.append(transcript("mine", group="vancouver-island"))
        self.log.append(transcript("theirs", group="interior"))
        worker = outputs.OutputWorker(
            self.log, FakeDestination(), "vancouver-island", self.stop)
        self.assertEqual(worker.pending(), 1)

    def test_a_throttled_warning_does_not_query_the_store(self):
        """The throttle has to be checked before the message is built. A
        pending() count interpolated into an argument is evaluated on every
        retry even when the warning is discarded — and that query takes the same
        lock the transcription thread needs in order to append."""
        outputs.FAILURE_REWARN_S = 3600     # everything after the first is thrown away
        self.existing_cursor()
        self.log.append(transcript("stuck"))

        calls = []
        real = type(self.log).latest_seq
        type(self.log).latest_seq = lambda s, group=None: (
            calls.append(1), real(s, group=group))[1]
        try:
            dest = FakeDestination(fail_times=10_000)
            self.run_worker(dest, until=lambda: dest.attempts >= 20, timeout=5.0)
        finally:
            type(self.log).latest_seq = real

        self.assertGreaterEqual(dest.attempts, 20)
        self.assertLess(len(calls), 10,
                        f"{len(calls)} store reads for one printed warning across "
                        f"{dest.attempts} retries — the throttle is saving the "
                        f"print but not the query")


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
        for code in (400, 404, 410):
            err = urllib.error.HTTPError("http://x", code, "nope", {}, None)
            with self.assertRaises(outputs.PermanentFailure):
                outputs._raise_for_http_error(err)

    def test_server_errors_are_retryable(self):
        import urllib.error
        err = urllib.error.HTTPError("http://x", 503, "Unavailable", {}, None)
        with self.assertRaises(outputs.RetryableFailure):
            outputs._raise_for_http_error(err)

    def test_recoverable_client_errors_are_retryable(self):
        """A rotated token or a revoked permission is fixable. Classifying it
        permanent discards every event from the outage window, which is exactly
        what keeping a cursor is supposed to prevent."""
        import urllib.error
        for code in (401, 403, 408):
            err = urllib.error.HTTPError("http://x", code, "nope", {}, None)
            with self.subTest(code=code):
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


class FeedApiVersion(unittest.TestCase):
    """The scanner and the Worker deploy independently, so for a while either
    can be the older one. The fallback means that does not have to be
    sequenced by hand."""

    def setUp(self):
        self.dest = outputs.FeedDestination("https://feed.example/", "tok")
        self.posted = []
        self._real = outputs._post

    def tearDown(self):
        outputs._post = self._real

    def stub(self, on_v1=None):
        def _post(url, payload, headers):
            self.posted.append((url, payload))
            if url.endswith("/v1/ingest") and on_v1 is not None:
                raise on_v1
        outputs._post = _post

    def event(self):
        return events.transcript("g", "Mid Island", "engine three", 2.0)

    def test_it_posts_the_envelope_to_v1(self):
        self.stub()
        self.dest.deliver(self.event())
        url, payload = self.posted[0]
        self.assertEqual(url, "https://feed.example/v1/ingest")
        self.assertEqual(payload["type"], events.TRANSCRIPT_FINAL)
        self.assertIn("data", payload)
        self.assertIn("tier", payload)

    def test_a_404_falls_back_to_the_v0_shape(self):
        self.stub(on_v1=outputs.PermanentFailure("HTTP 404 Not Found", status=404))
        self.dest.deliver(self.event())
        urls = [u for u, _ in self.posted]
        self.assertEqual(urls, ["https://feed.example/v1/ingest",
                                "https://feed.example/ingest"])
        _, legacy = self.posted[-1]
        self.assertIn("line", legacy)
        self.assertEqual(legacy["type"], "transcript")

    def test_the_fallback_sticks(self):
        """One wasted request, not one per event."""
        self.stub(on_v1=outputs.PermanentFailure("HTTP 404 Not Found", status=404))
        self.dest.deliver(self.event())
        self.posted.clear()
        self.dest.deliver(self.event())
        self.assertEqual([u for u, _ in self.posted], ["https://feed.example/ingest"])

    def test_other_permanent_failures_are_not_swallowed(self):
        """A 401 means a bad token, not an old Worker. Falling back would hide
        it and post to a second endpoint that will also reject."""
        self.stub(on_v1=outputs.PermanentFailure("HTTP 401 Unauthorized", status=401))
        with self.assertRaises(outputs.PermanentFailure):
            self.dest.deliver(self.event())
        self.assertTrue(self.dest.use_v1, "still v1; this was not a version problem")

    def test_a_retryable_failure_does_not_trigger_the_fallback(self):
        self.stub(on_v1=outputs.RetryableFailure("connection refused"))
        with self.assertRaises(outputs.RetryableFailure):
            self.dest.deliver(self.event())
        self.assertTrue(self.dest.use_v1)


class FeedFallbackPrecision(unittest.TestCase):
    """The fallback exists for one situation — a Worker that predates
    /v1/ingest — and must not fire for anything that merely looks like it."""

    def setUp(self):
        self.dest = outputs.FeedDestination("https://feed.example/", "tok")
        self.posted = []
        self._real = outputs._post

    def tearDown(self):
        outputs._post = self._real

    def event(self):
        return events.transcript("g", "Mid Island", "engine three", 2.0)

    def stub(self, v1_error=None, v0_error=None):
        def _post(url, payload, headers):
            self.posted.append(url)
            if url.endswith("/v1/ingest") and v1_error:
                raise v1_error
            if url.endswith("/ingest") and not url.endswith("/v1/ingest") and v0_error:
                raise v0_error
        outputs._post = _post

    def test_the_status_is_carried_not_parsed_out_of_the_message(self):
        """A reason phrase can contain "404" without the status being 404."""
        error = outputs.PermanentFailure("HTTP 400 Bad Request: field 404 invalid",
                                         status=400)
        self.stub(v1_error=error)
        with self.assertRaises(outputs.PermanentFailure):
            self.dest.deliver(self.event())
        self.assertTrue(self.dest.use_v1, "a 400 is not an old Worker")

    def test_a_misconfigured_url_does_not_latch(self):
        """A wrong feed_url 404s for both paths. Latching on the v1 404 alone
        would report "your Worker is old", which is the wrong diagnosis, and
        then hide the real cause behind it for the life of the process."""
        not_found = outputs.PermanentFailure("HTTP 404 Not Found", status=404)
        self.stub(v1_error=not_found, v0_error=not_found)
        with self.assertRaises(outputs.PermanentFailure):
            self.dest.deliver(self.event())
        self.assertTrue(self.dest.use_v1,
                        "v0 never succeeded, so nothing was proven about v1")

    def test_it_keeps_trying_v1_until_v0_actually_works(self):
        not_found = outputs.PermanentFailure("HTTP 404 Not Found", status=404)
        self.stub(v1_error=not_found, v0_error=not_found)
        for _ in range(3):
            with self.assertRaises(outputs.PermanentFailure):
                self.dest.deliver(self.event())
        self.assertEqual(self.posted.count("https://feed.example/v1/ingest"), 3,
                         "a bad URL must not be misreported as an old Worker")

    def test_it_latches_once_v0_succeeds(self):
        self.stub(v1_error=outputs.PermanentFailure("HTTP 404 Not Found", status=404))
        self.dest.deliver(self.event())
        self.assertFalse(self.dest.use_v1)
        self.posted.clear()
        self.dest.deliver(self.event())
        self.assertEqual(self.posted, ["https://feed.example/ingest"])

    def test_the_http_classifier_records_the_status(self):
        import urllib.error
        err = urllib.error.HTTPError("http://x", 404, "Not Found", {}, None)
        with self.assertRaises(outputs.PermanentFailure) as caught:
            outputs._raise_for_http_error(err)
        self.assertEqual(caught.exception.status, 404)
