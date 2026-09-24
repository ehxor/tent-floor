"""The silence this fixes: PulsePoint went behind an AWS WAF and answered
every poll with an empty 202 challenge for eight days, while the poller
reported "no data returned (try again)" onto a stderr nobody was attached to.

Two things are pinned here. fetch_incidents names the cause of a failure
instead of collapsing every one of them into None, and PollerHealth turns a
poll-by-poll failure into a few durable rows rather than 2,880 a day or one
row and then silence.
"""

import io
import json
import sys
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import events
import pulsepoint_poller as pp
from store import HEALTH_TYPES, print_health, Store


class FakeResponse:
    """Enough of http.client.HTTPResponse for fetch_incidents."""

    def __init__(self, body=b"", status=200, headers=None):
        self._body = body
        self.status = status
        self.headers = headers or {}

    def read(self):
        return self._body

    def getcode(self):
        return self.status


def _urlopen(response):
    return mock.patch.object(pp.urllib.request, "urlopen",
                             return_value=response)


def _urlopen_raises(exc):
    return mock.patch.object(pp.urllib.request, "urlopen", side_effect=exc)


# ---------------------------------------------------------------------------
# fetch_incidents
# ---------------------------------------------------------------------------
class FetchDiagnosisTests(unittest.TestCase):

    def test_waf_challenge_is_named_and_not_retryable(self):
        """The failure that started this. A 202 with an empty body and the WAF
        header is a bot block, and retrying the identical request never clears
        it — so it must not be reported as the alternating-empty-poll quirk."""
        resp = FakeResponse(b"", status=202,
                            headers={"x-amzn-waf-action": "challenge"})
        with _urlopen(resp):
            result = pp.fetch_incidents("EMS1201")

        self.assertFalse(result.ok)
        self.assertIsNone(result.active)
        self.assertEqual(result.error.kind, "waf_challenge")
        self.assertFalse(result.error.retryable)
        self.assertIn("challenge", result.error.detail)

    def test_waf_challenge_on_an_error_status(self):
        """WAF can answer with 403 rather than 202; it is the same block."""
        err = urllib.error.HTTPError(
            "url", 403, "Forbidden", {"x-amzn-waf-action": "block"}, None)
        with _urlopen_raises(err):
            result = pp.fetch_incidents("EMS1201")

        self.assertEqual(result.error.kind, "waf_challenge")
        self.assertFalse(result.error.retryable)

    def test_http_error_without_waf_header(self):
        err = urllib.error.HTTPError("url", 503, "Service Unavailable", {}, None)
        with _urlopen_raises(err):
            result = pp.fetch_incidents("EMS1201")

        self.assertEqual(result.error.kind, "http_error")
        self.assertIn("503", result.error.detail)
        self.assertTrue(result.error.retryable)

    def test_timeout_is_network(self):
        with _urlopen_raises(urllib.error.URLError(TimeoutError("timed out"))):
            result = pp.fetch_incidents("EMS1201")

        self.assertEqual(result.error.kind, "network")
        self.assertTrue(result.error.retryable)

    def test_empty_body_without_waf_header_stays_distinct(self):
        """PulsePoint really does return empty bodies on alternate polls. That
        is a different fact from being blocked, so it keeps its own kind."""
        with _urlopen(FakeResponse(b"", status=200)):
            result = pp.fetch_incidents("EMS1201")

        self.assertEqual(result.error.kind, "empty_response")

    def test_non_json_body_is_malformed(self):
        with _urlopen(FakeResponse(b"<html>challenge page</html>", status=200)):
            result = pp.fetch_incidents("EMS1201")

        self.assertEqual(result.error.kind, "malformed")

    def test_undecryptable_body(self):
        body = json.dumps({"ct": "AAAA", "iv": "00" * 16, "s": "00" * 8})
        with _urlopen(FakeResponse(body.encode(), status=200)):
            result = pp.fetch_incidents("EMS1201")

        self.assertEqual(result.error.kind, "decrypt_failed")
        self.assertFalse(result.error.retryable)

    def test_success_carries_both_lists(self):
        payload = {"incidents": {"active": [{"ID": "1"}], "recent": []}}
        body = json.dumps({"ct": "x", "iv": "y", "s": "z"}).encode()
        with _urlopen(FakeResponse(body, status=200)), \
                mock.patch.object(pp, "decrypt_pulsepoint", return_value=payload):
            result = pp.fetch_incidents("EMS1201")

        self.assertTrue(result.ok)
        self.assertEqual(result.active, [{"ID": "1"}])
        self.assertEqual(result.recent, [])

    def test_quiet_agency_is_not_a_failure(self):
        """No active calls must stay distinguishable from cannot look."""
        payload = {"incidents": {"active": [], "recent": []}}
        body = json.dumps({"ct": "x", "iv": "y", "s": "z"}).encode()
        with _urlopen(FakeResponse(body, status=200)), \
                mock.patch.object(pp, "decrypt_pulsepoint", return_value=payload):
            result = pp.fetch_incidents("EMS1201")

        self.assertTrue(result.ok)
        self.assertEqual(result.active, [])


# ---------------------------------------------------------------------------
# PollerHealth
# ---------------------------------------------------------------------------
class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


class PollerHealthTests(unittest.TestCase):

    def setUp(self):
        self.clock = FakeClock()
        self.emitted = []
        self.health = events.PollerHealth(
            "pulsepoint", target="EMS1201", group="vancouver-island",
            emit=self.emitted.append, rewarn_s=300, clock=self.clock)

    def test_first_failure_is_recorded_immediately(self):
        event = self.health.failed("waf_challenge", "HTTP 202", retryable=False)

        self.assertIsNotNone(event)
        self.assertEqual(event["type"], events.POLLER_ERROR)
        self.assertEqual(event["group"], "vancouver-island")
        self.assertEqual(event["data"]["poller"], "pulsepoint")
        self.assertEqual(event["data"]["target"], "EMS1201")
        self.assertEqual(event["data"]["kind"], "waf_challenge")
        self.assertFalse(event["data"]["retryable"])
        self.assertEqual(event["data"]["consecutive"], 1)
        self.assertEqual(self.emitted, [event])

    def test_unchanged_failure_is_throttled_then_rewarned(self):
        """A 30s poll interval would write 2,880 rows a day. One row, then one
        per rewarn window, and each carries how long it has been going."""
        self.health.failed("waf_challenge", "HTTP 202")
        for _ in range(9):
            self.clock.advance(30)
            self.assertIsNone(self.health.failed("waf_challenge", "HTTP 202"))
        self.assertEqual(len(self.emitted), 1)

        self.clock.advance(30)                      # 300s since the first
        event = self.health.failed("waf_challenge", "HTTP 202")
        self.assertIsNotNone(event)
        self.assertEqual(event["data"]["consecutive"], 11)
        self.assertAlmostEqual(event["data"]["failing_for_s"], 300.0)
        self.assertIn("×11", event["render"]["plain"])

    def test_a_different_kind_starts_a_new_run(self):
        """Eight days of timeouts followed by a WAF block is two facts. The
        second must not be swallowed by the first one's throttle window."""
        self.health.failed("network", "timed out")
        self.clock.advance(30)

        event = self.health.failed("waf_challenge", "HTTP 202")
        self.assertIsNotNone(event)
        self.assertEqual(event["data"]["kind"], "waf_challenge")
        self.assertEqual(event["data"]["consecutive"], 1)
        self.assertEqual(len(self.emitted), 2)

    def test_recovery_reports_the_outage(self):
        self.health.failed("network", "timed out")
        for _ in range(4):
            self.clock.advance(30)
            self.health.failed("network", "timed out")

        self.clock.advance(30)
        event = self.health.ok()

        self.assertEqual(event["type"], events.POLLER_RECOVERED)
        self.assertEqual(event["data"]["kind"], "network")
        self.assertEqual(event["data"]["consecutive"], 5)
        self.assertAlmostEqual(event["data"]["outage_s"], 150.0)
        self.assertFalse(self.health.failing)

    def test_success_while_healthy_emits_nothing(self):
        """The common case. A working poller must not write a row per poll."""
        self.assertIsNone(self.health.ok())
        self.clock.advance(30)
        self.assertIsNone(self.health.ok())
        self.assertEqual(self.emitted, [])

    def test_failing_again_after_recovery_records_the_new_run(self):
        self.health.failed("network", "timed out")
        self.clock.advance(30)
        self.health.ok()
        self.clock.advance(30)

        event = self.health.failed("network", "timed out")
        self.assertIsNotNone(event)
        self.assertEqual(event["data"]["consecutive"], 1)

    def test_health_events_are_not_delivered_to_outputs(self):
        """They are operator data. A public incident channel is the wrong place
        to learn a poller is wedged, and it would arrive every rewarn window."""
        import outputs
        self.assertIn(events.POLLER_ERROR, outputs.SUPPRESSED_TYPES)
        self.assertIn(events.POLLER_RECOVERED, outputs.SUPPRESSED_TYPES)


# ---------------------------------------------------------------------------
# End to end: a blocked poller leaves a durable trail
# ---------------------------------------------------------------------------
class PollerPersistenceTests(unittest.TestCase):

    def _run_one_poll(self, poller, response):
        """One pass of the poll loop. stop_event is set from inside the fetch,
        so the body runs to completion and the loop then falls out — setting it
        beforehand would skip the body entirely and pass vacuously."""
        def urlopen(*args, **kwargs):
            poller.stop_event.set()
            return response

        with mock.patch.object(pp.urllib.request, "urlopen", urlopen), \
                mock.patch.object(sys, "stderr", io.StringIO()):
            poller._poll_loop()

    def test_blocked_poll_is_written_to_the_event_log(self):
        """The whole point: the reason survives the process that found it."""
        store = Store(":memory:")
        self.addCleanup(store.close)

        poller = pp.PulsePointPoller(agency_id="EMS1201", store=store,
                                     scope="test", group="vancouver-island")
        self._run_one_poll(poller, FakeResponse(
            b"", status=202, headers={"x-amzn-waf-action": "challenge"}))

        rows = store.events.recent(types=(events.POLLER_ERROR,))
        self.assertEqual(len(rows), 1)
        _, event = rows[0]
        self.assertEqual(event["data"]["kind"], "waf_challenge")
        self.assertEqual(event["data"]["target"], "EMS1201")
        self.assertEqual(event["group"], "vancouver-island")
        self.assertFalse(event["data"]["retryable"])

    def test_tracker_is_not_run_on_a_failed_poll(self):
        """A failed fetch used to be indistinguishable from an empty snapshot
        one layer up. Handing None or [] to the tracker would read every live
        incident as cleared and announce it."""
        store = Store(":memory:")
        self.addCleanup(store.close)

        poller = pp.PulsePointPoller(agency_id="EMS1201", store=store,
                                     scope="test", group="vancouver-island")
        with mock.patch.object(poller.tracker, "update") as update:
            self._run_one_poll(poller, FakeResponse(
                b"", status=202, headers={"x-amzn-waf-action": "challenge"}))

        update.assert_not_called()

    def test_successful_poll_writes_no_health_row(self):
        """A working poller must leave the log alone."""
        store = Store(":memory:")
        self.addCleanup(store.close)

        poller = pp.PulsePointPoller(agency_id="EMS1201", store=store,
                                     scope="test", group="vancouver-island")
        payload = {"incidents": {"active": [], "recent": []}}
        body = json.dumps({"ct": "x", "iv": "y", "s": "z"}).encode()
        with mock.patch.object(pp, "decrypt_pulsepoint", return_value=payload):
            self._run_one_poll(poller, FakeResponse(body, status=200))

        self.assertEqual(store.events.recent(types=HEALTH_TYPES), [])

    def test_store_health_readout_names_the_failing_poller(self):
        """`store.py --health` is where an operator looks instead of at a
        stderr pipe that outlived its terminal."""
        store = Store(":memory:")
        self.addCleanup(store.close)

        poller = pp.PulsePointPoller(agency_id="EMS1201", store=store,
                                     scope="test", group="vancouver-island")
        self._run_one_poll(poller, FakeResponse(
            b"", status=202, headers={"x-amzn-waf-action": "challenge"}))

        out = io.StringIO()
        with mock.patch.object(sys, "stdout", out):
            print_health(store)
        text = out.getvalue()

        self.assertIn("pulsepoint EMS1201", text)
        self.assertIn("vancouver-island", text)
        self.assertIn("FAILING", text)
        self.assertIn("challenge", text)


if __name__ == "__main__":
    unittest.main()
