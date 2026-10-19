"""/healthz has to tell a monitor the thing nobody noticed on 2026-10-18: the
scanner process was gone. These pin each way it can say "not working" — no
heartbeat, a stale one (with the exit reason when there is one), a stuck loop,
an open failure run, a stream gone quiet — and that it stays quiet otherwise.
"""

import json
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import events
import healthcheck
from store import Store


class HealthcheckBase(unittest.TestCase):

    def setUp(self):
        self.store = Store(":memory:")
        self.addCleanup(self.store.close)
        self.now = time.time()
        self.started = self.now - 3600

    def beat(self, at=None, loop_at=None, streams=None, started=None):
        at = self.now if at is None else at
        started = self.started if started is None else started
        self.store.kv.set("health", "heartbeat", {
            "pid": 42,
            "started_at": _rfc3339(started),
            "interval_s": 30,
            "loop_at": at if loop_at is None else loop_at,
            "queue_depth": 0,
            "streams": streams or [],
        }, now=at)

    def append(self, event, at):
        self.store.events.append(event, now=at)

    def transcript(self, stream, at):
        self.append(events.transcript("g", stream, "engine three", 2.0), at)

    def evaluate(self, **kwargs):
        return healthcheck.evaluate(self.store.conn, now=self.now, **kwargs)


def _rfc3339(epoch):
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(epoch))


class Process(HealthcheckBase):

    def test_no_heartbeat_is_failing(self):
        report = self.evaluate()
        self.assertEqual(report["status"], "failing")
        self.assertIn("no scanner heartbeat", report["problems"][0])

    def test_a_fresh_heartbeat_is_ok(self):
        self.beat()
        report = self.evaluate()
        self.assertEqual(report["status"], "ok", report["problems"])
        self.assertEqual(report["scanner"]["pid"], 42)

    def test_a_stale_heartbeat_gives_the_recorded_exit_reason(self):
        self.beat(at=self.now - 600)
        self.append(events.process_exited(
            "fatal_error", detail="UnicodeDecodeError: invalid start byte",
            exit_code=1), self.now - 600)
        problems = self.evaluate()["problems"]
        self.assertEqual(len(problems), 1)
        self.assertIn("10m00s old", problems[0])
        self.assertIn("fatal_error: UnicodeDecodeError", problems[0])

    def test_a_stale_heartbeat_with_no_exit_was_killed(self):
        # The Oct 18 shape: nothing recorded, the process simply stopped.
        self.beat(at=self.now - 600)
        self.assertIn("killed or hung", self.evaluate()["problems"][0])

    def test_an_exit_from_an_earlier_run_is_not_the_explanation(self):
        self.append(events.process_exited("signal", detail="SIGINT"),
                    self.started - 60)
        self.beat(at=self.now - 600)
        self.assertIn("killed or hung", self.evaluate()["problems"][0])

    def test_a_stuck_loop_is_failing_while_the_heartbeat_is_fresh(self):
        self.beat(loop_at=self.now - 300)
        problems = self.evaluate()["problems"]
        self.assertEqual(problems, ["transcription loop has not run for 5m00s"])


class Components(HealthcheckBase):

    def setUp(self):
        super().setUp()
        self.beat()
        self.health = events.StreamHealth(
            "Cowichan Valley", "audio", group="vancouver-island",
            emit=lambda e: self.append(e, time.time()))

    def test_an_open_failure_past_the_grace_period_is_failing(self):
        self.health.failed("stalled", "no audio for 50s")
        report = self.evaluate(grace_s=0)
        self.assertEqual(report["status"], "failing")
        self.assertIn("Cowichan Valley audio [vancouver-island]: no audio for 50s",
                      report["problems"][0])

    def test_a_new_failure_is_pending_not_failing(self):
        self.health.failed("stalled", "no audio for 50s")
        report = self.evaluate(grace_s=120)
        self.assertEqual(report["status"], "ok")
        self.assertEqual(len(report["pending"]), 1)

    def test_a_recovered_failure_is_ok(self):
        self.health.failed("stalled", "no audio for 50s")
        self.health.ok()
        report = self.evaluate(grace_s=0)
        self.assertEqual(report["status"], "ok", report["problems"])
        self.assertEqual(report["components"][0]["state"], "ok")

    def test_a_failure_from_before_the_restart_does_not_count(self):
        # Health runs do not survive a restart, so this one can never clear.
        stale = events.StreamHealth("Kelowna", "audio", group="interior",
                                    emit=lambda e: self.append(e, self.started - 60))
        stale.failed("eof", "ffmpeg closed its output")
        report = self.evaluate(grace_s=0)
        self.assertEqual(report["status"], "ok", report["problems"])
        self.assertEqual(report["components"], [])


class Silence(HealthcheckBase):

    STREAMS = [{"name": "Kamloops", "group": "interior", "silence_alert_s": 1800}]

    def test_a_recent_transcript_is_ok(self):
        self.beat(streams=self.STREAMS)
        self.transcript("Kamloops", self.now - 600)
        report = self.evaluate()
        self.assertEqual(report["status"], "ok", report["problems"])
        self.assertEqual(report["streams"][0]["last_transcript_age_s"], 600.0)

    def test_silence_past_the_limit_is_failing(self):
        self.beat(streams=self.STREAMS)
        self.transcript("Kamloops", self.now - 2400)
        problems = self.evaluate()["problems"]
        self.assertEqual(problems, ["Kamloops [interior]: no transcript for "
                                    "40m00s (alert after 30m00s)"])

    def test_silence_older_than_the_lookback_is_still_failing(self):
        self.beat(streams=self.STREAMS, started=self.now - 86400)
        self.transcript("Kamloops", self.now - 86000)
        problems = self.evaluate()["problems"]
        self.assertEqual(len(problems), 1)
        self.assertIn("no transcript for at least 1h00m", problems[0])

    def test_silence_is_counted_from_the_restart(self):
        self.beat(streams=self.STREAMS, started=self.now - 600)
        self.transcript("Kamloops", self.now - 7200)
        self.assertEqual(self.evaluate()["status"], "ok")

    def test_a_dead_scanner_reports_itself_not_every_stream(self):
        self.beat(streams=self.STREAMS, at=self.now - 7200)
        self.transcript("Kamloops", self.now - 7200)
        problems = self.evaluate()["problems"]
        self.assertEqual(len(problems), 1)
        self.assertIn("heartbeat", problems[0])

    def test_no_transcript_text_reaches_the_report(self):
        self.beat(streams=self.STREAMS)
        self.transcript("Kamloops", self.now - 60)
        self.assertNotIn("engine three", json.dumps(self.evaluate()))


if __name__ == "__main__":
    unittest.main()
