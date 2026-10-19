"""A feed that stops transcribing while the stream still plays online left
nothing behind but terminal output. These pin the durable trail: why a reader
ended, what ffmpeg said before it did, whisper failures, and dropped
transmissions all reach the event log, and `store.py --health` reads them back.

They also pin the hang underneath it: ffmpeg's stderr used to be read with a
blocking read-to-EOF on a process that was still running.
"""

import io
import queue
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import events
import outputs
import scanner_transcribe_gpu as stg
from store import HEALTH_TYPES, print_health, Store


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


class Completed:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


# ---------------------------------------------------------------------------
# StreamHealth
# ---------------------------------------------------------------------------
class StreamHealthTests(unittest.TestCase):

    def setUp(self):
        self.clock = FakeClock()
        self.emitted = []
        self.health = events.StreamHealth(
            "Cowichan Valley", "audio", group="vancouver-island",
            emit=self.emitted.append, rewarn_s=300, clock=self.clock)

    def test_failure_names_the_stream_and_component(self):
        event = self.health.failed("stalled", "no audio for 50s",
                                   context={"stderr": ["HTTP error 404"]})
        self.assertEqual(event["type"], events.STREAM_ERROR)
        self.assertEqual(event["stream"], "Cowichan Valley")
        self.assertEqual(event["group"], "vancouver-island")
        self.assertEqual(event["data"]["component"], "audio")
        self.assertEqual(event["data"]["kind"], "stalled")
        self.assertEqual(event["data"]["stderr"], ["HTTP error 404"])
        self.assertEqual(event["tier"], events.TIER_SENSITIVE)

    def test_restart_loop_is_throttled_then_recovers(self):
        self.health.failed("startup_failed", "no audio")
        for _ in range(5):
            self.clock.advance(30)
            self.assertIsNone(self.health.failed("startup_failed", "no audio"))
        self.clock.advance(150)
        rewarn = self.health.failed("startup_failed", "no audio")
        self.assertEqual(rewarn["data"]["consecutive"], 7)

        recovered = self.health.ok()
        self.assertEqual(recovered["type"], events.STREAM_RECOVERED)
        self.assertEqual(recovered["data"]["consecutive"], 7)
        self.assertEqual(len(self.emitted), 3)

    def test_unknown_component_is_rejected(self):
        with self.assertRaises(ValueError):
            events.StreamHealth("X", "gpu")

    def test_stream_health_is_not_delivered_to_outputs(self):
        self.assertIn(events.STREAM_ERROR, outputs.SUPPRESSED_TYPES)
        self.assertIn(events.STREAM_RECOVERED, outputs.SUPPRESSED_TYPES)

    def test_concurrent_failures_count_every_one(self):
        """The queue tracker is hit from reader threads, so the count must not
        lose updates."""
        health = events.StreamHealth("X", "queue", rewarn_s=1e9)

        def hammer():
            for _ in range(500):
                health.failed("queue_full", "dropped")

        threads = [threading.Thread(target=hammer) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(health.consecutive, 2000)


# ---------------------------------------------------------------------------
# ffmpeg stderr
# ---------------------------------------------------------------------------
class StderrDrainTests(unittest.TestCase):

    def _spawn(self, script):
        proc = subprocess.Popen([sys.executable, "-c", script],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.addCleanup(lambda: (proc.kill(), proc.wait(), proc.stdout.close()))
        proc.stderr_tail = stg.StderrTail()
        threading.Thread(target=stg._drain_stderr,
                         args=(proc, proc.stderr_tail), daemon=True).start()
        return proc

    def test_reading_a_live_process_does_not_block(self):
        """The watchdog read stderr to EOF on a still-running ffmpeg, holding
        managed_lock, and so never restarted anything again."""
        proc = self._spawn(
            "import sys, time\n"
            "sys.stderr.write('Connection reset by peer\\n'); sys.stderr.flush()\n"
            "time.sleep(60)\n")
        deadline = time.monotonic() + 5
        while not stg.ffmpeg_stderr_tail(proc) and time.monotonic() < deadline:
            time.sleep(0.02)

        t0 = time.monotonic()
        tail = stg.ffmpeg_stderr_tail(proc)
        with mock.patch.object(sys, "stdout", io.StringIO()):
            stg.log_ffmpeg_stderr("X", proc)
        self.assertLess(time.monotonic() - t0, 0.5)
        self.assertEqual(len(tail), 1)
        self.assertIn("Connection reset by peer", tail[0])

    def test_a_chatty_process_never_fills_the_pipe(self):
        """Unread, ~64 KB of warnings blocks ffmpeg on its stderr write, which
        looks exactly like a stalled stream."""
        proc = self._spawn(
            "import sys\n"
            "for i in range(20000): sys.stderr.write(f'warning {i}\\n')\n"
            "sys.stderr.flush()\n")
        self.assertEqual(proc.wait(timeout=10), 0)
        deadline = time.monotonic() + 5
        while (not stg.ffmpeg_stderr_tail(proc)
               or not stg.ffmpeg_stderr_tail(proc)[-1].endswith("warning 19999")) \
                and time.monotonic() < deadline:
            time.sleep(0.02)
        tail = stg.ffmpeg_stderr_tail(proc)
        self.assertEqual(len(tail), stg.FFMPEG_STDERR_TAIL)
        self.assertTrue(tail[-1].endswith("warning 19999"))

    def test_a_process_without_a_drain_has_no_tail(self):
        self.assertEqual(stg.ffmpeg_stderr_tail(object()), [])


# ---------------------------------------------------------------------------
# Reader exit reasons
# ---------------------------------------------------------------------------
class FakeProc:
    """An ffmpeg whose stdout is a pipe we control."""

    def __init__(self):
        import os
        r, self._w = os.pipe()
        self.stdout = open(r, "rb", buffering=0)
        self.returncode = None

    def poll(self):
        return self.returncode

    def close_output(self):
        import os
        if self._w is not None:
            os.close(self._w)
            self._w = None

    def close(self):
        self.close_output()
        self.stdout.close()


class ReaderExitTests(unittest.TestCase):

    def _run_reader(self, proc, work_queue=None, queue_health=None):
        self.addCleanup(proc.close)
        status = {}
        work_queue = work_queue or queue.Queue()
        with mock.patch.object(sys, "stdout", io.StringIO()):
            stg.stream_reader_thread("X", proc, work_queue, threading.Event(),
                                     status=status, queue_health=queue_health)
        return status

    def test_eof_is_recorded_as_the_exit_reason(self):
        proc = FakeProc()
        proc.close_output()
        status = self._run_reader(proc)
        self.assertEqual(status["exit"][0], "eof")

    def test_ffmpeg_death_is_recorded_with_its_exit_code(self):
        proc = FakeProc()
        proc.returncode = 8
        with mock.patch.object(stg, "READ_TIMEOUT_S", 0.01):
            status = self._run_reader(proc)
        self.assertEqual(status["exit"][0], "ffmpeg_exit")
        self.assertIn("8", status["exit"][1])

    def test_stall_is_recorded(self):
        proc = FakeProc()
        with mock.patch.object(stg, "READ_TIMEOUT_S", 0.01), \
                mock.patch.object(stg, "MAX_CONSECUTIVE_TIMEOUTS", 2):
            status = self._run_reader(proc)
        self.assertEqual(status["exit"][0], "stalled")

    def test_a_full_queue_records_a_dropped_transmission(self):
        emitted = []
        health = events.StreamHealth("X", "queue", emit=emitted.append)
        full = queue.Queue(maxsize=1)
        full.put(("X", b""))
        chunk = stg.np.zeros(stg.SAMPLE_RATE // 4, dtype=stg.np.float32)
        with mock.patch.object(full, "put", side_effect=queue.Full), \
                mock.patch.object(sys, "stdout", io.StringIO()):
            stg._emit_transmission("X", [chunk] * 8, full, 4, health)
        self.assertEqual(emitted[0]["data"]["kind"], "queue_full")

        ok_queue = queue.Queue()
        stg._emit_transmission("X", [chunk] * 8, ok_queue, 4, health)
        self.assertEqual(emitted[1]["type"], events.STREAM_RECOVERED)


# ---------------------------------------------------------------------------
# whisper failures say why
# ---------------------------------------------------------------------------
class WhisperErrorTests(unittest.TestCase):

    def _transcribe(self, result=None, raises=None):
        errors = []

        def fake_run(*args, **kwargs):
            if raises is not None:
                raise raises
            return result

        with mock.patch.object(stg.subprocess, "run", fake_run), \
                mock.patch.object(sys, "stdout", io.StringIO()):
            text = stg.transcribe_chunk("whisper-cli", "model.bin", b"RIFF",
                                        on_error=lambda k, d: errors.append((k, d)))
        return text, errors

    def test_nonzero_exit_reports_the_end_of_stderr(self):
        stderr = "\n".join(["loading model"] * 30 + ["ggml_metal: out of memory"])
        text, errors = self._transcribe(Completed(returncode=1, stderr=stderr))
        self.assertIsNone(text)
        self.assertEqual(errors[0][0], "whisper_exit")
        self.assertIn("out of memory", errors[0][1])

    def test_timeout_is_reported(self):
        text, errors = self._transcribe(
            raises=subprocess.TimeoutExpired("whisper-cli", 60))
        self.assertIsNone(text)
        self.assertEqual(errors[0][0], "whisper_timeout")

    def test_success_reports_nothing(self):
        text, errors = self._transcribe(Completed(stdout="engine three\n"))
        self.assertEqual(text, "engine three")
        self.assertEqual(errors, [])

    def test_unexpected_exception_is_a_failed_transcription(self):
        text, errors = self._transcribe(
            raises=BlockingIOError(35, "Resource temporarily unavailable"))
        self.assertIsNone(text)
        self.assertEqual(errors[0][0], "whisper_failed")
        self.assertIn("BlockingIOError", errors[0][1])

    def test_split_utf8_character_does_not_raise(self):
        # whisper.cpp prints tokens byte by byte, so half of a multi-byte
        # character reaches stdout on its own. Strict decoding raised out of
        # transcribe_chunk and the main loop's handler stopped the scanner.
        with tempfile.TemporaryDirectory() as tmp:
            fake = Path(tmp) / "whisper-cli"
            fake.write_text("#!/bin/sh\nprintf ' Engine \\342\\200 three\\n'\n")
            fake.chmod(0o755)
            errors = []
            with mock.patch.object(sys, "stdout", io.StringIO()):
                text = stg.transcribe_chunk(
                    str(fake), "model.bin", b"RIFF",
                    on_error=lambda k, d: errors.append((k, d)))
        self.assertEqual(text, "Engine  three")
        self.assertEqual(errors, [])


# ---------------------------------------------------------------------------
# store.py --health
# ---------------------------------------------------------------------------
class HealthReadoutTests(unittest.TestCase):

    def setUp(self):
        self.store = Store(":memory:")
        self.addCleanup(self.store.close)

    def _print(self, **kwargs):
        out = io.StringIO()
        with mock.patch.object(sys, "stdout", out):
            print_health(self.store, **kwargs)
        return out.getvalue()

    def test_a_failing_stream_shows_with_ffmpegs_last_words(self):
        health = events.StreamHealth("Cowichan Valley", "audio",
                                     group="vancouver-island",
                                     emit=self.store.events.append)
        health.failed("startup_failed", "no audio within 16s of connecting",
                      context={"stderr": ["Server returned 401 Unauthorized"]})
        self.assertIn(events.STREAM_ERROR, HEALTH_TYPES)

        text = self._print()
        self.assertIn("Cowichan Valley audio", text)
        self.assertIn("FAILING", text)
        self.assertIn("401 Unauthorized", text)

    def test_latest_is_per_component(self):
        audio = events.StreamHealth("A", "audio", emit=self.store.events.append)
        whisper = events.StreamHealth("A", "transcribe",
                                      emit=self.store.events.append)
        audio.failed("stale", "no audio for 61s")
        audio.ok()
        whisper.failed("whisper_timeout", "slow")

        text = self._print()
        self.assertRegex(text, r"A audio\s+ok")
        self.assertRegex(text, r"A transcribe\s+FAILING")

    def test_a_fatal_exit_shows_stopped_with_its_traceback(self):
        self.store.events.append(events.process_started(pid=42))
        self.store.events.append(events.process_exited(
            "fatal_error", detail="UnicodeDecodeError: invalid start byte",
            exit_code=1, uptime_s=90061, pid=42,
            traceback=["Traceback (most recent call last):",
                       '  File "scanner_transcribe_gpu.py", line 1, in main',
                       "UnicodeDecodeError: invalid start byte"]))
        self.assertIn(events.PROCESS_EXITED, HEALTH_TYPES)
        self.assertIn(events.PROCESS_EXITED, outputs.SUPPRESSED_TYPES)

        text = self._print()
        self.assertRegex(text, r"scanner process\s+STOPPED")
        self.assertIn("fatal_error: UnicodeDecodeError", text)
        self.assertIn("code 1, up 1d01h", text)
        self.assertIn("| UnicodeDecodeError: invalid start byte", text)

    def test_a_restart_shows_running(self):
        self.store.events.append(events.process_exited("signal", detail="SIGTERM"))
        self.store.events.append(events.process_started())

        self.assertRegex(self._print(), r"scanner process\s+running")

    def test_history_lists_every_event(self):
        audio = events.StreamHealth("A", "audio", emit=self.store.events.append)
        audio.failed("stale", "no audio for 61s")
        audio.ok()
        audio.failed("ffmpeg_exit", "ffmpeg exited with code 1")

        text = self._print(history=True)
        self.assertIn("no audio for 61s", text)
        self.assertIn("recovered", text)
        self.assertIn("exited with code 1", text)


if __name__ == "__main__":
    unittest.main()
