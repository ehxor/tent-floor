"""Clips are the largest and most sensitive thing the system stores, so the
properties that matter are about lifecycle: an encoder failure must not take
the transcript with it, a clip nobody will reference must not sit on disk for a
week, and nothing may grow without bound.

ffmpeg is not required here — ClipStore takes an injectable encoder, and the
real invocation is asserted as a command line rather than executed.
"""

import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import clips
import events
from store import Store


def fake_encoder(payload=b"OggS-fake-opus"):
    """Stands in for ffmpeg. Writes a deterministic file."""
    def encode(pcm_bytes, destination):
        Path(destination).write_bytes(payload + bytes([len(pcm_bytes) % 251]))
    return encode


class ClipStoreBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(":memory:")
        self.clips = clips.ClipStore(
            self.store, directory=Path(self.tmp.name) / "clips",
            retention_days=7, encoder=fake_encoder())

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def pcm(self, seed=b"\x01\x02"):
        return seed * 8000


class Writing(ClipStoreBase):
    def test_a_clip_is_written_and_recorded(self):
        record = self.clips.write(self.pcm(), "vancouver-island", "Mid Island", 4.2)
        self.assertIsNotNone(record)
        self.assertTrue(Path(record["path"]).exists())
        self.assertEqual(record["codec"], "opus")
        self.assertEqual(record["duration_s"], 4.2)
        self.assertGreater(record["bytes"], 0)
        self.assertEqual(self.clips.get(record["id"]), record)

    def test_identical_audio_is_stored_once(self):
        first = self.clips.write(self.pcm(), "g", "s", 1.0)
        second = self.clips.write(self.pcm(), "g", "s", 1.0)
        self.assertEqual(first["id"], second["id"],
                         "content addressing means a retry cannot duplicate")
        count, _ = self.clips.usage()
        self.assertEqual(count, 1)

    def test_different_audio_gets_different_ids(self):
        a = self.clips.write(self.pcm(b"\x01\x02"), "g", "s", 1.0)
        b = self.clips.write(self.pcm(b"\x03\x04"), "g", "s", 1.0)
        self.assertNotEqual(a["id"], b["id"])

    def test_an_encoder_failure_costs_only_the_audio(self):
        def broken(pcm_bytes, destination):
            raise RuntimeError("ffmpeg exited 1: Unknown encoder 'libopus'")
        self.clips._encoder = broken
        record = self.clips.write(self.pcm(), "g", "s", 1.0)
        self.assertIsNone(record, "a failed encode returns None rather than raising")
        count, _ = self.clips.usage()
        self.assertEqual(count, 0)

    def test_a_failed_encode_leaves_no_partial_file(self):
        def half_written(pcm_bytes, destination):
            Path(destination).write_bytes(b"partial")
            raise RuntimeError("died midway")
        self.clips._encoder = half_written
        self.clips.write(self.pcm(), "g", "s", 1.0)
        leftovers = list(Path(self.clips.dir).rglob("*.opus"))
        self.assertEqual(leftovers, [])

    def test_clips_fan_out_across_directories(self):
        record = self.clips.write(self.pcm(), "g", "s", 1.0)
        # A week of a busy scanner is a lot of files for one directory.
        self.assertEqual(Path(record["path"]).parent.name, record["id"][:2])


class Deleting(ClipStoreBase):
    def test_delete_removes_the_file_and_the_row(self):
        record = self.clips.write(self.pcm(), "g", "s", 1.0)
        path = Path(record["path"])
        self.clips.delete(record["id"])
        self.assertFalse(path.exists())
        self.assertIsNone(self.clips.get(record["id"]))

    def test_deleting_an_unknown_clip_is_harmless(self):
        self.clips.delete("nope")

    def test_delete_survives_a_missing_file(self):
        record = self.clips.write(self.pcm(), "g", "s", 1.0)
        Path(record["path"]).unlink()
        self.clips.delete(record["id"])
        self.assertIsNone(self.clips.get(record["id"]))


class Retention(ClipStoreBase):
    def test_expired_clips_are_removed(self):
        now = time.time()
        old = self.clips.write(self.pcm(b"\x01"), "g", "s", 1.0, now=now - 8 * 86400)
        fresh = self.clips.write(self.pcm(b"\x02"), "g", "s", 1.0, now=now)
        expired, orphans = self.clips.sweep(now=now)
        self.assertEqual(expired, 1)
        self.assertFalse(Path(old["path"]).exists())
        self.assertTrue(Path(fresh["path"]).exists())
        self.assertIsNone(self.clips.get(old["id"]))

    def test_orphaned_files_are_collected(self):
        """A crash between encoding and recording leaves a file with no row.
        Without this they accumulate forever, which is the unbounded growth
        retention exists to prevent."""
        orphan = Path(self.clips.dir) / "ab" / "abcdef.opus"
        orphan.parent.mkdir(parents=True, exist_ok=True)
        orphan.write_bytes(b"stranded")
        old = time.time() - 30 * 86400
        import os
        os.utime(orphan, (old, old))

        expired, orphans = self.clips.sweep()
        self.assertEqual(orphans, 1)
        self.assertFalse(orphan.exists())

    def test_a_recent_orphan_is_left_alone(self):
        """It may belong to a write still in flight."""
        orphan = Path(self.clips.dir) / "cd" / "cdefgh.opus"
        orphan.parent.mkdir(parents=True, exist_ok=True)
        orphan.write_bytes(b"in flight")
        expired, orphans = self.clips.sweep()
        self.assertEqual(orphans, 0)
        self.assertTrue(orphan.exists())

    def test_sweeping_does_not_touch_live_clips(self):
        record = self.clips.write(self.pcm(), "g", "s", 1.0)
        self.clips.sweep()
        self.assertTrue(Path(record["path"]).exists())
        self.assertIsNotNone(self.clips.get(record["id"]))

    def test_usage_reports_what_is_held(self):
        self.clips.write(self.pcm(b"\x01"), "g", "s", 1.0)
        self.clips.write(self.pcm(b"\x02"), "g", "s", 1.0)
        count, total = self.clips.usage()
        self.assertEqual(count, 2)
        self.assertGreater(total, 0)


class Envelope(ClipStoreBase):
    def test_a_transcript_can_carry_an_audio_reference(self):
        record = self.clips.write(self.pcm(), "vancouver-island", "Mid Island", 4.2)
        event = events.transcript("vancouver-island", "Mid Island",
                                  "engine three responding", 4.2,
                                  audio=events.audio_ref(record))
        audio = event["data"]["audio"]
        self.assertEqual(audio["id"], record["id"])
        self.assertEqual(audio["codec"], "opus")
        self.assertEqual(audio["duration_s"], 4.2)
        self.assertIsNone(audio["url"], "no serving layer yet")
        self.assertTrue(audio["expires_at"].endswith("Z"))

    def test_the_local_path_never_leaves_the_host(self):
        record = self.clips.write(self.pcm(), "g", "s", 1.0)
        audio = events.audio_ref(record)
        self.assertNotIn("path", audio)
        self.assertNotIn(self.tmp.name, repr(audio),
                         "the filesystem layout must not reach subscribers")

    def test_a_transcript_without_audio_omits_the_field(self):
        event = events.transcript("g", "s", "no clip", 1.0)
        self.assertNotIn("audio", event["data"])

    def test_the_render_is_unchanged_by_audio(self):
        record = self.clips.write(self.pcm(), "g", "Mid Island", 4.2)
        with_audio = events.transcript("g", "Mid Island", "hello", 4.2,
                                       audio=events.audio_ref(record))
        without = events.transcript("g", "Mid Island", "hello", 4.2)
        self.assertEqual(with_audio["render"], without["render"],
                         "adding audio must not change what Discord shows")


class EncoderCommand(unittest.TestCase):
    """The ffmpeg invocation cannot be run here, so it is asserted instead."""

    def test_the_command_describes_the_captured_format(self):
        command = clips.encode_command("ffmpeg", 16000, "16k", "/tmp/x.opus")
        joined = " ".join(command)
        self.assertIn("-f s16le", joined, "input is raw PCM, as captured")
        self.assertIn("-ar 16000", joined)
        self.assertIn("-ac 1", joined)
        self.assertIn("-c:a libopus", joined)
        self.assertIn("-b:a 16k", joined)
        self.assertIn("-application voip", joined, "speech, not music")
        self.assertEqual(command[-1], "/tmp/x.opus")

    def test_the_capture_rate_is_one_opus_supports(self):
        self.assertIn(16000, clips.SUPPORTED_RATES,
                      "otherwise every clip would need resampling")

    def test_probe_reports_a_missing_ffmpeg_rather_than_raising(self):
        ok, detail = clips.probe_encoder("definitely-not-a-real-binary")
        self.assertFalse(ok)
        self.assertIn("not found", detail)


class RealEncoder(unittest.TestCase):
    """The one thing a fake encoder cannot prove: that the ffmpeg invocation
    actually produces a playable Opus file, at roughly the size the retention
    figures assume.

    Skipped where ffmpeg is absent, so this only really runs in CI — which is
    the point of installing ffmpeg there.
    """

    @classmethod
    def setUpClass(cls):
        ok, detail = clips.probe_encoder()
        if not ok:
            raise unittest.SkipTest(f"no Opus encoder: {detail}")

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(":memory:")
        self.clips = clips.ClipStore(
            self.store, directory=Path(self.tmp.name) / "clips", retention_days=7)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def speech_like_pcm(self, seconds):
        """Not speech, but the right shape: 16 kHz signed 16-bit mono."""
        import math
        import struct
        frames = []
        for n in range(int(16000 * seconds)):
            # A couple of formants' worth of tone so the encoder has structure
            # to work with rather than pure noise, which Opus handles badly and
            # would distort the size check.
            value = (0.4 * math.sin(2 * math.pi * 220 * n / 16000)
                     + 0.2 * math.sin(2 * math.pi * 880 * n / 16000))
            frames.append(struct.pack("<h", int(value * 20000)))
        return b"".join(frames)

    def test_a_real_clip_is_a_playable_opus_file(self):
        pcm = self.speech_like_pcm(3.0)
        record = self.clips.write(pcm, "vancouver-island", "Mid Island", 3.0)
        self.assertIsNotNone(record, "the real encoder should have succeeded")
        path = Path(record["path"])
        self.assertTrue(path.exists())
        # Opus ships in an Ogg container; every such file starts with this.
        self.assertEqual(path.read_bytes()[:4], b"OggS")
        self.assertEqual(record["bytes"], path.stat().st_size)
        self.assertGreater(record["bytes"], 0)

    def test_the_size_matches_the_retention_arithmetic(self):
        """The README and docs size a disk off ~2 kB per second of speech at
        16 kbps. If that is wrong, the retention guidance is wrong."""
        seconds = 5.0
        pcm = self.speech_like_pcm(seconds)
        record = self.clips.write(pcm, "g", "s", seconds)
        expected = 16_000 / 8 * seconds          # 16 kbps -> bytes
        ratio = record["bytes"] / expected
        self.assertGreater(ratio, 0.4,
                           f"{record['bytes']}B is far below the 16 kbps budget")
        self.assertLess(ratio, 2.5,
                        f"{record['bytes']}B is far above the 16 kbps budget")

    def test_a_real_clip_is_much_smaller_than_the_pcm(self):
        """The 16x claim in the docs, checked rather than asserted in prose."""
        seconds = 5.0
        pcm = self.speech_like_pcm(seconds)
        record = self.clips.write(pcm, "g", "s", seconds)
        self.assertGreater(len(pcm) / record["bytes"], 8,
                           "Opus should be at least 8x smaller than raw PCM")

    def test_a_real_encode_of_garbage_still_fails_cleanly(self):
        """An odd number of bytes is not a whole s16le frame."""
        record = self.clips.write(b"\x01", "g", "s", 0.0)
        # ffmpeg may accept or reject this; either way nothing may be left
        # half-recorded, and it must not raise.
        if record is None:
            self.assertEqual(list(Path(self.clips.dir).rglob("*.opus")), [])
        else:
            self.assertTrue(Path(record["path"]).exists())


if __name__ == "__main__":
    unittest.main()
