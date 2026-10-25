"""The Whistle evaluation is only worth anything if its score means what it says,
so most of this is about the metric: an insertion must not cascade the way it
would under Hamming distance, formatting differences must not count, and the
corpus figure must weight clips by length.

The end-to-end test stands a fake `needle` module in for Whistle, so it runs
without the engine or its weights, but decodes a real Opus clip with ffmpeg
when ffmpeg can encode one.
"""

import io
import json
import sys
import tempfile
import types
import unittest
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

import clips
import eval_whistle as ev
import events
from store import Store


class NormalizeTests(unittest.TestCase):
    def test_case_punctuation_and_number_words(self):
        self.assertEqual(ev.basic_normalize("Engine Three, copy."), "engine 3 copy")
        self.assertEqual(ev.basic_normalize("Engine 3 copy"), "engine 3 copy")

    def test_keeps_radio_codes_together(self):
        self.assertEqual(ev.basic_normalize("10-4, show us 10-8"), "104 show us 108")

    def test_strips_accents_and_apostrophes(self):
        self.assertEqual(ev.basic_normalize("We're at the Café"), "were at the cafe")


class ScoreTests(unittest.TestCase):
    def test_identical_after_normalisation(self):
        s = ev.score("Engine 3, responding.", "engine three responding")
        self.assertEqual(s["wer"], 0.0)
        self.assertEqual(s["cer"], 0.0)

    def test_one_insertion_is_one_edit(self):
        # The case Hamming distance gets wrong: everything after the inserted
        # word shifts by one, but only one word is actually different.
        s = ev.score("rescue one en route to bowen road",
                     "rescue one is en route to bowen road")
        self.assertEqual((s["sub"], s["del"], s["ins"]), (0, 0, 1))
        self.assertAlmostEqual(s["wer"], 1 / 7)

    def test_substitution_and_deletion(self):
        s = ev.score("structure fire confirmed at lantzville",
                     "structure fire at lanceville")
        self.assertEqual((s["sub"], s["del"], s["ins"]), (1, 1, 0))
        self.assertAlmostEqual(s["wer"], 2 / 5)
        # The misheard place name is a near miss at the character level.
        self.assertLess(s["cer"], s["wer"])

    def test_empty_hypothesis_is_all_deletions(self):
        s = ev.score("engine seven clear", "")
        self.assertEqual(s["del"], 3)
        self.assertEqual(s["wer"], 1.0)

    def test_empty_reference_does_not_divide_by_zero(self):
        self.assertEqual(ev.score("", "")["wer"], 0.0)
        self.assertEqual(ev.score("", "hello")["wer"], 1.0)

    def test_wer_can_exceed_one(self):
        self.assertGreater(ev.score("copy", "copy that dispatch thank you")["wer"], 1.0)

    def test_render_diff(self):
        ops = ev.score("unit 37 acknowledging", "unit 47 acknowledging now")["ops"]
        self.assertEqual(ev.render_diff(ops), "unit [37->47] acknowledging [+now]")


class SummaryTests(unittest.TestCase):
    def row(self, ref, hyp, duration=2.0, whistle_s=0.1, latency=1.0):
        return {"score": ev.score(ref, hyp), "duration_s": duration,
                "whistle_s": whistle_s, "whisper_latency_s": latency}

    def test_corpus_wer_weights_by_length(self):
        rows = [self.row("copy", "coffee"),                     # 1 / 1
                self.row("a b c d e f g h i", "a b c d e f g h i")]  # 0 / 9
        summary = ev.summarize(rows)
        self.assertAlmostEqual(summary["corpus_wer"], 1 / 10)
        self.assertEqual(summary["exact"], 1)

    def test_failed_rows_are_counted_not_scored(self):
        rows = [self.row("copy", "copy"), {"score": None, "duration_s": 1.0,
                                           "whistle_s": None, "whisper_latency_s": None}]
        summary = ev.summarize(rows)
        self.assertEqual((summary["clips"], summary["failed"]), (1, 1))


class CleanupTests(unittest.TestCase):
    def test_matches_scanner_filtering(self):
        self.assertEqual(ev.clean_like_scanner("[static] engine 3 (inaudible)", set()), "engine 3")
        self.assertEqual(ev.clean_like_scanner("Thank you.", {"Thank you."}), "")


class FakeWhistle:
    """Stands in for needle.Whistle. Returns a canned transcript per call, and
    records what it was handed so the decode path can be checked."""

    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []

    def transcribe(self, audio, language=None, keywords=None, word_timestamps=False):
        self.calls.append({"bytes": len(audio), "language": language, "keywords": keywords})
        return {"text": self.replies.pop(0), "language": "en", "ttft_ms": 1.0, "decode_tps": 1.0}


@unittest.skipUnless(clips.probe_encoder()[0], "ffmpeg with libopus not available")
class EndToEndTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.db = root / "t.db"
        self.clips_dir = root / "clips"
        store = Store(str(self.db))
        clip_store = clips.ClipStore(store, directory=self.clips_dir)
        texts = ["Engine three responding to Bowen Road",
                 "Structure fire confirmed",
                 "Rescue one en route code 3"]
        for i, text in enumerate(texts):
            # One second of a quiet tone, different per clip so they don't dedupe.
            pcm = bytes((((n * (i + 3)) % 64) - 32) & 0xFF if n % 2 else 0
                        for n in range(32000))
            clip = clip_store.write(pcm, "vancouver-island", "Mid Island", 1.0)
            store.events.append(events.transcript("vancouver-island", "Mid Island", text, 1.0,
                                                  model="large-v3", latency_s=2.0,
                                                  audio=events.audio_ref(clip)))
        # A transcript with no clip is skipped, not counted as a failure.
        store.events.append(events.transcript("vancouver-island", "Mid Island", "no audio", 1.0))
        store.close()

    def tearDown(self):
        sys.modules.pop("needle", None)
        self.tmp.cleanup()

    def install(self, fake):
        sys.modules["needle"] = types.SimpleNamespace(Whistle=lambda weights=None: fake)

    def test_scores_the_last_n(self):
        fake = FakeWhistle(["structure fire confirmed.", "rescue 1 en route code three"])
        self.install(fake)
        out = Path(self.tmp.name) / "out.json"
        with redirect_stdout(io.StringIO()):
            ev.main(["--db", str(self.db), "--clips", str(self.clips_dir), "-n", "2",
                     "--normalizer", "basic", "--json", str(out),
                     "--jargon", str(Path(__file__).resolve().parent.parent / "jargon.txt")])
        report = json.loads(out.read_text())
        self.assertEqual(report["summary"]["clips"], 2)
        self.assertEqual(report["summary"]["corpus_wer"], 0.0)
        # Oldest of the two first.
        self.assertEqual([r["whisper_text"] for r in report["rows"]],
                         ["Structure fire confirmed", "Rescue one en route code 3"])
        # One second of 16 kHz float32.
        self.assertAlmostEqual(fake.calls[0]["bytes"], 16000 * 4, delta=4 * 400)
        self.assertEqual(fake.calls[0]["language"], "en")
        self.assertIn("Nanaimo", fake.calls[0]["keywords"])

    def test_never_writes_to_the_store(self):
        self.install(FakeWhistle(["x"] * 3))
        before = self.db.read_bytes()
        with redirect_stdout(io.StringIO()):
            ev.main(["--db", str(self.db), "--clips", str(self.clips_dir), "-n", "3"])
        self.assertEqual(self.db.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
