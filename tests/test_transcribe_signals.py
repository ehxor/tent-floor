"""The clip exists so a human can hear what whisper heard. That guarantee has
one hinge: whether the caller can tell "nothing was said" from "whisper
crashed".

Collapsing both to "" deletes the clip in exactly the case the clip exists for
— a wedged GPU, an OOM, a missing model, or a transmission that blows the 60s
budget. These tests pin the distinction at the source and at the decision.
"""

import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import scanner_transcribe_gpu as stg


class Completed:
    """Stands in for subprocess.CompletedProcess."""

    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class TranscribeSignals(unittest.TestCase):
    def transcribe(self, result=None, raises=None):
        def fake_run(*args, **kwargs):
            if raises is not None:
                raise raises
            return result
        with mock.patch.object(stg.subprocess, "run", fake_run):
            return stg.transcribe_chunk("whisper-cli", "model.bin", b"RIFF")

    def test_a_transcript_comes_back_as_text(self):
        out = self.transcribe(Completed(stdout="engine three responding\n"))
        self.assertEqual(out, "engine three responding")

    def test_nothing_transcribable_is_empty_string_not_none(self):
        """Silence and hallucination-filtered output mean the audio is not
        worth keeping. That is a real answer, not a failure."""
        stg.HALLUCINATIONS.add("Thank you.")
        try:
            out = self.transcribe(Completed(stdout="Thank you.\n"))
        finally:
            stg.HALLUCINATIONS.discard("Thank you.")
        self.assertEqual(out, "")
        self.assertIsNotNone(out)

    def test_empty_output_is_empty_string(self):
        self.assertEqual(self.transcribe(Completed(stdout="\n\n")), "")

    def test_a_nonzero_exit_is_none(self):
        """A wedged GPU, an OOM, a missing model file."""
        out = self.transcribe(Completed(returncode=1, stderr="CUDA error"))
        self.assertIsNone(out)

    def test_a_timeout_is_none(self):
        out = self.transcribe(raises=subprocess.TimeoutExpired("whisper-cli", 60))
        self.assertIsNone(out)


class ClipDecision(unittest.TestCase):
    """The predicate the consumer loop uses after transcription.

    The loop itself needs a GPU and live streams, so the rule is extracted into
    should_delete_clip() and asserted here — testing the real function rather
    than a restatement of it.
    """

    def test_a_whisper_failure_keeps_the_clip(self):
        self.assertFalse(
            stg.should_delete_clip(None, {"id": "x", "reused": False}),
            "this is precisely the audio worth keeping")

    def test_silence_deletes_the_clip(self):
        self.assertTrue(
            stg.should_delete_clip("", {"id": "x", "reused": False}))

    def test_a_successful_transcript_keeps_the_clip(self):
        self.assertFalse(
            stg.should_delete_clip("engine three", {"id": "x", "reused": False}))

    def test_a_reused_clip_is_never_deleted(self):
        self.assertFalse(
            stg.should_delete_clip("", {"id": "x", "reused": True}),
            "an earlier transcript still references it")

    def test_no_clip_means_nothing_to_delete(self):
        self.assertFalse(stg.should_delete_clip("", None))
        self.assertFalse(stg.should_delete_clip(None, None))

    def test_a_clip_missing_the_flag_is_treated_as_created(self):
        """Defensive: a record from get() has no `reused` key."""
        self.assertTrue(stg.should_delete_clip("", {"id": "x"}))


if __name__ == "__main__":
    unittest.main()
