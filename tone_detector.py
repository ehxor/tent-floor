"""
tone_detector.py — Detect two-tone sequential pager tones in audio chunks.

Identifies Motorola Quick Call II / Plectron-style two-tone sequential paging.
Each page consists of Tone A followed by Tone B, each a pure sine wave at a
specific frequency from the standard tone set (~300-2800 Hz).

Usage:
    Integrated into scanner_transcribe.py, or standalone for testing:
        python tone_detector.py test.wav

The lookup table (tone_lookup.json) maps "freqA/freqB" pairs to unit names.
Unknown tones are logged so you can build the table over time.
"""

import json
import struct
import sys
import wave
import numpy as np
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Standard two-tone paging frequencies (Hz)
# These are the Motorola Quick Call II / Plectron standard tone set.
# The detector snaps detected frequencies to the nearest standard tone.
# ---------------------------------------------------------------------------
STANDARD_TONES = [
    330.5, 349.0, 368.5, 389.0, 410.8, 433.7, 457.9, 483.5, 510.5,
    539.0, 569.1, 600.9, 634.1, 669.3, 706.4, 745.5, 786.7, 830.0,
    875.6, 923.8, 975.0, 1029.0, 1085.9, 1146.5, 1210.0, 1277.5,
    1348.5, 1423.4, 1502.3, 1585.5, 1673.5, 1766.5, 1864.5, 1968.0,
    2076.5, 2190.5, 2311.5, 2439.5, 2575.5, 2719.5, 2872.0,
]

# ---------------------------------------------------------------------------
# Detection parameters — tweak these based on your audio
# ---------------------------------------------------------------------------
MIN_TONE_DURATION_S = 0.5     # Minimum duration of each tone (seconds)
MAX_TONE_DURATION_S = 4.0     # Maximum duration of each tone
TONE_GAP_MAX_S = 0.2          # Max silence between tone A and tone B
TONE_POWER_THRESHOLD = 0.005  # Minimum signal power to consider (filters noise)
FREQ_SNAP_TOLERANCE_HZ = 15   # Max deviation from standard tone to snap
GOERTZEL_WINDOW_MS = 100      # Analysis window size in ms
GOERTZEL_HOP_MS = 50          # Hop between analysis windows in ms
TONE_STABILITY_WINDOWS = 3    # Consecutive windows with same freq = confirmed tone
SAMPLE_RATE = 16000

# ---------------------------------------------------------------------------
# Lookup table
# ---------------------------------------------------------------------------
DEFAULT_LOOKUP_PATH = Path(__file__).parent / "tone_lookup.json"
DEFAULT_UNKNOWN_LOG_PATH = Path(__file__).parent / "unknown_tones.log"


def load_lookup_table(path=None):
    """Load tone pair -> unit name lookup table from JSON.

    Format: { "1050.0/1450.0": "Nanaimo Engine 3", ... }
    """
    path = Path(path) if path else DEFAULT_LOOKUP_PATH
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def save_lookup_table(table, path=None):
    """Save lookup table back to JSON."""
    path = Path(path) if path else DEFAULT_LOOKUP_PATH
    with open(path, "w") as f:
        json.dump(table, f, indent=2, sort_keys=True)


def snap_to_standard(freq_hz):
    """Snap a detected frequency to the nearest standard tone.

    Returns (standard_freq, deviation) or (None, None) if too far off.
    """
    if freq_hz is None:
        return None, None

    closest = min(STANDARD_TONES, key=lambda t: abs(t - freq_hz))
    deviation = abs(closest - freq_hz)

    if deviation <= FREQ_SNAP_TOLERANCE_HZ:
        return closest, deviation
    return None, None


def goertzel_magnitude(samples, target_freq, sample_rate):
    """Compute Goertzel magnitude for a specific frequency.

    More efficient than FFT when you only need a few frequencies.
    Returns the magnitude (power) at the target frequency.
    """
    n = len(samples)
    k = int(0.5 + n * target_freq / sample_rate)
    w = 2.0 * np.pi * k / n
    coeff = 2.0 * np.cos(w)

    s0 = 0.0
    s1 = 0.0
    s2 = 0.0

    for sample in samples:
        s0 = sample + coeff * s1 - s2
        s2 = s1
        s1 = s0

    magnitude = s1 * s1 + s2 * s2 - coeff * s1 * s2
    return magnitude / (n * n)  # Normalize


def detect_dominant_tone(samples, sample_rate=SAMPLE_RATE):
    """Find the dominant standard tone frequency in a short audio window.

    Returns (frequency, magnitude) or (None, 0) if no clear tone.
    """
    # Check if there's enough signal power
    power = np.mean(samples ** 2)
    if power < TONE_POWER_THRESHOLD:
        return None, 0

    # Test each standard tone frequency using Goertzel
    best_freq = None
    best_mag = 0

    for tone_freq in STANDARD_TONES:
        mag = goertzel_magnitude(samples, tone_freq, sample_rate)
        if mag > best_mag:
            best_mag = mag
            best_freq = tone_freq

    # Check that the dominant tone is significantly stronger than background
    # Use a simple SNR check: dominant should be > 3x average of others
    if best_freq is not None:
        other_mags = []
        for tone_freq in STANDARD_TONES:
            if tone_freq != best_freq:
                other_mags.append(goertzel_magnitude(samples, tone_freq, sample_rate))
        avg_other = np.mean(other_mags) if other_mags else 0

        if avg_other > 0 and best_mag / avg_other < 3.0:
            return None, 0  # Not a clean tone

    return best_freq, best_mag


class TwoToneDetector:
    """Stateful detector for two-tone sequential paging across audio chunks.

    Call feed_audio() with each chunk. It tracks tone state across chunk
    boundaries so a tone pair split across two chunks is still detected.
    """

    def __init__(self, lookup_path=None, unknown_log_path=None):
        self.lookup = load_lookup_table(lookup_path)
        self.lookup_path = lookup_path
        self.unknown_log_path = Path(unknown_log_path) if unknown_log_path else DEFAULT_UNKNOWN_LOG_PATH

        # State machine
        self.state = "idle"         # idle -> tone_a -> gap -> tone_b -> idle
        self.tone_a_freq = None
        self.tone_a_start = 0
        self.tone_a_duration = 0
        self.tone_b_freq = None
        self.gap_duration = 0
        self.stable_freq = None
        self.stable_count = 0
        self.total_samples_fed = 0

    def feed_audio(self, audio_np, sample_rate=SAMPLE_RATE):
        """Process an audio chunk and return list of detected tone events.

        Each event is a dict:
            {
                "tone_a": 1050.0,
                "tone_b": 1450.0,
                "key": "1050.0/1450.0",
                "unit": "Nanaimo Engine 3" or None,
                "timestamp": "14:32:05"
            }
        """
        events = []
        window_samples = int(sample_rate * GOERTZEL_WINDOW_MS / 1000)
        hop_samples = int(sample_rate * GOERTZEL_HOP_MS / 1000)
        num_windows = max(1, (len(audio_np) - window_samples) // hop_samples + 1)

        for i in range(num_windows):
            start = i * hop_samples
            end = start + window_samples
            if end > len(audio_np):
                break

            window = audio_np[start:end]
            freq, mag = detect_dominant_tone(window, sample_rate)

            event = self._update_state(freq, GOERTZEL_HOP_MS / 1000.0)
            if event:
                events.append(event)

        self.total_samples_fed += len(audio_np)
        return events

    def _update_state(self, detected_freq, dt_seconds):
        """State machine for tracking tone A -> gap -> tone B sequence."""

        if self.state == "idle":
            if detected_freq is not None:
                self.stable_freq = detected_freq
                self.stable_count = 1
                self.state = "maybe_tone_a"
            return None

        elif self.state == "maybe_tone_a":
            if detected_freq == self.stable_freq:
                self.stable_count += 1
                if self.stable_count >= TONE_STABILITY_WINDOWS:
                    self.tone_a_freq = self.stable_freq
                    self.tone_a_duration = self.stable_count * dt_seconds
                    self.state = "tone_a"
            else:
                # Unstable — reset
                self.state = "idle"
                self.stable_freq = None
                self.stable_count = 0
            return None

        elif self.state == "tone_a":
            if detected_freq == self.tone_a_freq:
                self.tone_a_duration += dt_seconds
                if self.tone_a_duration > MAX_TONE_DURATION_S:
                    # Too long to be a page tone, probably something else
                    self._reset()
                return None
            elif detected_freq is None:
                # Tone A ended, now in gap
                if self.tone_a_duration >= MIN_TONE_DURATION_S:
                    self.gap_duration = dt_seconds
                    self.state = "gap"
                else:
                    self._reset()
                return None
            else:
                # Different frequency — could be immediate transition to tone B
                if self.tone_a_duration >= MIN_TONE_DURATION_S:
                    self.stable_freq = detected_freq
                    self.stable_count = 1
                    self.gap_duration = 0
                    self.state = "maybe_tone_b"
                else:
                    self._reset()
                return None

        elif self.state == "gap":
            self.gap_duration += dt_seconds
            if self.gap_duration > TONE_GAP_MAX_S:
                # Gap too long, not a two-tone page
                self._reset()
                return None
            if detected_freq is not None:
                self.stable_freq = detected_freq
                self.stable_count = 1
                self.state = "maybe_tone_b"
            return None

        elif self.state == "maybe_tone_b":
            if detected_freq == self.stable_freq:
                self.stable_count += 1
                if self.stable_count >= TONE_STABILITY_WINDOWS:
                    self.tone_b_freq = self.stable_freq
                    self.state = "tone_b"
                    # We have both tones — but wait for tone B to finish
                    self.tone_b_duration = self.stable_count * dt_seconds
            else:
                self._reset()
            return None

        elif self.state == "tone_b":
            if detected_freq == self.tone_b_freq:
                self.tone_b_duration += dt_seconds
                if self.tone_b_duration > MAX_TONE_DURATION_S:
                    self._reset()
                return None
            else:
                # Tone B ended — we have a complete two-tone page!
                if self.tone_b_duration >= MIN_TONE_DURATION_S:
                    event = self._emit_event()
                    self._reset()
                    return event
                else:
                    self._reset()
                return None

        return None

    def _emit_event(self):
        """Create a tone detection event and log unknowns."""
        key = f"{self.tone_a_freq}/{self.tone_b_freq}"
        unit = self.lookup.get(key)
        timestamp = datetime.now().strftime("%H:%M:%S")

        event = {
            "tone_a": self.tone_a_freq,
            "tone_b": self.tone_b_freq,
            "key": key,
            "unit": unit,
            "timestamp": timestamp,
        }

        if unit is None:
            self._log_unknown(event)

        return event

    def _log_unknown(self, event):
        """Append unknown tone pair to log file for later identification."""
        with open(self.unknown_log_path, "a") as f:
            f.write(f"[{event['timestamp']}] {event['key']} (unknown)\n")

    def _reset(self):
        """Reset state machine to idle."""
        self.state = "idle"
        self.tone_a_freq = None
        self.tone_a_duration = 0
        self.tone_b_freq = None
        self.gap_duration = 0
        self.stable_freq = None
        self.stable_count = 0

    def add_mapping(self, key, unit_name):
        """Add a tone pair -> unit mapping to the lookup table."""
        self.lookup[key] = unit_name
        save_lookup_table(self.lookup, self.lookup_path)

    def get_unknown_tones(self):
        """Read the unknown tones log."""
        if self.unknown_log_path.exists():
            return self.unknown_log_path.read_text()
        return ""


# ---------------------------------------------------------------------------
# Standalone test mode
# ---------------------------------------------------------------------------
def test_wav_file(wav_path):
    """Run tone detection on a WAV file for testing."""
    with wave.open(wav_path, "rb") as wf:
        assert wf.getnchannels() == 1, "Expected mono WAV"
        assert wf.getsampwidth() == 2, "Expected 16-bit WAV"
        sr = wf.getframerate()
        raw = wf.readframes(wf.getnframes())

    audio = np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0

    detector = TwoToneDetector()

    # Process in chunks like the live pipeline would
    chunk_size = int(sr * 5)  # 5-second chunks
    for i in range(0, len(audio), chunk_size):
        chunk = audio[i:i + chunk_size]
        events = detector.feed_audio(chunk, sr)
        for event in events:
            if event["unit"]:
                print(f"[{event['timestamp']}] TONE: {event['key']} → {event['unit']}")
            else:
                print(f"[{event['timestamp']}] TONE: {event['key']} → UNKNOWN (logged)")

    print(f"\nUnknown tones logged to: {detector.unknown_log_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tone_detector.py <test.wav>")
        print("       Runs tone detection on a WAV file for testing/tuning.")
        sys.exit(1)
    test_wav_file(sys.argv[1])
