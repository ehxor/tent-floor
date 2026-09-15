#!/usr/bin/env python3
"""
scanner_transcribe.py — Live-transcribe multiple scanner audio streams
                        using whisper.cpp with group-based configuration.

Usage:
    # Config file mode (recommended)
    python scanner_transcribe.py --config config.json

    # Quick single-stream mode (still works)
    python scanner_transcribe.py https://icecast0.scanbc.com/nanaimo

Broadcastify premium "Static URL" streams
(https://audio.broadcastify.com/<FEEDID>.mp3) additionally require
BROADCASTIFY_USERNAME and BROADCASTIFY_PASSWORD in the environment.

See config.example.json for the full config format with groups, pollers,
and per-group outputs (Discord webhooks + web feed URLs).

Requirements:
    pip install numpy
    pip install cryptography  (only if using PulsePoint poller)
    System: ffmpeg installed
    whisper.cpp built with -DGGML_VULKAN=ON or -DGGML_METAL=ON
"""

import sys
import subprocess
import signal
import time
import argparse
import json
import re
import threading
import queue
import tempfile
import struct
import os
import select
import base64
import urllib.request
import numpy as np
from pathlib import Path
from urllib.parse import urlparse

import events

# ---------------------------------------------------------------------------
# Config — tweak these to taste
# ---------------------------------------------------------------------------
SAMPLE_RATE = 16000
BEAM_SIZE = 3
SPECTRAL_FLUX_THRESHOLD = 0.5

# Transmission-aware chunking
MINI_CHUNK_MS = 250
SILENCE_TO_SPLIT_MS = 600
MAX_TRANSMISSION_S = 30
MIN_TRANSMISSION_S = 1.0
PRE_SPEECH_PADDING_MS = 250

# Stream management
STREAM_RESTART_S = 36 * 3600
STREAM_HEALTH_CHECK_S = 30

# Read timeout - how long to wait for data before considering stream stalled
READ_TIMEOUT_S = 5.0
MAX_CONSECUTIVE_TIMEOUTS = 10  # After this many timeouts, restart stream
STARTUP_GRACE_PERIOD_S = 15  # Time allowed for new stream to produce first data
STREAM_LIVENESS_TIMEOUT_S = 60  # Max time without data before restarting verified stream

# Hallucination filter (loaded from hallucinations.txt, reloaded hourly)
HALLUCINATIONS = set()

STREAM_COLORS = [
    "\033[96m", "\033[93m", "\033[92m", "\033[95m",
    "\033[91m", "\033[94m", "\033[97m", "\033[33m",
]
COLOR_RESET = "\033[0m"
COLOR_BOLD = "\033[1m"
# ---------------------------------------------------------------------------


# ===================================================================
# Output helpers
# ===================================================================
def send_to_discord(message, webhook_url):
    if not webhook_url:
        return
    data = json.dumps({"content": message}).encode("utf-8")
    req = urllib.request.Request(webhook_url, data=data,
        headers={"Content-Type": "application/json",
                 "User-Agent": "ScannerFeed/1.0"})
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


def send_to_feed(message, feed_url, feed_token, line_type="transcript"):
    if not feed_url:
        return
    data = json.dumps({"line": message, "type": line_type}).encode("utf-8")
    req = urllib.request.Request(feed_url.rstrip("/") + "/ingest", data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {feed_token}",
            "User-Agent": "ScannerFeed/1.0",
        })
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


class GroupOutputs:
    """Holds output destinations for a group."""
    def __init__(self, discord_webhook=None, feed_url=None, feed_token=None):
        self.discord_webhook = discord_webhook
        self.feed_url = feed_url
        self.feed_token = feed_token or ""

    def send_discord(self, message):
        send_to_discord(message, self.discord_webhook)

    def send_feed(self, message, line_type="transcript"):
        send_to_feed(message, self.feed_url, self.feed_token, line_type)

    def emit(self, event):
        """Publish a structured event (see events.py).

        Phase 0 renders it down to the strings the existing outputs already
        take, so nothing downstream changes yet. Phase 2 replaces the body with
        an append to the local event log, at which point these blocking sends
        move off the transcription path and onto their own cursors.
        """
        render = event["render"]
        self.send_discord(render["discord"])
        self.send_feed(render["plain"], events.legacy_line_type(event))


# ===================================================================
# Jargon
# ===================================================================
def load_jargon(jargon_path):
    if not jargon_path:
        return None
    path = Path(jargon_path)
    if not path.exists():
        print(f"[warn] Jargon file not found: {jargon_path}")
        return None
    terms = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            terms.append(line)
    if not terms:
        return None
    prompt = "Transcript of emergency radio dispatch. Common terms: " + ", ".join(terms) + "."
    if len(prompt) > 800:
        prompt = prompt[:800].rsplit(",", 1)[0] + "."
    return prompt


def load_hallucinations(path="hallucinations.txt"):
    """Load hallucination filter phrases from text file (one per line)."""
    try:
        p = Path(path)
        if not p.exists():
            return {
                ".", "..", "...", "Thank you.", "Thanks for watching!",
                "Thank you for watching!", "Please subscribe.", "Thanks for listening.",
                "you", "You", "Bye.", "Bye-bye.", "Thank you for watching.",
                "I'm sorry.", "Okay.", "Oh.", "Hmm.", "Ah.",
            }
        return set(line.strip() for line in p.read_text().splitlines() if line.strip())
    except Exception as e:
        print(f"[warn] Failed to load hallucinations: {e}")
        return set()


# ===================================================================
# VAD — spectral flux
# ===================================================================
def has_speech(audio_np, sample_rate=SAMPLE_RATE, debug=False):
    window_size = int(sample_rate * 0.025)
    hop_size = int(sample_rate * 0.010)
    spectra = []
    for start in range(0, len(audio_np) - window_size, hop_size):
        window = audio_np[start:start + window_size]
        mag = np.abs(np.fft.rfft(window * np.hanning(window_size)))
        spectra.append(mag)
    if len(spectra) < 2:
        if debug:
            print(f"  [vad] insufficient_spectra: len={len(spectra)}, audio_len={len(audio_np)}")
        return False
    flux_values = []
    for i in range(1, len(spectra)):
        diff = spectra[i] - spectra[i - 1]
        flux_values.append(np.sum(np.maximum(diff, 0)))
    if len(flux_values) == 0:
        if debug:
            print(f"  [vad] empty_flux_values (audio_len={len(audio_np)})")
        return False
    avg_flux = np.mean(flux_values)
    if np.isnan(avg_flux):
        if debug:
            print(f"  [vad] nan_flux_result")
        return False
    #if debug:
    #    print(f"  [vad] spectral_flux={avg_flux:.4f}")
    return avg_flux > SPECTRAL_FLUX_THRESHOLD


# ===================================================================
# Audio helpers
# ===================================================================
def pcm_to_wav_bytes(audio_np):
    pcm_int16 = (audio_np * 32767).astype(np.int16)
    num_samples = len(pcm_int16)
    data_size = num_samples * 2
    buf = bytearray()
    buf.extend(b"RIFF")
    buf.extend(struct.pack("<I", 36 + data_size))
    buf.extend(b"WAVE")
    buf.extend(b"fmt ")
    buf.extend(struct.pack("<I", 16))
    buf.extend(struct.pack("<H", 1))
    buf.extend(struct.pack("<H", 1))
    buf.extend(struct.pack("<I", SAMPLE_RATE))
    buf.extend(struct.pack("<I", SAMPLE_RATE * 2))
    buf.extend(struct.pack("<H", 2))
    buf.extend(struct.pack("<H", 16))
    buf.extend(b"data")
    buf.extend(struct.pack("<I", data_size))
    buf.extend(pcm_int16.tobytes())
    return bytes(buf)


def transcribe_chunk(whisper_bin, model_path, wav_bytes, prompt=None, beam_size=BEAM_SIZE):
    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    try:
        tmp.write(wav_bytes)
        tmp.close()
        cmd = [whisper_bin, "--model", model_path, "--file", tmp.name,
               "--language", "en", "--beam-size", str(beam_size), "--no-timestamps"]
        if prompt:
            cmd.extend(["--prompt", prompt])
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            print(f"[debug] whisper failed (code {result.returncode}): {result.stderr.strip()}")
            return ""
        lines = []
        for line in result.stdout.splitlines():
            text = line.strip()
            if text.startswith("[") and "-->" in text:
                bracket_end = text.rfind("]")
                if bracket_end >= 0:
                    text = text[bracket_end + 1:].strip()
            text = re.sub(r'\*[^*]+\*', '', text).strip()
            text = re.sub(r'\[[^\]]*\]', '', text).strip()
            text = re.sub(r'\([^)]*\)', '', text).strip()
            if text and text not in HALLUCINATIONS:
                lines.append(text)
        return " ".join(lines)
    except subprocess.TimeoutExpired:
        print(f"[debug] whisper timeout (>60s)")
        return ""
    finally:
        os.unlink(tmp.name)


# ===================================================================
# FFmpeg + transmission-aware chunking
# ===================================================================
def log_ffmpeg_stderr(stream_name, proc, prefix=""):
    """Log any stderr output from ffmpeg for debugging."""
    try:
        # Non-blocking read of stderr
        stderr = proc.stderr.read()
        if stderr:
            stderr_text = stderr.decode('utf-8', errors='replace').strip()
            if stderr_text:
                # Limit output to avoid log spam
                lines = stderr_text.splitlines()[:5]
                for line in lines:
                    print(f"[{format_timestamp()}] [{stream_name}] {prefix}ffmpeg: {line}")
    except Exception:
        pass


FFMPEG_USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# Broadcastify premium "Static URL" streams require HTTP basic auth using the
# membership login. Credentials come from the environment so they never land in
# config.json (which is tracked in git).
BROADCASTIFY_AUTH_HOSTS = ("audio.broadcastify.com",)
BROADCASTIFY_USER_ENV = "BROADCASTIFY_USERNAME"
BROADCASTIFY_PASS_ENV = "BROADCASTIFY_PASSWORD"


def url_needs_broadcastify_auth(stream_url):
    try:
        return (urlparse(stream_url).hostname or "").lower() in BROADCASTIFY_AUTH_HOSTS
    except ValueError:
        return False


def broadcastify_auth_header():
    """Build an Authorization header value, or None if credentials are unset."""
    user = os.environ.get(BROADCASTIFY_USER_ENV)
    password = os.environ.get(BROADCASTIFY_PASS_ENV)
    if not user or not password:
        return None
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return f"Authorization: Basic {token}\r\n"


def start_ffmpeg(stream_url):
    cmd = ["ffmpeg", "-loglevel", "warning",
           "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_at_eof", "1",
           "-reconnect_delay_max", "30",
           "-user_agent", FFMPEG_USER_AGENT]
    # Sent as a header rather than as userinfo in the URL. Note that basic auth
    # is only base64, so this keeps the plaintext password out of the process
    # list but is not a real secret barrier against anyone who can read `ps`.
    if url_needs_broadcastify_auth(stream_url):
        header = broadcastify_auth_header()
        if header:
            cmd += ["-headers", header]
    cmd += ["-i", stream_url, "-f", "s16le", "-acodec", "pcm_s16le",
            "-ar", str(SAMPLE_RATE), "-ac", "1", "-"]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


# Global stream activity tracking (thread-safe)
stream_activity = {}
stream_activity_lock = threading.Lock()


def update_stream_activity(stream_name):
    """Called by reader thread when it receives data."""
    with stream_activity_lock:
        stream_activity[stream_name] = time.monotonic()


def get_stream_activity(stream_name):
    """Get last activity time for a stream."""
    with stream_activity_lock:
        return stream_activity.get(stream_name, 0)


def stream_reader_thread(stream_name, ffmpeg_proc, work_queue, stop_event, debug_vad=False):
    mini_chunk_samples = int(SAMPLE_RATE * MINI_CHUNK_MS / 1000)
    mini_chunk_bytes = mini_chunk_samples * 2
    silence_split_chunks = int(SILENCE_TO_SPLIT_MS / MINI_CHUNK_MS)
    max_chunks = int(MAX_TRANSMISSION_S * 1000 / MINI_CHUNK_MS)
    min_chunks = int(MIN_TRANSMISSION_S * 1000 / MINI_CHUNK_MS)
    padding_chunks = int(PRE_SPEECH_PADDING_MS / MINI_CHUNK_MS)

    transmission_buf = []
    silence_count = 0
    in_transmission = False
    pre_speech_ring = []
    raw_buffer = b""
    consecutive_timeouts = 0

    try:
        while not stop_event.is_set():
            # Use select to check if data is available before reading
            try:
                # select() on file descriptor with timeout
                ready, _, _ = select.select([ffmpeg_proc.stdout], [], [], READ_TIMEOUT_S)
            except (ValueError, OSError) as e:
                # File descriptor invalid (process died)
                print(f"[{format_timestamp()}] [{stream_name}] SELECT ERROR: {type(e).__name__}: {e}")
                break

            if not ready:
                # Timeout - no data available
                consecutive_timeouts += 1
                if debug_vad:
                    print(f"[{format_timestamp()}] [{stream_name}] READ TIMEOUT ({consecutive_timeouts}/{MAX_CONSECUTIVE_TIMEOUTS})")
                
                # Check if ffmpeg process is still alive
                if ffmpeg_proc.poll() is not None:
                    print(f"[{format_timestamp()}] [{stream_name}] FFmpeg process died (exit code: {ffmpeg_proc.returncode})")
                    log_ffmpeg_stderr(stream_name, ffmpeg_proc, "on death: ")
                    break
                
                if consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS:
                    print(f"[{format_timestamp()}] [{stream_name}] STALLED: no data for {MAX_CONSECUTIVE_TIMEOUTS * READ_TIMEOUT_S:.0f}s")
                    break
                continue

            # Data is available, reset timeout counter
            consecutive_timeouts = 0

            try:
                data = ffmpeg_proc.stdout.read(mini_chunk_bytes)
            except Exception as e:
                print(f"[{format_timestamp()}] [{stream_name}] READ ERROR: {type(e).__name__}: {e}")
                break

            if not data:
                print(f"[{format_timestamp()}] [{stream_name}] EOF: stdout closed")
                break
            if len(data) < mini_chunk_bytes:
                print(f"[{format_timestamp()}] [{stream_name}] PARTIAL READ: {len(data)}/{mini_chunk_bytes} bytes")
            
            # Update activity tracking
            update_stream_activity(stream_name)
            
            raw_buffer += data
            while len(raw_buffer) >= mini_chunk_bytes:
                chunk_bytes = raw_buffer[:mini_chunk_bytes]
                raw_buffer = raw_buffer[mini_chunk_bytes:]
                try:
                    audio_np = np.frombuffer(chunk_bytes, dtype=np.int16).astype(np.float32) / 32768.0
                except Exception as e:
                    print(f"[{format_timestamp()}] [{stream_name}] AUDIO CONVERSION ERROR: {type(e).__name__}: {e}")
                    continue
                is_speech = has_speech(audio_np, SAMPLE_RATE, debug=debug_vad)

                if is_speech:
                    if not in_transmission:
                        in_transmission = True
                        transmission_buf = list(pre_speech_ring)
                        pre_speech_ring = []
                    transmission_buf.append(audio_np)
                    silence_count = 0
                    if len(transmission_buf) >= max_chunks:
                        _emit_transmission(stream_name, transmission_buf, work_queue, min_chunks)
                        transmission_buf = []
                        in_transmission = False
                else:
                    if in_transmission:
                        silence_count += 1
                        transmission_buf.append(audio_np)
                        if silence_count >= silence_split_chunks:
                            _emit_transmission(stream_name, transmission_buf, work_queue, min_chunks)
                            transmission_buf = []
                            silence_count = 0
                            in_transmission = False
                    else:
                        pre_speech_ring.append(audio_np)
                        if len(pre_speech_ring) > padding_chunks:
                            pre_speech_ring.pop(0)

        if transmission_buf:
            _emit_transmission(stream_name, transmission_buf, work_queue, min_chunks)
    except Exception as e:
        print(f"[{format_timestamp()}] [{stream_name}] THREAD ERROR: {type(e).__name__}: {e}")
    finally:
        print(f"[{format_timestamp()}] [{stream_name}] THREAD END")
        try:
            work_queue.put((stream_name, None), timeout=5)
        except queue.Full:
            print(f"[{format_timestamp()}] [{stream_name}] QUEUE FULL on shutdown signal")


def _emit_transmission(stream_name, mini_chunks, work_queue, min_chunks):
    if len(mini_chunks) < min_chunks:
        return
    try:
        audio = np.concatenate(mini_chunks)
        pcm_bytes = (audio * 32768).astype(np.int16).tobytes()
        work_queue.put((stream_name, pcm_bytes), timeout=5)
    except queue.Full:
        print(f"[{format_timestamp()}] [{stream_name}] QUEUE FULL: dropping transmission")
    except Exception as e:
        print(f"[{format_timestamp()}] [{stream_name}] EMIT ERROR: {type(e).__name__}: {e}")


# ===================================================================
# Whisper binary / model finders
# ===================================================================
def find_whisper_bin():
    for c in ["./whisper.cpp/build/bin/whisper-cli",
              str(Path.home() / "whisper.cpp/build/bin/whisper-cli")]:
        if Path(c).exists():
            return c
    result = subprocess.run(["which", "whisper-cli"], capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout.strip()
    return None


def find_model_file(model_name):
    for c in [f"./whisper.cpp/models/ggml-{model_name}.bin",
              str(Path.home() / f"whisper.cpp/models/ggml-{model_name}.bin")]:
        if Path(c).exists():
            return c
    return None


def format_timestamp():
    """Wall-clock time for terminal output. Events carry UTC — see events.py."""
    return events.local_clock()


# ===================================================================
# Config loader
# ===================================================================
ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def expand_env_vars(value, missing):
    """Recursively substitute ${VAR} references in config strings.

    Unset variables expand to an empty string and are collected in `missing`
    so the caller can warn about them.
    """
    if isinstance(value, str):
        def replace(match):
            name = match.group(1)
            env_value = os.environ.get(name)
            if env_value is None:
                missing.add(name)
                return ""
            return env_value
        return ENV_VAR_PATTERN.sub(replace, value)
    if isinstance(value, dict):
        return {k: expand_env_vars(v, missing) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env_vars(v, missing) for v in value]
    return value


def load_config(config_path):
    """Load and validate the JSON config file.

    String values may reference environment variables as ${VAR}; secrets such
    as feed tokens and webhook URLs should be kept out of the file this way.

    Returns a dict with:
        whisper: {bin, model, model_path}
        groups: {group_name: {outputs: GroupOutputs, streams: [...], pollers: [...]}}
        all_streams: [{name, url, jargon_prompt, tone_lookup_path, group}]
    """
    with open(config_path) as f:
        raw = json.load(f)

    missing = set()
    raw = expand_env_vars(raw, missing)
    for name in sorted(missing):
        print(f"[config] Warning: ${{{name}}} is not set, expanded to empty string")

    whisper_cfg = raw.get("whisper", {})

    groups = {}
    all_streams = []
    color_idx = 0

    for group_name, group_def in raw.get("groups", {}).items():
        # Outputs
        out_cfg = group_def.get("outputs", {})
        outputs = GroupOutputs(
            discord_webhook=out_cfg.get("discord_webhook"),
            feed_url=out_cfg.get("feed_url"),
            feed_token=out_cfg.get("feed_token"),
        )

        # Streams
        streams = []
        for s in group_def.get("streams", []):
            entry = {
                "name": s.get("name", f"Stream {color_idx + 1}"),
                "url": s["url"],
                "jargon_prompt": load_jargon(s.get("jargon")),
                "tone_lookup_path": s.get("tone_lookup"),
                "group": group_name,
                "color": STREAM_COLORS[color_idx % len(STREAM_COLORS)],
            }
            streams.append(entry)
            all_streams.append(entry)
            color_idx += 1

        # Pollers
        pollers = group_def.get("pollers", [])

        groups[group_name] = {
            "outputs": outputs,
            "streams": streams,
            "pollers": pollers,
        }

    return {
        "whisper": whisper_cfg,
        "groups": groups,
        "all_streams": all_streams,
    }


# ===================================================================
# Main
# ===================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Live-transcribe scanner audio streams using whisper.cpp")
    parser.add_argument("stream_url", nargs="?", default=None,
                        help="Audio stream URL (quick single-stream mode)")
    parser.add_argument("--config", "-c",
                        help="Path to config.json (recommended)")
    parser.add_argument("--jargon", "-j", help="Jargon file (single-stream mode)")
    parser.add_argument("--model", "-m", default="large-v3")
    parser.add_argument("--model-path", "-M")
    parser.add_argument("--whisper-bin", "-w")
    parser.add_argument("--no-tones", action="store_true")
    parser.add_argument("--no-color", action="store_true")
    parser.add_argument("--debug-vad", action="store_true")
    # Legacy single-output flags (used in single-stream mode)
    parser.add_argument("--discord-webhook")
    parser.add_argument("--feed-url")
    parser.add_argument("--feed-token")
    args = parser.parse_args()

    if not args.stream_url and not args.config:
        parser.error("Provide a stream URL or --config config.json")

    # --- Load config ---
    if args.config:
        config = load_config(args.config)
    else:
        # Build a config from CLI args for backwards compatibility
        config = {
            "whisper": {"bin": args.whisper_bin, "model": args.model, "model_path": args.model_path},
            "groups": {
                "default": {
                    "outputs": GroupOutputs(
                        discord_webhook=args.discord_webhook,
                        feed_url=args.feed_url,
                        feed_token=args.feed_token,
                    ),
                    "streams": [{
                        "name": "Scanner",
                        "url": args.stream_url,
                        "jargon_prompt": load_jargon(args.jargon),
                        "tone_lookup_path": None,
                        "group": "default",
                        "color": STREAM_COLORS[0],
                    }],
                    "pollers": [],
                }
            },
            "all_streams": [{
                "name": "Scanner",
                "url": args.stream_url,
                "jargon_prompt": load_jargon(args.jargon),
                "tone_lookup_path": None,
                "group": "default",
                "color": STREAM_COLORS[0],
            }],
        }

    all_streams = config["all_streams"]
    groups = config["groups"]

    if not all_streams:
        print("[error] No streams configured")
        sys.exit(1)

    # Broadcastify premium streams fail with 401 if credentials are missing;
    # say so up front rather than looping on connection failures.
    if any(url_needs_broadcastify_auth(s["url"]) for s in all_streams):
        if broadcastify_auth_header():
            print("[init] Broadcastify credentials: loaded from environment")
        else:
            print(f"[error] Broadcastify streams are configured but "
                  f"${BROADCASTIFY_USER_ENV}/${BROADCASTIFY_PASS_ENV} are not set.")
            sys.exit(1)

    # --- Locate whisper.cpp ---
    w_cfg = config["whisper"]
    whisper_bin = args.whisper_bin or w_cfg.get("bin") or find_whisper_bin()
    if not whisper_bin:
        print("[error] Could not find whisper-cli binary.")
        sys.exit(1)
    print(f"[init] whisper-cli: {whisper_bin}")

    model_name = args.model or w_cfg.get("model", "large-v3")
    if args.model_path or w_cfg.get("model_path"):
        model_path = args.model_path or w_cfg["model_path"]
    else:
        model_path = find_model_file(model_name)
    if not model_path or not Path(model_path).exists():
        print(f"[error] Model file not found for '{model_name}'.")
        sys.exit(1)
    print(f"[init] Model: {model_path}")

    # --- Build lookups ---
    stream_group_map = {}   # stream_name -> group_name
    stream_jargon = {}      # stream_name -> prompt or None
    stream_color_map = {}   # stream_name -> ANSI color

    for s in all_streams:
        stream_group_map[s["name"]] = s["group"]
        stream_jargon[s["name"]] = s["jargon_prompt"]
        stream_color_map[s["name"]] = s["color"] if not args.no_color else ""

    # Print stream info
    for group_name, g in groups.items():
        print(f"\n[init] === Group: {group_name} ===")
        out = g["outputs"]
        if out.discord_webhook:
            print(f"[init]   Discord: configured")
        if out.feed_url:
            print(f"[init]   Web feed: {out.feed_url}")
        for s in g["streams"]:
            color = s["color"] if not args.no_color else ""
            reset = COLOR_RESET if not args.no_color else ""
            j = "with jargon" if s["jargon_prompt"] else "no jargon"
            print(f"[init]   {color}{s['name']}{reset}: {s['url']} ({j})")
        for p in g["pollers"]:
            print(f"[init]   Poller: {p['type']}"
                  + (f" (agency: {p.get('agency', '?')})" if p.get('agency') else ""))

    # --- Tone detector (per-stream) ---
    stream_tone_detectors = {}
    if not args.no_tones:
        try:
            from tone_detector import TwoToneDetector
            for s in all_streams:
                tlp = s.get("tone_lookup_path")
                if tlp:
                    safe_name = s["name"].lower().replace(" ", "_")
                    unknown_log = f"unknown_tones_{safe_name}.log"
                    det = TwoToneDetector(lookup_path=tlp, unknown_log_path=unknown_log)
                    stream_tone_detectors[s["name"]] = det
                    print(f"[init]   {s['name']}: tone detector ready ({len(det.lookup)} mappings)")
            if stream_tone_detectors:
                print(f"[init] Tone detection enabled for {len(stream_tone_detectors)} stream(s)")
            else:
                print("[init] Tone detection disabled (no streams have tone_lookup configured)")

        except ImportError:
            print("\n[init] Tone detection disabled (tone_detector.py not found)")

    HALLUCINATIONS.update(load_hallucinations())
    print(f"[init] Loaded {len(HALLUCINATIONS)} hallucination filters")

    # --- VAD ---
    print(f"[init] Chunking: transmission-aware (flux: {SPECTRAL_FLUX_THRESHOLD}, "
          f"split: {SILENCE_TO_SPLIT_MS}ms)")
    print(f"[init] Read timeout: {READ_TIMEOUT_S}s, max consecutive: {MAX_CONSECUTIVE_TIMEOUTS}")

    # --- Start pollers per group ---
    all_pollers = []

    for group_name, g in groups.items():
        out = g["outputs"]
        for poller_cfg in g["pollers"]:
            ptype = poller_cfg["type"]

            if ptype == "pulsepoint":
                try:
                    from pulsepoint_poller import PulsePointPoller, format_event, format_event_discord
                    agency = poller_cfg["agency"]
                    prefixes = poller_cfg.get("unit_prefix")

                    def make_pp_cb(o, gn):
                        def cb(event):
                            msg = format_event(event)
                            print(f"[{format_timestamp()}] [{gn}] {msg}")
                            o.emit(events.poller(gn, event, msg,
                                                 format_event_discord(event)))
                        return cb

                    pp = PulsePointPoller(agency_id=agency, unit_prefixes=prefixes,
                                          callback=make_pp_cb(out, group_name))
                    pp.start()
                    all_pollers.append(pp)
                    print(f"[init] [{group_name}] PulsePoint poller started (agency: {agency})")
                except ImportError:
                    print(f"[warn] [{group_name}] PulsePoint disabled (missing dependency)")

            elif ptype == "nanaimo_fire":
                try:
                    from nanaimo_fire_poller import NanaimoFirePoller, \
                        format_event as nf_fmt, format_event_discord as nf_fmt_d

                    def make_nf_cb(o, gn):
                        def cb(event):
                            print(f"[{format_timestamp()}] [{gn}] {nf_fmt(event)}")
                            o.emit(events.poller(gn, event, nf_fmt(event),
                                                 nf_fmt_d(event)))
                        return cb

                    nf = NanaimoFirePoller(callback=make_nf_cb(out, group_name))
                    nf.start()
                    all_pollers.append(nf)
                    print(f"[init] [{group_name}] Nanaimo Fire poller started")
                except ImportError:
                    print(f"[warn] [{group_name}] Nanaimo Fire disabled (missing module)")

            elif ptype == "bc_wildfire":
                try:
                    from bc_wildfire_poller import BCWildfirePoller, parse_polygon, \
                        format_event as wf_fmt, format_event_discord as wf_fmt_d

                    polygon_cfg = poller_cfg.get("polygon")
                    polygon = parse_polygon(polygon_cfg) if polygon_cfg else None
                    fire_centre = poller_cfg.get("fire_centre_code")

                    def make_wf_cb(o, gn):
                        def cb(event):
                            print(f"[{format_timestamp()}] [{gn}] {wf_fmt(event)}")
                            if event["type"] != "wildfire_removed":
                                o.emit(events.poller(gn, event, wf_fmt(event),
                                                     wf_fmt_d(event)))

                        return cb

                    wf = BCWildfirePoller(polygon=polygon,
                                          fire_centre_code=fire_centre,
                                          callback=make_wf_cb(out, group_name))
                    wf.start()
                    all_pollers.append(wf)
                    scope = f"{len(polygon)} point polygon" if polygon else "no polygon filter"
                    print(f"[init] [{group_name}] BC Wildfire poller started ({scope})")
                except ImportError:
                    print(f"[warn] [{group_name}] BC Wildfire disabled (missing module)")

            else:
                print(f"[warn] [{group_name}] Unknown poller type: {ptype}")

    # --- Stream manager ---
    work_queue = queue.Queue(maxsize=20)
    stop_event = threading.Event()

    # --- Tone lookup reloader (per-stream) ---
    if stream_tone_detectors:
        from tone_detector import load_lookup_table

        def lookup_reloader():
            while not stop_event.is_set():
                stop_event.wait(300)
                if stop_event.is_set():
                    break
                for sname, det in stream_tone_detectors.items():
                    try:
                        new_lookup = load_lookup_table(det.lookup_path)
                        det.lookup = new_lookup
                        print(f"[{format_timestamp()}] [tones] [{sname}] Reloaded lookup ({len(new_lookup)} mappings)")
                    except Exception as e:
                        print(f"[{format_timestamp()}] [tones] [{sname}] Lookup reload failed: {e}")

        threading.Thread(target=lookup_reloader, daemon=True).start()

    def hallucinations_reloader():
        while not stop_event.is_set():
            stop_event.wait(300)
            if stop_event.is_set():
                break
            try:
                new_hallucinations = load_hallucinations()
                HALLUCINATIONS.clear()
                HALLUCINATIONS.update(new_hallucinations)
                print(f"[{format_timestamp()}] [hallucinations] Reloaded ({len(new_hallucinations)} filters)")
            except Exception as e:
                print(f"[{format_timestamp()}] [hallucinations] Reload failed: {e}")

    threading.Thread(target=hallucinations_reloader, daemon=True).start()

    managed_streams = []
    managed_lock = threading.Lock()

    def start_one_stream(stream_def):
        proc = start_ffmpeg(stream_def["url"])
        reader = threading.Thread(
            target=stream_reader_thread,
            args=(stream_def["name"], proc, work_queue, stop_event, args.debug_vad),
            daemon=True)
        reader.start()
        return {
            "name": stream_def["name"],
            "url": stream_def["url"],
            "proc": proc,
            "reader": reader,
            "started_at": time.monotonic(),
            "startup_verified": False,  # New field for startup verification
        }

    for s in all_streams:
        color = s["color"] if not args.no_color else ""
        reset = COLOR_RESET if not args.no_color else ""
        print(f"[stream] Connecting to {color}{s['name']}{reset}...")
        managed_streams.append(start_one_stream(s))

    time.sleep(3)
    for ms in managed_streams:
        if ms["proc"].poll() is not None:
            print(f"[error] {ms['name']} failed to connect")
            log_ffmpeg_stderr(ms['name'], ms["proc"], "startup: ")

    active = sum(1 for ms in managed_streams if ms["proc"].poll() is None)
    print(f"\n[stream] {active}/{len(all_streams)} streams connected.")
    print(f"[stream] Streams restart every {STREAM_RESTART_S // 3600}h")
    print(f"[stream] Startup grace period: {STARTUP_GRACE_PERIOD_S}s")
    print("=" * 60)

    def kill_ffmpeg(proc):
        """Aggressively kill an FFmpeg process and close its pipes."""
        try:
            proc.terminate()
        except Exception:
            pass
        try:
            proc.stdout.close()  # Unblocks the reader thread's read() call
        except Exception:
            pass
        try:
            proc.stderr.close()
        except Exception:
            pass
        try:
            proc.wait(timeout=3)
        except Exception:
            try:
                proc.kill()  # SIGKILL if it didn't die from SIGTERM
                proc.wait(timeout=2)
            except Exception:
                pass

    # Enhanced Watchdog with startup verification
    def stream_watchdog():
        while not stop_event.is_set():
            stop_event.wait(STREAM_HEALTH_CHECK_S)
            if stop_event.is_set():
                break
            now = time.monotonic()
            with managed_lock:
                for i, ms in enumerate(managed_streams):
                    # Check if thread died
                    if not ms["reader"].is_alive():
                        print(f"[{format_timestamp()}] [stream] {ms['name']} reader thread died, restarting...")
                        log_ffmpeg_stderr(ms['name'], ms["proc"], "before restart: ")
                        kill_ffmpeg(ms["proc"])
                        managed_streams[i] = start_one_stream(all_streams[i])
                        continue
                    
                    age = now - ms["started_at"]
                    
                    # Check if ffmpeg process died
                    if ms["proc"].poll() is not None:
                        print(f"[{format_timestamp()}] [stream] {ms['name']} ffmpeg died (exit: {ms['proc'].returncode}), restarting...")
                        log_ffmpeg_stderr(ms['name'], ms["proc"])
                        managed_streams[i] = start_one_stream(all_streams[i])
                        continue
                    
                    # Check for startup verification (stream must produce data within grace period)
                    if not ms["startup_verified"]:
                        last_activity = get_stream_activity(ms["name"])
                        time_since_start = now - ms["started_at"]
                        time_since_data = now - last_activity
                        
                        if time_since_start > STARTUP_GRACE_PERIOD_S and time_since_data > STARTUP_GRACE_PERIOD_S:
                            print(f"[{format_timestamp()}] [stream] {ms['name']} STARTUP FAILED: no data after {time_since_start:.0f}s")
                            log_ffmpeg_stderr(ms['name'], ms["proc"], "startup failed: ")
                            kill_ffmpeg(ms["proc"])
                            managed_streams[i] = start_one_stream(all_streams[i])
                            continue
                        elif time_since_data < STARTUP_GRACE_PERIOD_S:
                            # Stream has produced data recently, mark as verified
                            ms["startup_verified"] = True
                    
                    # Steady-state liveness check: restart if no data for too long
                    if ms["startup_verified"]:
                        last_activity = get_stream_activity(ms["name"])
                        time_since_data = now - last_activity
                        if time_since_data > STREAM_LIVENESS_TIMEOUT_S:
                            print(f"[{format_timestamp()}] [stream] {ms['name']} STALE: no audio for {time_since_data:.0f}s, restarting...")
                            log_ffmpeg_stderr(ms['name'], ms["proc"], "stale stream: ")
                            kill_ffmpeg(ms["proc"])
                            managed_streams[i] = start_one_stream(all_streams[i])
                            continue
                    
                    # Scheduled restart
                    if age >= STREAM_RESTART_S:
                        print(f"[{format_timestamp()}] [stream] {ms['name']} "
                              f"scheduled restart ({age / 3600:.1f}h)")
                        old_proc = ms["proc"]
                        managed_streams[i] = start_one_stream(all_streams[i])
                        kill_ffmpeg(old_proc)

    threading.Thread(target=stream_watchdog, daemon=True).start()

    # Transcription lock
    transcribe_lock = threading.Lock()

    # Shutdown
    def shutdown(sig=None, frame=None):
        print("\n[exit] Shutting down...")
        stop_event.set()
        for p in all_pollers:
            try:
                p.stop()
            except Exception:
                pass
        with managed_lock:
            for ms in managed_streams:
                kill_ffmpeg(ms["proc"])
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Silence monitor
    last_activity = {s["name"]: time.monotonic() for s in all_streams}

    def silence_monitor():
        while not stop_event.is_set():
            time.sleep(30)
            now = time.monotonic()
            for s in all_streams:
                idle = now - last_activity.get(s["name"], now)
                if idle > 120:
                    color = stream_color_map.get(s["name"], "")
                    reset = COLOR_RESET if not args.no_color else ""
                    print(f"[{format_timestamp()}] {color}{s['name']}{reset} (silence ~{int(idle)}s)")

    threading.Thread(target=silence_monitor, daemon=True).start()

    # --- Main processing loop ---
    try:
        while not stop_event.is_set():
            try:
                stream_name, chunk_data = work_queue.get(timeout=1)
            except queue.Empty:
                continue

            if chunk_data is None:
                print(f"[{format_timestamp()}] [{stream_name}] READER_SHUTDOWN signal received")
                continue

            last_activity[stream_name] = time.monotonic()

            # Look up this stream's group and outputs
            group_name = stream_group_map.get(stream_name, "default")
            group = groups.get(group_name)
            if not group:
                continue
            out = group["outputs"]

            audio_np = np.frombuffer(chunk_data, dtype=np.int16).astype(np.float32) / 32768.0
            duration = len(audio_np) / SAMPLE_RATE

            color = stream_color_map.get(stream_name, "")
            reset = COLOR_RESET if not args.no_color else ""
            bold = COLOR_BOLD if not args.no_color else ""

            # Tone detection
            det = stream_tone_detectors.get(stream_name)
            if det:
                tone_events = det.feed_audio(audio_np, SAMPLE_RATE)
                for event in tone_events:
                    unit = event["unit"] or "UNKNOWN (logged)"
                    print(f"[{format_timestamp()}] {bold}{color}{stream_name}{reset} "
                          f"*** PAGE: {event['key']} → {unit} ***")
                    out.emit(events.tone_page(group_name, stream_name, event))

            # Transcribe
            prompt = stream_jargon.get(stream_name)
            with transcribe_lock:
                t0 = time.monotonic()
                wav_bytes = pcm_to_wav_bytes(audio_np)
                text = transcribe_chunk(whisper_bin, model_path, wav_bytes,
                                         prompt=prompt, beam_size=BEAM_SIZE)
                elapsed = time.monotonic() - t0

            if text:
                print(f"[{format_timestamp()}] {color}{stream_name}{reset} "
                      f"({duration:.1f}s audio, {elapsed:.1f}s) {text}")
                out.emit(events.transcript(group_name, stream_name, text,
                                          duration_s=duration,
                                          model=model_name,
                                          latency_s=elapsed))

    except Exception as e:
        print(f"[error] {e}")
    finally:
        shutdown()


if __name__ == "__main__":
    main()
