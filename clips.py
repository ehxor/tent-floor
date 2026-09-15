"""
clips.py — audio clips for the transcripts.

A transcript of garbled radio is not verifiable without the audio it came from.
whisper hallucinates — the existence of hallucinations.txt is an admission of
that — so a consumer looking at "engine three responding" has no way to tell a
clean transcription from an invented one, and neither does anyone tuning
jargon.txt. The clip is the ground truth.

The PCM is already in hand at the emit site and was previously discarded after
whisper read it. Here it is encoded to Opus and kept for a week.

Storage shape:

- Content-addressed on the sha256 of the PCM, so the same audio never lands
  twice and a retry cannot produce a second copy.
- Written *before* transcription, so a whisper timeout or crash cannot take the
  audio with it. A clip whose transcription produces nothing is deleted again;
  one orphaned by a crash is collected by the sweep.
- Its own retention, shorter than the log's. Clips are the largest and most
  sensitive thing the system stores, and the events that reference them outlive
  them: an expired clip leaves the transcript intact with a dead reference,
  which is the intended shape rather than a bug.

See docs/architecture.md.

Requirements:
    ffmpeg with libopus (the same ffmpeg already required for stream decoding)
"""

import hashlib
import os
import subprocess
import time
from pathlib import Path

# ~16 kbps mono is transparent for dispatch voice. Raw capture is 16 kHz 16-bit
# mono, i.e. 256 kbps, so this is about 16x smaller than the WAV that gets built
# for whisper anyway — roughly 2 kB per second of speech, so an hour of actual
# transmissions a day costs about 50 MB across a 7 day retention.
DEFAULT_BITRATE = "16k"
DEFAULT_RETENTION_DAYS = 7
DEFAULT_DIR = "clips"

ENCODE_TIMEOUT_S = 30

# Opus only supports a handful of sample rates and 16000 is one of them, so the
# capture rate passes through without resampling.
SUPPORTED_RATES = (8000, 12000, 16000, 24000, 48000)


def probe_encoder(ffmpeg_bin="ffmpeg"):
    """Whether this ffmpeg can encode Opus. Returns (ok, detail)."""
    try:
        result = subprocess.run([ffmpeg_bin, "-hide_banner", "-encoders"],
                                capture_output=True, text=True, timeout=10)
    except FileNotFoundError:
        return False, f"{ffmpeg_bin} not found"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
    if result.returncode != 0:
        return False, f"{ffmpeg_bin} -encoders exited {result.returncode}"
    if "libopus" not in result.stdout:
        return False, f"{ffmpeg_bin} has no libopus encoder"
    return True, "libopus"


def encode_command(ffmpeg_bin, sample_rate, bitrate, destination):
    """The ffmpeg invocation, split out so it can be asserted without running."""
    return [
        ffmpeg_bin, "-hide_banner", "-loglevel", "error",
        "-f", "s16le",                  # raw PCM on stdin, as captured
        "-ar", str(sample_rate),
        "-ac", "1",
        "-i", "pipe:0",
        "-c:a", "libopus",
        "-b:a", bitrate,
        "-application", "voip",         # tuned for speech, not music
        "-y", str(destination),
    ]


class ClipStore:
    """Writes clips to disk and records them in the store."""

    def __init__(self, store, directory=DEFAULT_DIR, retention_days=DEFAULT_RETENTION_DAYS,
                 bitrate=DEFAULT_BITRATE, sample_rate=16000, ffmpeg_bin="ffmpeg",
                 encoder=None):
        self.store = store
        self.dir = Path(directory)
        self.retention_days = retention_days
        self.bitrate = bitrate
        self.sample_rate = sample_rate
        self.ffmpeg_bin = ffmpeg_bin
        # Injectable so the lifecycle can be tested without ffmpeg present.
        self._encoder = encoder or self._encode_with_ffmpeg
        self.dir.mkdir(parents=True, exist_ok=True)

    # -- encoding -----------------------------------------------------------
    def _encode_with_ffmpeg(self, pcm_bytes, destination):
        command = encode_command(self.ffmpeg_bin, self.sample_rate,
                                 self.bitrate, destination)
        result = subprocess.run(command, input=pcm_bytes, capture_output=True,
                                timeout=ENCODE_TIMEOUT_S)
        if result.returncode != 0:
            raise RuntimeError(
                f"ffmpeg exited {result.returncode}: "
                f"{result.stderr.decode('utf-8', 'replace').strip()[:200]}")

    # -- writing ------------------------------------------------------------
    def write(self, pcm_bytes, group, stream, duration_s, now=None):
        """Encode and record one clip. Returns its record, or None on failure.

        Never raises: losing a clip is worse than losing a transcript, but
        losing both because the encoder broke is worst of all, so a failure here
        only costs the audio.
        """
        now = time.time() if now is None else now
        clip_id = hashlib.sha256(pcm_bytes).hexdigest()
        path = self.path_for(clip_id)

        existing = self.get(clip_id)
        if existing is not None and path.exists():
            return existing   # identical audio, already stored

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._encoder(pcm_bytes, path)
        except Exception as e:
            print(f"[warn] [clips] encode failed for {stream} "
                  f"({type(e).__name__}: {e})")
            self._unlink(path)
            return None

        try:
            size = path.stat().st_size
        except OSError:
            size = 0

        record = {
            "id": clip_id,
            "path": str(path),
            "group_name": group,
            "stream": stream,
            "duration_s": round(duration_s, 3),
            "bytes": size,
            "codec": "opus",
            "created_at": now,
            "expires_at": now + self.retention_days * 86400,
        }
        try:
            with self.store.lock:
                self.store.conn.execute(
                    "INSERT OR REPLACE INTO clips "
                    "(id, path, group_name, stream, duration_s, bytes, codec, "
                    " created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (record["id"], record["path"], record["group_name"],
                     record["stream"], record["duration_s"], record["bytes"],
                     record["codec"], record["created_at"], record["expires_at"]))
                self.store.conn.commit()
        except Exception as e:
            # The file is on disk but unrecorded. The sweep collects it as an
            # orphan rather than leaving it forever.
            print(f"[warn] [clips] could not record clip ({type(e).__name__}: {e})")
            return None
        return record

    def get(self, clip_id):
        with self.store.lock:
            row = self.store.conn.execute(
                "SELECT * FROM clips WHERE id = ?", (clip_id,)).fetchone()
        return dict(row) if row else None

    def path_for(self, clip_id):
        # Two-level fan-out: a week of a busy scanner is a lot of files for one
        # directory, and most filesystems slow down long before it matters.
        return self.dir / clip_id[:2] / f"{clip_id}.opus"

    def delete(self, clip_id):
        """Remove a clip and its row. Used when a transmission turned out to
        have no transcript worth keeping."""
        record = self.get(clip_id)
        if record:
            self._unlink(Path(record["path"]))
        with self.store.lock:
            self.store.conn.execute("DELETE FROM clips WHERE id = ?", (clip_id,))
            self.store.conn.commit()

    @staticmethod
    def _unlink(path):
        try:
            Path(path).unlink()
        except FileNotFoundError:
            pass
        except OSError as e:
            print(f"[warn] [clips] could not remove {path} ({e})")

    # -- maintenance --------------------------------------------------------
    def sweep(self, now=None):
        """Delete expired clips, and any file on disk with no row behind it.

        Returns (expired, orphans). Orphans are how a crash between encoding
        and recording is cleaned up — without this they would sit on disk
        forever, which is exactly the unbounded growth retention exists to stop.
        """
        now = time.time() if now is None else now
        with self.store.lock:
            rows = self.store.conn.execute(
                "SELECT id, path FROM clips WHERE expires_at < ?", (now,)).fetchall()
            for row in rows:
                self._unlink(Path(row["path"]))
            self.store.conn.execute("DELETE FROM clips WHERE expires_at < ?", (now,))
            self.store.conn.commit()
            known = {r[0] for r in
                     self.store.conn.execute("SELECT path FROM clips").fetchall()}

        orphans = 0
        cutoff = now - self.retention_days * 86400
        for path in self.dir.rglob("*.opus"):
            if str(path) in known:
                continue
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink()
                    orphans += 1
            except OSError:
                continue
        return len(rows), orphans

    def usage(self):
        """(count, bytes) currently stored, for the startup line."""
        with self.store.lock:
            row = self.store.conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(bytes), 0) FROM clips").fetchone()
        return row[0], row[1]
