#!/usr/bin/env python3
"""
admin_ui.py — a local web UI for searching transcripts and hearing the audio.

Everything the scanner records is already in the state store, but getting at it
means `sqlite3` and hand-written `json_extract` queries, and the clips are
sha256-named files in a fan-out directory that nothing maps back to the text.
This is the missing half of keeping the audio: a box to type a phrase into and
a play button next to each hit.

Run it on the machine doing the transcription, alongside the scanner:

    python admin_ui.py --db tentfloor.db --clips clips

It is an admin tool, not part of the public feed. It opens the store
**read-only** so it can never disturb the scanner, and binds to localhost so it
is not a public feed by accident. Binding anywhere else requires --token.

Requirements:
    None (stdlib only)
"""

import argparse
import json
import os
import re
import secrets
import sqlite3
import sys
import threading
import urllib.parse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import events

DEFAULT_HOST = "127.0.0.1"
# Above 1024: binding a privileged port would need root, which is the last thing
# a tool that reads a database and serves files off disk should be asking for.
DEFAULT_PORT = 8842
DEFAULT_LIMIT = 50
MAX_LIMIT = 200

# Clip ids are sha256 hex. Anything else never reaches the filesystem.
CLIP_ID_RE = re.compile(r"^[0-9a-f]{64}$")

PAGE_PATH = Path(__file__).resolve().parent / "web" / "admin" / "index.html"


def _like_escape(term):
    r"""Escape LIKE wildcards so a search for "50%" means "50%".

    Without this, % and _ in a query silently match anything, which reads as
    the search being broken rather than as a syntax the user opted into.
    """
    return (term.replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_"))


def parse_range(header, size):
    """Resolve a Range header against a file size.

    Returns (start, end) inclusive, or (None, None) when the range cannot be
    satisfied and the caller owes a 416. A missing or unparseable header means
    the whole file — a malformed Range is not worth failing a request over,
    but a range genuinely past the end is, or the player retries forever.
    """
    if not header or not header.startswith("bytes="):
        return 0, size - 1
    spec = header[len("bytes="):].split(",")[0].strip()
    try:
        if spec.startswith("-"):
            # Suffix range: the last N bytes.
            length = int(spec[1:])
            if length <= 0:
                return None, None
            return max(0, size - length), size - 1
        start_text, sep, end_text = spec.partition("-")
        if not sep:
            return 0, size - 1
        start = int(start_text)
        end = int(end_text) if end_text else size - 1
    except ValueError:
        return 0, size - 1
    if start < 0 or start >= size or end < start:
        return None, None
    return start, min(end, size - 1)


class Hallucinations:
    """The scanner's hallucination filter, as an editable list.

    whisper invents phrases on silence — "Thank you.", "Bye." — and the scanner
    drops any output line that matches this file exactly. Finding one of those
    is the main thing a person does while reading the archive, so the archive
    is where it should be possible to add it.

    One wrinkle worth knowing: the filter is applied to each *line* whisper
    emits, while a stored transcript is those lines joined with spaces. Adding a
    multi-line transcript verbatim would therefore never match anything, which
    is why the UI lets the phrase be edited before it is added rather than
    submitting the transcript blind.

    The scanner reloads this file every five minutes, so an addition takes
    effect on its own.
    """

    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.Lock()

    def phrases(self):
        try:
            return [line.strip() for line in
                    self.path.read_text(encoding="utf-8").splitlines()
                    if line.strip()]
        except FileNotFoundError:
            return []

    def add(self, phrase):
        """Append a phrase. Returns (added, count).

        `added` is False when the phrase is already present — worth saying, so
        the UI can report "already filtered" rather than claiming a change it
        did not make.
        """
        phrase = (phrase or "").strip()
        if not phrase:
            raise ValueError("phrase is empty")
        if "\n" in phrase or "\r" in phrase:
            # One phrase per line is the whole format; a newline would silently
            # add two entries, one of them probably nonsense.
            raise ValueError("phrase must be a single line")

        with self.lock:
            existing = self.phrases()
            if phrase in existing:
                return False, len(existing)

            updated = existing + [phrase]
            # Write a sibling and rename, so a crash mid-write cannot leave the
            # scanner reloading a truncated filter.
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text("\n".join(updated) + "\n", encoding="utf-8")
            os.replace(tmp, self.path)
            return True, len(updated)


class Library:
    """Read-only view of the store, for searching."""

    def __init__(self, db_path, clips_dir):
        self.db_path = str(db_path)
        self.clips_dir = Path(clips_dir).resolve()
        self._local = threading.local()

    def connect(self):
        """One connection per thread. sqlite3 connections are not safe to share
        across threads, and ThreadingHTTPServer hands each request its own."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            # mode=ro is the point: this process must never be able to write to
            # the scanner's database, whatever a handler gets wrong.
            uri = f"file:{urllib.parse.quote(self.db_path)}?mode=ro"
            conn = sqlite3.connect(uri, uri=True)
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return conn

    # -- search -------------------------------------------------------------
    def search(self, query="", stream=None, group=None, since=None, until=None,
               limit=DEFAULT_LIMIT, offset=0):
        """Transcripts matching the filters, newest first.

        `type = 'transcript.final'` is an indexed column, so the json_extract
        below only runs over transcripts rather than the whole log.
        """
        where = ["type = ?"]
        params = [events.TRANSCRIPT_FINAL]

        if query:
            where.append("json_extract(body, '$.data.text') LIKE ? ESCAPE '\\'")
            params.append(f"%{_like_escape(query)}%")
        if stream:
            where.append("stream = ?")
            params.append(stream)
        if group:
            where.append("group_name = ?")
            params.append(group)
        if since:
            where.append("ts >= ?")
            params.append(since)
        if until:
            where.append("ts <= ?")
            params.append(until)

        # One extra row tells us whether there is a next page without paying
        # for a second COUNT(*) scan over the same predicate.
        sql = f"""
            SELECT seq, id, ts, group_name, stream,
                   json_extract(body, '$.data.text')       AS text,
                   json_extract(body, '$.data.duration_s') AS duration_s,
                   json_extract(body, '$.data.model')      AS model,
                   json_extract(body, '$.data.audio.id')   AS clip_id
            FROM events
            WHERE {' AND '.join(where)}
            ORDER BY seq DESC
            LIMIT ? OFFSET ?
        """
        rows = self.connect().execute(sql, params + [limit + 1, offset]).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]

        results = [dict(row) for row in rows]
        self._attach_clips(results)
        return results, has_more

    def _attach_clips(self, results):
        """Fill in each result's clip state in one lookup by primary key.

        Joining on json_extract(body, ...) would defeat the clips index; the
        ids are already in hand, so ask for them directly.
        """
        wanted = {r["clip_id"] for r in results if r["clip_id"]}
        if not wanted:
            for result in results:
                result["clip"] = None
            return
        marks = ", ".join("?" * len(wanted))
        rows = self.connect().execute(
            f"SELECT id, bytes, duration_s, expires_at FROM clips WHERE id IN ({marks})",
            list(wanted)).fetchall()
        known = {row["id"]: dict(row) for row in rows}
        for result in results:
            clip_id = result["clip_id"]
            if not clip_id:
                result["clip"] = None
            elif clip_id in known:
                clip = known[clip_id]
                clip["expires_at"] = datetime.fromtimestamp(
                    clip["expires_at"], tz=timezone.utc).isoformat(
                        timespec="seconds").replace("+00:00", "Z")
                result["clip"] = clip
            else:
                # The transcript outlives its clip on purpose. Say so, rather
                # than rendering a play button that 404s.
                result["clip"] = {"id": clip_id, "expired": True}

    # -- facets -------------------------------------------------------------
    def streams(self):
        rows = self.connect().execute(
            "SELECT DISTINCT group_name, stream FROM events "
            "WHERE type = ? AND stream IS NOT NULL ORDER BY group_name, stream",
            (events.TRANSCRIPT_FINAL,)).fetchall()
        return [{"group": r["group_name"], "stream": r["stream"]} for r in rows]

    def stats(self):
        conn = self.connect()
        transcripts = conn.execute(
            "SELECT COUNT(*) FROM events WHERE type = ?",
            (events.TRANSCRIPT_FINAL,)).fetchone()[0]
        clips, clip_bytes = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(bytes), 0) FROM clips").fetchone()
        oldest = conn.execute(
            "SELECT MIN(ts) FROM events WHERE type = ?",
            (events.TRANSCRIPT_FINAL,)).fetchone()[0]
        return {"transcripts": transcripts, "clips": clips,
                "clip_bytes": clip_bytes, "oldest": oldest}

    # -- clips --------------------------------------------------------------
    def clip_path(self, clip_id):
        """The file for a clip id, or None.

        The path comes from the database and is then checked to be inside the
        clips directory. The id is never used to build a path, so a crafted id
        cannot escape the directory even if the row were somehow wrong.
        """
        if not CLIP_ID_RE.match(clip_id):
            return None
        row = self.connect().execute(
            "SELECT path FROM clips WHERE id = ?", (clip_id,)).fetchone()
        if row is None:
            return None
        path = Path(row["path"]).resolve()
        try:
            path.relative_to(self.clips_dir)
        except ValueError:
            print(f"[warn] clip {clip_id[:12]} resolves outside "
                  f"{self.clips_dir}; refusing to serve it")
            return None
        return path if path.exists() else None


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    server_version = "TentFloorAdmin/1.0"
    # Audio elements open several ranged requests per clip; on HTTP/1.0 each
    # one costs a fresh connection. Every response below sets Content-Length,
    # which is what keep-alive requires to find the end of a body.
    protocol_version = "HTTP/1.1"
    library = None          # set on the server instance
    hallucinations = None
    token = None
    read_only = False

    # -- helpers ------------------------------------------------------------
    def _authorised(self):
        """Only enforced when serving somewhere other than localhost."""
        if not self.token:
            return True
        header = self.headers.get("Authorization", "")
        supplied = header[7:] if header.startswith("Bearer ") else ""
        if not supplied:
            supplied = urllib.parse.parse_qs(
                urllib.parse.urlparse(self.path).query).get("token", [""])[0]
        # Constant-time: a token is only worth having if guessing it is not
        # cheaper than brute force.
        return secrets.compare_digest(supplied, self.token)

    def _send_json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        # This is an admin tool reading private radio traffic. Nothing about it
        # should end up in a browser cache or an embedding page.
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status, message):
        self._send_json({"error": message}, status=status)

    # -- routes -------------------------------------------------------------
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        route = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        if not self._authorised():
            self._send_error(401, "token required")
            return

        try:
            if route in ("/", "/index.html"):
                self._serve_page()
            elif route == "/api/search":
                self._serve_search(params)
            elif route == "/api/streams":
                self._send_json({"streams": self.library.streams()})
            elif route == "/api/stats":
                self._send_json(self.library.stats())
            elif route == "/api/hallucinations":
                self._send_json({"count": len(self.hallucinations.phrases()),
                                 "read_only": self.read_only})
            elif route.startswith("/api/clip/"):
                self._serve_clip(route[len("/api/clip/"):])
            else:
                self._send_error(404, "not found")
        except BrokenPipeError:
            pass        # the browser moved on mid-response; not our problem
        except Exception as e:
            print(f"[error] {route}: {type(e).__name__}: {e}")
            self._send_error(500, f"{type(e).__name__}: {e}")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if not self._authorised():
            self._send_error(401, "token required")
            return
        if not self._same_origin():
            # This listens on localhost, which every page the browser visits can
            # reach. A form post cannot set a custom header, and a cross-origin
            # fetch that sets one has to preflight -- which nothing here answers.
            self._send_error(403, "cross-origin request refused")
            return
        try:
            if parsed.path == "/api/hallucinations":
                self._add_hallucination()
            else:
                self._send_error(404, "not found")
        except BrokenPipeError:
            pass
        except Exception as e:
            print(f"[error] POST {parsed.path}: {type(e).__name__}: {e}")
            self._send_error(500, f"{type(e).__name__}: {e}")

    def _same_origin(self):
        if self.headers.get("X-Tent-Floor") != "1":
            return False
        origin = self.headers.get("Origin")
        if origin:
            host = self.headers.get("Host", "")
            if urllib.parse.urlparse(origin).netloc != host:
                return False
        return True

    def _body(self, limit=64 * 1024):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return {}
        if length <= 0 or length > limit:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return {}

    def _add_hallucination(self):
        if self.read_only:
            self._send_error(403, "server started with --read-only")
            return
        payload = self._body()
        if not isinstance(payload, dict):
            self._send_error(400, "expected a JSON object")
            return
        try:
            added, count = self.hallucinations.add(payload.get("phrase"))
        except ValueError as e:
            self._send_error(400, str(e))
            return
        except OSError as e:
            self._send_error(500, f"could not write the filter: {e}")
            return
        self._send_json({"added": added, "count": count,
                         "phrase": (payload.get("phrase") or "").strip()})

    def _serve_page(self):
        try:
            body = PAGE_PATH.read_bytes()
        except OSError as e:
            self._send_error(500, f"cannot read {PAGE_PATH}: {e}")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _serve_search(self, params):
        def one(name, default=None):
            value = params.get(name, [default])[0]
            return value.strip() if isinstance(value, str) else value

        try:
            limit = min(int(one("limit", str(DEFAULT_LIMIT))), MAX_LIMIT)
            offset = max(int(one("offset", "0")), 0)
        except ValueError:
            self._send_error(400, "limit and offset must be integers")
            return
        if limit < 1:
            self._send_error(400, "limit must be at least 1")
            return

        results, has_more = self.library.search(
            query=one("q", "") or "",
            stream=one("stream") or None,
            group=one("group") or None,
            since=one("since") or None,
            until=one("until") or None,
            limit=limit, offset=offset)
        self._send_json({"results": results, "has_more": has_more,
                         "limit": limit, "offset": offset})

    def _serve_clip(self, clip_id):
        path = self.library.clip_path(clip_id)
        if path is None:
            self._send_error(404, "no such clip")
            return

        size = path.stat().st_size
        start, end = self._range(size)
        if start is None:
            # Unsatisfiable range: say so properly, or the player retries forever.
            self.send_response(416)
            self.send_header("Content-Range", f"bytes */{size}")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        partial = (start, end) != (0, size - 1)
        length = end - start + 1
        self.send_response(206 if partial else 200)
        self.send_header("Content-Type", "audio/ogg; codecs=opus")
        self.send_header("Content-Length", str(length))
        # Seeking in an <audio> element needs byte ranges; without this some
        # browsers refuse to scrub and Safari will not play at all.
        self.send_header("Accept-Ranges", "bytes")
        if partial:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()

        with path.open("rb") as handle:
            handle.seek(start)
            remaining = length
            while remaining > 0:
                chunk = handle.read(min(64 * 1024, remaining))
                if not chunk:
                    break
                self.wfile.write(chunk)
                remaining -= len(chunk)

    def _range(self, size):
        return parse_range(self.headers.get("Range"), size)

    def log_message(self, fmt, *args):
        # The default logs every request to stderr, which buries the scanner's
        # own output when both share a terminal.
        if self.server.verbose:
            super().log_message(fmt, *args)


class AdminServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, handler, verbose=False):
        self.verbose = verbose
        super().__init__(address, handler)


def main():
    parser = argparse.ArgumentParser(
        description="Local web UI for searching transcripts and playing clips")
    parser.add_argument("--db", default="tentfloor.db", help="State store path")
    parser.add_argument("--clips", default="clips", help="Clips directory")
    parser.add_argument("--host", default=DEFAULT_HOST,
                        help=f"Bind address (default {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--hallucinations", default="hallucinations.txt",
                        help="Filter file the 'not speech' button appends to")
    parser.add_argument("--read-only", action="store_true",
                        help="Disable the hallucination button; search and "
                             "playback only")
    parser.add_argument("--token", help="Require this bearer token. Mandatory "
                                        "when binding off localhost.")
    parser.add_argument("--verbose", action="store_true", help="Log every request")
    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"[error] No state store at {args.db}. Start the scanner first, "
              f"or pass --db.")
        sys.exit(1)

    local = args.host in ("127.0.0.1", "::1", "localhost")
    if not local and not args.token:
        # This serves transcribed emergency radio and the audio behind it. It is
        # not something to put on a LAN by accident.
        print(f"[error] Refusing to bind {args.host} without --token. This "
              f"serves transcripts and audio with no access control of its own.")
        sys.exit(1)

    library = Library(args.db, args.clips)
    try:
        library.connect()
    except sqlite3.Error as e:
        print(f"[error] Cannot open {args.db} read-only: {e}")
        sys.exit(1)

    stats = library.stats()
    hallucinations = Hallucinations(args.hallucinations)
    Handler.library = library
    Handler.hallucinations = hallucinations
    Handler.token = args.token
    Handler.read_only = args.read_only

    server = AdminServer((args.host, args.port), Handler, verbose=args.verbose)
    print(f"[init] Tent Floor admin UI on http://{args.host}:{args.port}")
    print(f"[init] Store: {args.db} (read-only) · clips: {library.clips_dir}")
    print(f"[init] {stats['transcripts']} transcript(s), {stats['clips']} clip(s), "
          f"{stats['clip_bytes'] / 1e6:.1f} MB")
    if args.read_only:
        print("[init] Hallucination filter: read-only")
    else:
        print(f"[init] Hallucination filter: {hallucinations.path} "
              f"({len(hallucinations.phrases())} phrases, reloaded by the "
              f"scanner every 5 min)")
    if not local:
        print(f"[warn] Bound to {args.host} — reachable beyond this machine.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[exit] Shutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
