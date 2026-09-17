"""The admin UI reads the scanner's live database and serves audio off disk, so
the tests that matter are about not breaking either: the connection must be
read-only, and a clip id from a URL must never become a filesystem path.

The HTTP tests run a real server on an ephemeral port rather than mocking the
handler, because the behaviour worth checking — byte ranges, content types,
status codes — is exactly what a mock would paper over.
"""

import json
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import admin_ui
import clips
import events
from store import Store


def seed(db_path, clips_dir):
    """A store with a handful of transcripts, one whose clip has been removed."""
    store = Store(str(db_path))
    clip_store = clips.ClipStore(
        store, directory=clips_dir, retention_days=7,
        encoder=lambda pcm, dst: Path(dst).write_bytes(b"OggS" + bytes(len(pcm) % 97)))
    rows = [
        ("Mid Island", "vancouver-island", "Engine three responding to Bowen Road"),
        ("Mid Island", "vancouver-island", "Structure fire confirmed"),
        ("Cowichan Valley", "vancouver-island", "Rescue one en route code 3"),
        ("Kamloops", "interior", "Engine seven clear of the scene"),
        ("Mid Island", "vancouver-island", "Dispatch, show us at 50% containment"),
        ("Mid Island", "vancouver-island", "Unit 3_7 acknowledging"),
    ]
    for index, (stream, group, text) in enumerate(rows):
        pcm = bytes([(index * 31 + n) % 256 for n in range(3200)])
        clip = clip_store.write(pcm, group, stream, duration_s=2.0)
        store.events.append(events.transcript(group, stream, text, 2.0,
                                              model="large-v3",
                                              audio=events.audio_ref(clip)))
    # A transcript whose clip has since been swept.
    orphaned = clip_store.write(b"\x07" * 2000, "vancouver-island", "Mid Island", 1.0)
    store.events.append(events.transcript("vancouver-island", "Mid Island",
                                          "Old call, audio gone", 1.0,
                                          audio=events.audio_ref(orphaned)))
    clip_store.delete(orphaned["id"])
    # A transcript that never had audio at all.
    store.events.append(events.transcript("vancouver-island", "Mid Island",
                                          "No clip was kept here", 1.0))
    store.close()


class LibraryBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.db = root / "tentfloor.db"
        self.clips_dir = root / "clips"
        seed(self.db, self.clips_dir)
        self.library = admin_ui.Library(self.db, self.clips_dir)

    def tearDown(self):
        self.tmp.cleanup()

    def texts(self, **kwargs):
        results, _ = self.library.search(**kwargs)
        return [r["text"] for r in results]


class ReadOnly(LibraryBase):
    def test_the_connection_cannot_write(self):
        """The scanner owns this database. The admin UI must not be able to
        damage it however wrong a handler goes."""
        conn = self.library.connect()
        with self.assertRaises(sqlite3.OperationalError) as caught:
            conn.execute("DELETE FROM events")
        self.assertIn("readonly", str(caught.exception).lower())

    def test_reads_still_work(self):
        self.assertGreater(self.library.stats()["transcripts"], 0)

    def test_each_thread_gets_its_own_connection(self):
        seen = []
        def grab():
            seen.append(id(self.library.connect()))
        threads = [threading.Thread(target=grab) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(set(seen)), 4,
                         "sqlite connections are not safe to share across threads")


class Search(LibraryBase):
    def test_a_plain_query_matches_case_insensitively(self):
        self.assertIn("Engine three responding to Bowen Road", self.texts(query="engine"))
        self.assertIn("Engine seven clear of the scene", self.texts(query="ENGINE"))

    def test_results_are_newest_first(self):
        results, _ = self.library.search()
        seqs = [r["seq"] for r in results]
        self.assertEqual(seqs, sorted(seqs, reverse=True))

    def test_a_percent_in_the_query_is_a_literal(self):
        """Otherwise searching for "50%" quietly matches everything, which
        reads as the search being broken."""
        self.assertEqual(self.texts(query="50%"),
                         ["Dispatch, show us at 50% containment"])

    def test_a_bare_wildcard_matches_nothing(self):
        self.assertEqual(self.texts(query="%%%"), [])

    def test_an_underscore_is_a_literal_too(self):
        self.assertEqual(self.texts(query="3_7"), ["Unit 3_7 acknowledging"])

    def test_stream_and_group_filters(self):
        self.assertEqual(self.texts(stream="Kamloops"),
                         ["Engine seven clear of the scene"])
        for text in self.texts(group="interior"):
            self.assertIn("Engine seven", text)

    def test_date_filters_bound_the_range(self):
        self.assertEqual(self.texts(since="2999-01-01T00:00:00Z"), [])
        self.assertNotEqual(self.texts(since="2000-01-01T00:00:00Z"), [])

    def test_paging_reports_more_without_a_second_count(self):
        first, has_more = self.library.search(limit=3)
        self.assertEqual(len(first), 3)
        self.assertTrue(has_more)
        rest, has_more = self.library.search(limit=100, offset=3)
        self.assertFalse(has_more)
        self.assertEqual(len({r["seq"] for r in first} & {r["seq"] for r in rest}), 0,
                         "pages must not overlap")

    def test_only_transcripts_are_returned(self):
        store = Store(str(self.db))
        store.events.append(events.tone_page(
            "vancouver-island", "Mid Island",
            {"tone_a": 1.0, "tone_b": 2.0, "key": "1.0/2.0", "unit": "X"}))
        store.close()
        results, _ = self.library.search()
        self.assertTrue(all(r["text"] is not None for r in results))


class ClipAttachment(LibraryBase):
    def one(self, query):
        results, _ = self.library.search(query=query)
        self.assertEqual(len(results), 1, f"expected exactly one hit for {query!r}")
        return results[0]

    def test_a_live_clip_carries_its_size_and_expiry(self):
        clip = self.one("Bowen")["clip"]
        self.assertFalse(clip.get("expired"))
        self.assertGreater(clip["bytes"], 0)
        self.assertTrue(clip["expires_at"].endswith("Z"))

    def test_a_swept_clip_is_reported_as_expired(self):
        """The transcript outlives its clip by design, so the UI has to be able
        to say so rather than render a play button that 404s."""
        clip = self.one("audio gone")["clip"]
        self.assertTrue(clip["expired"])

    def test_a_transcript_with_no_audio_has_no_clip(self):
        self.assertIsNone(self.one("No clip was kept")["clip"])


class ClipPaths(LibraryBase):
    def known_id(self):
        results, _ = self.library.search(query="Bowen")
        return results[0]["clip"]["id"]

    def test_a_known_clip_resolves_to_a_file(self):
        path = self.library.clip_path(self.known_id())
        self.assertIsNotNone(path)
        self.assertTrue(path.exists())

    def test_a_non_hex_id_is_refused_before_touching_disk(self):
        for bad in ("../../etc/passwd", "zzz", "", "/etc/passwd", "a" * 63):
            with self.subTest(bad=bad):
                self.assertIsNone(self.library.clip_path(bad))

    def test_an_unknown_but_well_formed_id_is_none(self):
        self.assertIsNone(self.library.clip_path("a" * 64))

    def test_a_row_pointing_outside_the_clips_dir_is_refused(self):
        """Defence in depth: the id is never used to build a path, but a row
        with a path outside the directory must not be served either."""
        outside = Path(self.tmp.name) / "escaped.opus"
        outside.write_bytes(b"OggS")
        conn = sqlite3.connect(str(self.db))
        conn.execute("UPDATE clips SET path = ? WHERE id = ?",
                     (str(outside), self.known_id()))
        conn.commit()
        conn.close()
        self.assertIsNone(self.library.clip_path(self.known_id()))


class RangeParsing(unittest.TestCase):
    def test_no_header_is_the_whole_file(self):
        self.assertEqual(admin_ui.parse_range(None, 100), (0, 99))
        self.assertEqual(admin_ui.parse_range("", 100), (0, 99))

    def test_an_explicit_range(self):
        self.assertEqual(admin_ui.parse_range("bytes=10-19", 100), (10, 19))

    def test_an_open_ended_range(self):
        self.assertEqual(admin_ui.parse_range("bytes=50-", 100), (50, 99))

    def test_a_suffix_range(self):
        self.assertEqual(admin_ui.parse_range("bytes=-20", 100), (80, 99))

    def test_a_suffix_longer_than_the_file_clamps(self):
        self.assertEqual(admin_ui.parse_range("bytes=-500", 100), (0, 99))

    def test_an_end_past_the_file_clamps(self):
        self.assertEqual(admin_ui.parse_range("bytes=90-500", 100), (90, 99))

    def test_a_start_past_the_end_is_unsatisfiable(self):
        self.assertEqual(admin_ui.parse_range("bytes=100-", 100), (None, None))
        self.assertEqual(admin_ui.parse_range("bytes=500-600", 100), (None, None))

    def test_a_backwards_range_is_unsatisfiable(self):
        self.assertEqual(admin_ui.parse_range("bytes=50-10", 100), (None, None))

    def test_a_zero_length_suffix_is_unsatisfiable(self):
        self.assertEqual(admin_ui.parse_range("bytes=-0", 100), (None, None))

    def test_garbage_falls_back_to_the_whole_file(self):
        """A malformed Range is not worth failing a request over; a range past
        the end is, or the player retries forever."""
        for header in ("bytes=abc", "bytes=", "chunks=0-10", "bytes=1-2-3"):
            with self.subTest(header=header):
                self.assertEqual(admin_ui.parse_range(header, 100), (0, 99))

    def test_only_the_first_range_of_a_set_is_honoured(self):
        self.assertEqual(admin_ui.parse_range("bytes=0-9,20-29", 100), (0, 9))


class HttpServer(unittest.TestCase):
    """A real server on an ephemeral port — byte ranges and status codes are
    exactly what a mocked handler would hide."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name)
        cls.db = root / "tentfloor.db"
        cls.clips_dir = root / "clips"
        seed(cls.db, cls.clips_dir)

        admin_ui.Handler.library = admin_ui.Library(cls.db, cls.clips_dir)
        admin_ui.Handler.token = None
        cls.server = admin_ui.AdminServer(("127.0.0.1", 0), admin_ui.Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)
        admin_ui.Handler.library = None
        cls.tmp.cleanup()

    def get(self, path, headers=None):
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, dict(response.headers), response.read()
        except urllib.error.HTTPError as e:
            return e.code, dict(e.headers), e.read()

    def clip_id(self):
        _, _, body = self.get("/api/search?q=Bowen")
        return json.loads(body)["results"][0]["clip"]["id"]

    # -- routes -------------------------------------------------------------
    def test_the_page_is_served(self):
        status, headers, body = self.get("/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", headers["Content-Type"])
        self.assertIn(b"Tent Floor", body)

    def test_search_returns_json(self):
        status, headers, body = self.get("/api/search?q=engine")
        self.assertEqual(status, 200)
        self.assertIn("application/json", headers["Content-Type"])
        payload = json.loads(body)
        self.assertGreater(len(payload["results"]), 0)
        for result in payload["results"]:
            self.assertIn("engine", result["text"].lower())

    def test_streams_and_stats(self):
        _, _, body = self.get("/api/streams")
        names = {s["stream"] for s in json.loads(body)["streams"]}
        self.assertIn("Mid Island", names)
        _, _, body = self.get("/api/stats")
        self.assertGreater(json.loads(body)["transcripts"], 0)

    def test_an_unknown_route_is_404(self):
        status, _, _ = self.get("/nope")
        self.assertEqual(status, 404)

    def test_a_bad_limit_is_rejected(self):
        for query in ("limit=abc", "limit=0", "offset=xyz"):
            with self.subTest(query=query):
                status, _, _ = self.get(f"/api/search?{query}")
                self.assertEqual(status, 400)

    def test_the_limit_is_capped(self):
        _, _, body = self.get("/api/search?limit=99999")
        self.assertLessEqual(json.loads(body)["limit"], admin_ui.MAX_LIMIT)

    # -- clips --------------------------------------------------------------
    def test_a_clip_is_served_as_ogg_opus(self):
        status, headers, body = self.get(f"/api/clip/{self.clip_id()}")
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "audio/ogg; codecs=opus")
        self.assertEqual(headers["Accept-Ranges"], "bytes")
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(int(headers["Content-Length"]), len(body))

    def test_a_ranged_request_returns_206(self):
        clip = self.clip_id()
        _, headers, whole = self.get(f"/api/clip/{clip}")
        size = len(whole)
        status, headers, body = self.get(f"/api/clip/{clip}",
                                         {"Range": "bytes=0-9"})
        self.assertEqual(status, 206)
        self.assertEqual(headers["Content-Range"], f"bytes 0-9/{size}")
        self.assertEqual(body, whole[:10])

    def test_a_suffix_range_returns_the_tail(self):
        clip = self.clip_id()
        _, _, whole = self.get(f"/api/clip/{clip}")
        status, _, body = self.get(f"/api/clip/{clip}", {"Range": "bytes=-5"})
        self.assertEqual(status, 206)
        self.assertEqual(body, whole[-5:])

    def test_an_unsatisfiable_range_is_416_with_a_length(self):
        """Content-Length matters: on HTTP/1.1 a body-less response without one
        leaves the connection hanging."""
        status, headers, _ = self.get(f"/api/clip/{self.clip_id()}",
                                      {"Range": "bytes=999999-"})
        self.assertEqual(status, 416)
        self.assertIn("Content-Range", headers)
        self.assertEqual(headers["Content-Length"], "0")

    def test_a_traversal_attempt_is_404(self):
        for bad in ("..%2F..%2Fetc%2Fpasswd", "zzz", "a" * 64):
            with self.subTest(bad=bad):
                status, _, _ = self.get(f"/api/clip/{bad}")
                self.assertEqual(status, 404)

    def test_clip_responses_are_not_cached(self):
        _, headers, _ = self.get(f"/api/clip/{self.clip_id()}")
        self.assertEqual(headers["Cache-Control"], "no-store")


class TokenAuth(unittest.TestCase):
    """Only enforced off localhost, but the mechanism has to work."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name)
        cls.db = root / "t.db"
        seed(cls.db, root / "clips")
        admin_ui.Handler.library = admin_ui.Library(cls.db, root / "clips")
        admin_ui.Handler.token = "sekrit"
        cls.server = admin_ui.AdminServer(("127.0.0.1", 0), admin_ui.Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)
        admin_ui.Handler.token = None
        admin_ui.Handler.library = None
        cls.tmp.cleanup()

    def get(self, path, headers=None):
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status
        except urllib.error.HTTPError as e:
            e.read()
            return e.code

    def test_no_token_is_401(self):
        self.assertEqual(self.get("/api/stats"), 401)

    def test_a_wrong_token_is_401(self):
        self.assertEqual(self.get("/api/stats", {"Authorization": "Bearer nope"}), 401)

    def test_the_right_token_in_a_header_works(self):
        self.assertEqual(self.get("/api/stats", {"Authorization": "Bearer sekrit"}), 200)

    def test_the_right_token_in_the_query_works(self):
        """An <audio src> cannot carry an Authorization header."""
        self.assertEqual(self.get("/api/stats?token=sekrit"), 200)


if __name__ == "__main__":
    unittest.main()
