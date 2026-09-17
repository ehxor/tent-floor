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


class ServerFixture:
    """Starts a real server on an ephemeral port.

    Deliberately not a TestCase: subclassing one would re-run every inherited
    test in each subclass, which is a slow way to test the same thing twice.
    """

    read_only = False

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name)
        cls.db = root / "tentfloor.db"
        cls.clips_dir = root / "clips"
        seed(cls.db, cls.clips_dir)

        cls.filter_path = root / "hallucinations.txt"
        cls.filter_path.write_text("Thank you.\nBye.\n", encoding="utf-8")
        admin_ui.Handler.library = admin_ui.Library(cls.db, cls.clips_dir)
        admin_ui.Handler.hallucinations = admin_ui.Hallucinations(cls.filter_path)
        admin_ui.Handler.token = None
        admin_ui.Handler.read_only = cls.read_only
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
        admin_ui.Handler.hallucinations = None
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

    def post(self, path, payload, headers=None):
        merged = {"Content-Type": "application/json", "X-Tent-Floor": "1"}
        merged.update(headers or {})
        # A None value means "send this request without that header", which is
        # how the CSRF tests reproduce what a plain <form> post can do.
        merged = {k: v for k, v in merged.items() if v is not None}
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers=merged, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return e.code, json.loads(raw)
            except ValueError:
                return e.code, {}


class Routes(ServerFixture, unittest.TestCase):
    """Byte ranges, content types and status codes are exactly what a mocked
    handler would hide, so this drives a live server."""

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
        admin_ui.Handler.hallucinations = admin_ui.Hallucinations(root / "h.txt")
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


class HallucinationFile(unittest.TestCase):
    """The one thing this tool writes. The store stays read-only; this is a
    plain text file the scanner reloads every five minutes."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "hallucinations.txt"
        self.path.write_text("Thank you.\nBye.\n", encoding="utf-8")
        self.filter = admin_ui.Hallucinations(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    def test_existing_phrases_are_read(self):
        self.assertEqual(self.filter.phrases(), ["Thank you.", "Bye."])

    def test_a_missing_file_reads_as_empty(self):
        missing = admin_ui.Hallucinations(Path(self.tmp.name) / "nope.txt")
        self.assertEqual(missing.phrases(), [])

    def test_adding_appends_and_preserves_what_was_there(self):
        added, count = self.filter.add("Thanks for watching!")
        self.assertTrue(added)
        self.assertEqual(count, 3)
        self.assertEqual(self.filter.phrases(),
                         ["Thank you.", "Bye.", "Thanks for watching!"])

    def test_the_file_keeps_one_phrase_per_line_and_a_trailing_newline(self):
        self.filter.add("Okay then.")
        raw = self.path.read_text(encoding="utf-8")
        self.assertTrue(raw.endswith("\n"))
        self.assertEqual(raw.count("\n"), 3)

    def test_adding_a_duplicate_reports_no_change(self):
        added, count = self.filter.add("Thank you.")
        self.assertFalse(added)
        self.assertEqual(count, 2)
        self.assertEqual(self.filter.phrases().count("Thank you."), 1)

    def test_surrounding_whitespace_is_trimmed_before_comparing(self):
        added, _ = self.filter.add("   Thank you.   ")
        self.assertFalse(added, "it is the same phrase")

    def test_an_empty_phrase_is_refused(self):
        for bad in ("", "   ", None):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    self.filter.add(bad)

    def test_a_multi_line_phrase_is_refused(self):
        """One phrase per line is the whole format — a newline would silently
        add two entries, one of them probably nonsense."""
        with self.assertRaises(ValueError):
            self.filter.add("one\ntwo")
        with self.assertRaises(ValueError):
            self.filter.add("one\rtwo")

    def test_a_refused_add_does_not_touch_the_file(self):
        before = self.path.read_text(encoding="utf-8")
        with self.assertRaises(ValueError):
            self.filter.add("")
        self.assertEqual(self.path.read_text(encoding="utf-8"), before)

    def test_the_write_leaves_no_temp_file_behind(self):
        self.filter.add("Okay then.")
        leftovers = [p.name for p in Path(self.tmp.name).iterdir()
                     if p.name != "hallucinations.txt"]
        self.assertEqual(leftovers, [])

    def test_the_file_is_created_if_it_does_not_exist(self):
        fresh = admin_ui.Hallucinations(Path(self.tmp.name) / "new" / "h.txt")
        added, count = fresh.add("Something.")
        self.assertTrue(added)
        self.assertEqual(count, 1)
        self.assertEqual(fresh.phrases(), ["Something."])

    def test_concurrent_adds_do_not_lose_phrases(self):
        """Read-modify-write under a lock: without it two threads racing lose
        one of the additions."""
        def add(n):
            self.filter.add(f"phrase {n}")
        threads = [threading.Thread(target=add, args=(n,)) for n in range(12)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        phrases = self.filter.phrases()
        self.assertEqual(len(phrases), 14)
        self.assertEqual(len(set(phrases)), 14)


class HallucinationEndpoint(ServerFixture, unittest.TestCase):

    def current(self):
        _, _, body = self.get("/api/hallucinations")
        return json.loads(body)

    def test_the_count_and_mode_are_reported(self):
        data = self.current()
        self.assertGreaterEqual(data["count"], 2)
        self.assertFalse(data["read_only"])

    def test_a_phrase_can_be_added(self):
        before = self.current()["count"]
        status, data = self.post("/api/hallucinations",
                                 {"phrase": "Please subscribe."})
        self.assertEqual(status, 200)
        self.assertTrue(data["added"])
        self.assertEqual(data["count"], before + 1)
        self.assertIn("Please subscribe.",
                      self.filter_path.read_text(encoding="utf-8"))

    def test_a_duplicate_is_reported_rather_than_claimed(self):
        self.post("/api/hallucinations", {"phrase": "Duplicate me."})
        status, data = self.post("/api/hallucinations", {"phrase": "Duplicate me."})
        self.assertEqual(status, 200)
        self.assertFalse(data["added"],
                         "the UI should say 'already filtered', not claim a change")

    def test_an_empty_phrase_is_400(self):
        status, _ = self.post("/api/hallucinations", {"phrase": "  "})
        self.assertEqual(status, 400)

    def test_a_multi_line_phrase_is_400(self):
        status, _ = self.post("/api/hallucinations", {"phrase": "a\nb"})
        self.assertEqual(status, 400)

    def test_a_missing_phrase_key_is_400(self):
        status, _ = self.post("/api/hallucinations", {})
        self.assertEqual(status, 400)

    def test_an_unknown_post_route_is_404(self):
        status, _ = self.post("/api/nope", {"phrase": "x"})
        self.assertEqual(status, 404)

    # -- CSRF ---------------------------------------------------------------
    def test_a_post_without_the_custom_header_is_refused(self):
        """This listens on localhost, which any page the browser visits can
        reach. A <form> post cannot set a custom header."""
        status, _ = self.post("/api/hallucinations", {"phrase": "csrf"},
                              headers={"X-Tent-Floor": None})
        self.assertEqual(status, 403)

    def test_a_foreign_origin_is_refused(self):
        status, _ = self.post("/api/hallucinations", {"phrase": "csrf"},
                              headers={"Origin": "http://evil.example"})
        self.assertEqual(status, 403)

    def test_a_matching_origin_is_allowed(self):
        status, _ = self.post(
            "/api/hallucinations", {"phrase": "Same origin is fine."},
            headers={"Origin": f"http://127.0.0.1:{self.port}"})
        self.assertEqual(status, 200)

    def test_a_refused_post_does_not_write(self):
        before = self.filter_path.read_text(encoding="utf-8")
        self.post("/api/hallucinations", {"phrase": "csrf"},
                  headers={"X-Tent-Floor": None})
        self.assertEqual(self.filter_path.read_text(encoding="utf-8"), before)


class ReadOnlyMode(ServerFixture, unittest.TestCase):
    """--read-only leaves search and playback working but refuses the button."""

    read_only = True

    def test_the_mode_is_advertised_so_the_button_can_hide(self):
        _, _, body = self.get("/api/hallucinations")
        self.assertTrue(json.loads(body)["read_only"])

    def test_adding_is_refused(self):
        before = self.filter_path.read_text(encoding="utf-8")
        status, data = self.post("/api/hallucinations", {"phrase": "nope"})
        self.assertEqual(status, 403)
        self.assertEqual(self.filter_path.read_text(encoding="utf-8"), before)

    def test_search_still_works(self):
        _, _, body = self.get("/api/search?q=engine")
        self.assertGreater(len(json.loads(body)["results"]), 0)

    def test_playback_still_works(self):
        status, _, _ = self.get(f"/api/clip/{self.clip_id()}")
        self.assertEqual(status, 200)


if __name__ == "__main__":
    unittest.main()
