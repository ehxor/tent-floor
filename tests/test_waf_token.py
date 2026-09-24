"""PulsePoint's API went behind AWS WAF, and a token lasts about five minutes.

Everything here runs against a fake minter. The real one drives Chromium, and
a test suite that needs a browser and a live third-party service to pass is a
test suite that fails for reasons that have nothing to do with the code.

What is worth pinning: that one token is shared rather than minted per poller,
that the API's verdict beats the cached expiry, and that a broken minter
degrades to the pre-WAF behaviour instead of spinning.
"""

import json
import sys
import threading
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pulsepoint_poller as pp  # noqa: E402
import waf_token  # noqa: E402
from store import Store  # noqa: E402
from test_poller_health import FakeResponse  # noqa: E402


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


class CountingMinter:
    def __init__(self, token="tok-1", fail_with=None):
        self.calls = 0
        self.token = token
        self.fail_with = fail_with

    def __call__(self):
        self.calls += 1
        if self.fail_with is not None:
            raise waf_token.MintError(self.fail_with)
        return f"{self.token}-{self.calls}"


# ---------------------------------------------------------------------------
# TokenCache
# ---------------------------------------------------------------------------
class TokenCacheTests(unittest.TestCase):

    def cache(self, minter=None, store=None, clock=None):
        return waf_token.TokenCache(
            store=store, mint=minter or CountingMinter(),
            clock=clock or FakeClock())

    def test_mints_on_first_use(self):
        minter = CountingMinter()
        cache = self.cache(minter)

        self.assertEqual(cache.get(), "tok-1-1")
        self.assertEqual(minter.calls, 1)

    def test_reuses_a_fresh_token(self):
        """A mint launches a browser. Doing it per poll would mean one every
        30 seconds instead of one every four minutes."""
        minter = CountingMinter()
        clock = FakeClock()
        cache = self.cache(minter, clock=clock)

        cache.get()
        clock.advance(120)
        self.assertEqual(cache.get(), "tok-1-1")
        self.assertEqual(minter.calls, 1)

    def test_re_mints_before_expiry_not_after(self):
        """Refreshing a minute early means a slow mint does not leave a window
        where every poll is challenged."""
        minter = CountingMinter()
        clock = FakeClock()
        cache = self.cache(minter, clock=clock)
        cache.get()

        clock.advance(239)                    # 300s ttl, 60s margin
        self.assertEqual(minter.calls, 1)
        clock.advance(2)                      # now inside the margin
        self.assertEqual(cache.get(), "tok-1-2")
        self.assertEqual(minter.calls, 2)

    def test_invalidate_forces_a_fresh_mint(self):
        """The API's rejection is the authority, not our guess at the TTL."""
        minter = CountingMinter()
        cache = self.cache(minter)
        cache.get()

        cache.invalidate()
        self.assertEqual(cache.get(), "tok-1-2")
        self.assertEqual(minter.calls, 2)

    def test_a_failing_minter_returns_none_and_records_why(self):
        """Without playwright installed this is the normal state, and it has
        to degrade to the pre-WAF behaviour rather than raise into the poller."""
        cache = self.cache(CountingMinter(fail_with="playwright is not installed"))

        self.assertIsNone(cache.get())
        self.assertIn("playwright", cache.last_error)

    def test_a_failed_re_mint_keeps_the_old_token(self):
        """It is probably stale, but the API decides that, and one wasted
        request beats sending nothing at all."""
        minter = CountingMinter()
        clock = FakeClock()
        cache = self.cache(minter, clock=clock)
        cache.get()

        minter.fail_with = "chromium crashed"
        clock.advance(400)
        self.assertEqual(cache.get(), "tok-1-1")
        self.assertIn("chromium", cache.last_error)

    def test_one_mint_under_concurrent_pollers(self):
        """The shipped config runs two PulsePoint pollers on one agency. They
        share a cache; if they did not serialise, every refresh would launch
        two browsers."""
        started = threading.Barrier(8)
        minter = CountingMinter()

        def slow_mint():
            import time as real_time
            real_time.sleep(0.05)
            return minter()

        cache = self.cache(slow_mint)
        tokens = []

        def worker():
            started.wait()
            tokens.append(cache.get())

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(minter.calls, 1)
        self.assertEqual(len(set(tokens)), 1)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
class TokenPersistenceTests(unittest.TestCase):

    def test_a_restart_reuses_a_token_with_life_left(self):
        """Five minutes is short enough that a restart mid-window should not
        pay for a browser launch before its first poll."""
        store = Store(":memory:")
        self.addCleanup(store.close)
        clock = FakeClock()
        minter = CountingMinter()

        first = waf_token.TokenCache(store=store, mint=minter, clock=clock)
        self.assertEqual(first.get(), "tok-1-1")

        clock.advance(30)
        second = waf_token.TokenCache(store=store, mint=minter, clock=clock)
        self.assertEqual(second.get(), "tok-1-1")
        self.assertEqual(minter.calls, 1)

    def test_a_restart_after_expiry_mints_again(self):
        store = Store(":memory:")
        self.addCleanup(store.close)
        clock = FakeClock()
        minter = CountingMinter()

        waf_token.TokenCache(store=store, mint=minter, clock=clock).get()
        clock.advance(600)

        revived = waf_token.TokenCache(store=store, mint=minter, clock=clock)
        self.assertEqual(revived.get(), "tok-1-2")

    def test_invalidate_clears_the_persisted_copy(self):
        """Otherwise the next restart would load a token the API has already
        rejected and spend a poll finding out again."""
        store = Store(":memory:")
        self.addCleanup(store.close)
        minter = CountingMinter()

        cache = waf_token.TokenCache(store=store, mint=minter, clock=FakeClock())
        cache.get()
        cache.invalidate()

        self.assertIsNone(store.kv.get(waf_token.KV_NAMESPACE, waf_token.KV_KEY))

    def test_a_store_without_the_token_survives(self):
        """No store is a supported mode (--no-store); it must not crash."""
        cache = waf_token.TokenCache(store=None, mint=CountingMinter(),
                                     clock=FakeClock())
        self.assertEqual(cache.get(), "tok-1-1")


# ---------------------------------------------------------------------------
# The token on the wire
# ---------------------------------------------------------------------------
class FetchWithTokenTests(unittest.TestCase):

    def _capture(self, token):
        seen = {}
        payload = {"incidents": {"active": [], "recent": []}}

        def urlopen(req, *a, **kw):
            seen["headers"] = dict(req.headers)
            return FakeResponse(json.dumps({"ct": "x"}).encode(), status=200)

        with mock.patch.object(pp.urllib.request, "urlopen", urlopen), \
                mock.patch.object(pp, "decrypt_pulsepoint", return_value=payload):
            pp.fetch_incidents("EMS1201", token=token)
        return seen["headers"]

    def test_token_is_sent_as_a_header(self):
        """Measured on 2026-09-24: the API takes the header as well as the
        cookie, and a header needs no cookie jar."""
        headers = self._capture("tok-abc")

        # urllib title-cases header names.
        self.assertEqual(headers.get("X-aws-waf-token"), "tok-abc")

    def test_no_token_sends_no_header(self):
        headers = self._capture(None)

        self.assertNotIn("X-aws-waf-token", headers)


# ---------------------------------------------------------------------------
# Poller integration
# ---------------------------------------------------------------------------
class PollerTokenTests(unittest.TestCase):

    def setUp(self):
        self.store = Store(":memory:")
        self.addCleanup(self.store.close)

    def _poller(self, minter):
        cache = waf_token.TokenCache(store=None, mint=minter, clock=FakeClock())
        return cache, pp.PulsePointPoller(
            agency_id="EMS1201", store=self.store, scope="test",
            group="vancouver-island", tokens=cache)

    def test_a_challenge_invalidates_and_retries_once(self):
        """A token can age out between two polls. One retry with a fresh token
        turns that into a non-event instead of a gap in the feed."""
        minter = CountingMinter()
        cache, poller = self._poller(minter)
        challenged = FakeResponse(b"", status=202,
                                  headers={"x-amzn-waf-action": "challenge"})
        good = FakeResponse(json.dumps({"ct": "x"}).encode(), status=200)
        payload = {"incidents": {"active": [], "recent": []}}

        with mock.patch.object(pp.urllib.request, "urlopen",
                               side_effect=[challenged, good]), \
                mock.patch.object(pp, "decrypt_pulsepoint", return_value=payload):
            result = poller._fetch()

        self.assertTrue(result.ok)
        self.assertEqual(minter.calls, 2)        # initial, then after invalidate

    def test_a_persistent_challenge_gives_up_after_one_retry(self):
        """If a fresh token is also refused, something else is wrong. Minting
        in a loop would launch a browser every 30s and never recover."""
        minter = CountingMinter()
        cache, poller = self._poller(minter)
        challenged = FakeResponse(b"", status=202,
                                  headers={"x-amzn-waf-action": "challenge"})

        with mock.patch.object(pp.urllib.request, "urlopen",
                               return_value=challenged):
            result = poller._fetch()

        self.assertFalse(result.ok)
        self.assertEqual(result.error.kind, "waf_challenge")
        self.assertEqual(minter.calls, 2)

    def test_a_non_waf_failure_does_not_burn_a_token(self):
        """A 503 says nothing about the token. Re-minting on one would launch
        a browser every time PulsePoint has a bad minute."""
        minter = CountingMinter()
        cache, poller = self._poller(minter)
        err = urllib.error.HTTPError("url", 503, "Service Unavailable", {}, None)

        with mock.patch.object(pp.urllib.request, "urlopen", side_effect=err):
            result = poller._fetch()

        self.assertEqual(result.error.kind, "http_error")
        self.assertEqual(minter.calls, 1)

    def test_a_broken_minter_reports_why_in_the_health_event(self):
        """"challenge" alone would send an operator looking at the network.
        The actionable fact is that the browser side is broken."""
        minter = CountingMinter(fail_with="playwright is not installed")
        cache, poller = self._poller(minter)
        challenged = FakeResponse(b"", status=202,
                                  headers={"x-amzn-waf-action": "challenge"})

        def urlopen(*a, **kw):
            poller.stop_event.set()
            return challenged

        with mock.patch.object(pp.urllib.request, "urlopen", urlopen), \
                mock.patch.object(sys, "stderr", mock.MagicMock()):
            poller._poll_loop()

        rows = self.store.events.recent(types=("poller.error",))
        self.assertEqual(len(rows), 1)
        self.assertIn("playwright", rows[0][1]["data"]["detail"])

    def test_no_token_cache_behaves_as_before(self):
        """--no-store, or playwright absent: the poller still runs and still
        reports the challenge. The WAF work must not become mandatory."""
        poller = pp.PulsePointPoller(agency_id="EMS1201", store=self.store,
                                     scope="test", group="vancouver-island")
        challenged = FakeResponse(b"", status=202,
                                  headers={"x-amzn-waf-action": "challenge"})

        with mock.patch.object(pp.urllib.request, "urlopen",
                               return_value=challenged):
            result = poller._fetch()

        self.assertEqual(result.error.kind, "waf_challenge")


# ---------------------------------------------------------------------------
# The minter's own failure reporting
# ---------------------------------------------------------------------------
class MintErrorTests(unittest.TestCase):

    def test_missing_playwright_is_a_mint_error_with_the_install_line(self):
        """The most likely failure on a fresh box, and the message should be
        the fix rather than a traceback."""
        with mock.patch.dict(sys.modules, {"playwright.async_api": None}):
            with self.assertRaises(waf_token.MintError) as caught:
                waf_token.mint_token()

        self.assertIn("playwright install chromium", str(caught.exception))

    def test_the_sync_api_is_not_used(self):
        """Playwright's sync API drives its event loop through greenlet, which
        segfaults on a free-threaded build -- exit 139, no traceback, no
        output, which is a miserable thing to debug. Pin the async import so a
        tidy-up cannot quietly reintroduce it."""
        source = (Path(waf_token.__file__)).read_text()

        self.assertIn("playwright.async_api", source)
        # Not "sync_playwright": "async_playwright" contains it.
        self.assertNotIn("playwright.sync_api", source)

    def test_subprocess_failure_surfaces_the_child_message(self):
        done = mock.Mock(returncode=1, stdout="", stderr="could not launch chromium")
        with mock.patch.object(waf_token.subprocess, "run", return_value=done):
            with self.assertRaises(waf_token.MintError) as caught:
                waf_token._mint_via_subprocess()

        self.assertIn("chromium", str(caught.exception))

    def test_subprocess_timeout_is_reported_not_hung(self):
        """A wedged browser must not take the poller thread with it."""
        import subprocess as sp
        with mock.patch.object(waf_token.subprocess, "run",
                               side_effect=sp.TimeoutExpired("cmd", 75)):
            with self.assertRaises(waf_token.MintError) as caught:
                waf_token._mint_via_subprocess()

        self.assertIn("did not exit", str(caught.exception))

    def test_subprocess_success_returns_the_token(self):
        done = mock.Mock(returncode=0, stdout=json.dumps({"token": "tok-xyz"}),
                         stderr="")
        with mock.patch.object(waf_token.subprocess, "run", return_value=done):
            self.assertEqual(waf_token._mint_via_subprocess(), "tok-xyz")


if __name__ == "__main__":
    unittest.main()
