"""
waf_token.py — mint and cache the AWS WAF token the PulsePoint API requires.

In September 2026 PulsePoint put api.pulsepoint.org/v1/webapp behind AWS WAF.
Every request without a token comes back `202` with an empty body and
`x-amzn-waf-action: challenge`, which is what the poller had been reporting as
"no data returned (try again)" for eight days. See pulsepoint_poller.py.

What the token is, and what it is not. web.pulsepoint.org loads AWS's WAF
JavaScript SDK in its <head>; the SDK solves a challenge and hands the page a
token, which it then attaches to the app's API calls. The SDK's `getToken()`
is a documented public API and this module calls it in a real browser. Nothing
here reimplements or forges the challenge -- if the browser cannot solve it,
this module reports a failure like any other.

Measured against the live API on 2026-09-24:

  * a token minted in one browser is accepted from a different host, a
    different IP and a different TLS stack, so the minter does not have to
    live next to the poller;
  * both the `x-aws-waf-token` header and the `aws-waf-token` cookie are
    accepted -- this module uses the header, which is what a server-side
    client should send;
  * the token survived between 212s and 242s of use, consistent with the
    300-second AWS default immunity time.

Five minutes is the whole design constraint. A token cannot be supplied by
hand, and a long-lived browser is a thing that wedges on an unattended box, so
each mint runs in a short-lived subprocess that is killed if it hangs. The
cost is a few seconds of Chromium every four minutes.

The TTL is an optimisation, not the correctness condition. The authority on
whether a token is still good is the API: a `waf_challenge` response
invalidates the cache and forces a fresh mint, so a change to PulsePoint's
immunity time costs one wasted request rather than an outage.

Usage:
    cache = TokenCache(store=store)
    token = cache.get()                     # mints on demand, may be None
    ...
    cache.invalidate()                      # on a waf_challenge response

Standalone, to check the browser side works at all:
    python waf_token.py --mint

Requirements:
    pip install playwright && playwright install chromium

    Optional. Without it the poller behaves exactly as it does today: it
    reports waf_challenge and records it. This module never becomes a hard
    dependency of the scanner.
"""

import asyncio
import json
import os
import subprocess
import sys
import threading
import time

# The page whose <head> loads the WAF SDK. The token is minted for
# PulsePoint's web ACL, so it has to come from PulsePoint's own origin rather
# than a local page that merely includes the same script.
TOKEN_PAGE_URL = "https://web.pulsepoint.org/"

# Measured at ~300s, which is the AWS default. Refreshing a minute early
# leaves room for a slow mint without a gap where every poll is challenged.
ASSUMED_TTL_S = 300
REFRESH_MARGIN_S = 60

# A mint that has not finished by now is wedged. Killing it and trying again
# on the next poll is always better than blocking the poller thread.
MINT_TIMEOUT_S = 60

KV_NAMESPACE = "waf"
KV_KEY = "pulsepoint_token"

USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/140.0.0.0 Safari/537.36")

# Loading the whole Respond app every four minutes would pull 1.7MB of
# application bundle plus ads and analytics from PulsePoint's CDN forever, for
# a token that needs none of it. Only the document and the WAF SDK are let
# through -- roughly 200KB a mint instead of 2MB.
BLOCKED_RESOURCE_TYPES = frozenset({"image", "media", "font", "stylesheet"})
BLOCKED_URL_FRAGMENTS = (
    "googlesyndication", "googletagmanager", "google-analytics",
    "doubleclick", "/static/js/main.", "/static/css/",
)


class MintError(Exception):
    """A token could not be minted. Carries a reason fit for a health event."""


# ---------------------------------------------------------------------------
# The browser half
#
# This runs in a subprocess (see _mint_via_subprocess), because a browser is a
# poor thing to keep alive unattended. Nothing here is held between mints.
# ---------------------------------------------------------------------------
async def _mint_async(timeout_s, url, headless):
    """The browser work. See mint_token for why this is the async API."""
    from playwright.async_api import async_playwright
    from playwright.async_api import Error as PlaywrightError

    timeout_ms = int(timeout_s * 1000)

    async def blocked(route):
        request = route.request
        if request.resource_type in BLOCKED_RESOURCE_TYPES:
            return await route.abort()
        if any(fragment in request.url for fragment in BLOCKED_URL_FRAGMENTS):
            return await route.abort()
        return await route.continue_()

    async with async_playwright() as play:
        try:
            browser = await play.chromium.launch(headless=headless)
        except PlaywrightError as e:
            raise MintError(
                f"could not launch chromium ({e}); "
                f"try: playwright install chromium") from e
        try:
            context = await browser.new_context(user_agent=USER_AGENT)
            page = await context.new_page()
            await page.route("**/*", blocked)
            await page.goto(url, wait_until="domcontentloaded",
                            timeout=timeout_ms)
            # The SDK is loaded `defer`, so it is not there on DOMContentLoaded.
            await page.wait_for_function(
                "() => window.AwsWafIntegration "
                "&& typeof window.AwsWafIntegration.getToken === 'function'",
                timeout=timeout_ms)
            return await page.evaluate(
                "async () => await window.AwsWafIntegration.getToken()")
        finally:
            await browser.close()


def mint_token(timeout_s=MINT_TIMEOUT_S, url=TOKEN_PAGE_URL, headless=True):
    """Drive a real browser to the PulsePoint page and ask the SDK for a token.

    Returns the token string. Raises MintError with a reportable reason.

    The async Playwright API, not the sync one. The sync API drives the event
    loop through greenlet, which segfaults outright on a free-threaded build
    (3.14.3t here: exit 139 before the first statement runs). The async API
    has no greenlet in it and works on both. Since minting already happens in
    its own subprocess, owning an event loop here costs nothing and the
    scanner's threads never see it.
    """
    try:
        import playwright.async_api  # noqa: F401
        from playwright.async_api import TimeoutError as PlaywrightTimeout
    except ImportError as e:
        raise MintError(
            f"playwright is not installed ({e}); "
            f"pip install playwright && playwright install chromium") from e

    try:
        token = asyncio.run(_mint_async(timeout_s, url, headless))
    except PlaywrightTimeout as e:
        raise MintError(f"timed out after {timeout_s}s: {e}") from e
    except MintError:
        raise
    except Exception as e:
        raise MintError(f"{type(e).__name__}: {e}") from e

    if not token or not isinstance(token, str):
        raise MintError(f"the SDK returned no token ({token!r})")
    return token


def _mint_via_subprocess(timeout_s=MINT_TIMEOUT_S, python_exe=None):
    """Mint in a child process, so a hung browser is killable.

    A wedged Chromium inside the scanner process cannot be recovered without
    restarting the scanner, which would take the radio streams down with it.
    """
    python_exe = python_exe or sys.executable
    cmd = [python_exe, os.path.abspath(__file__), "--mint", "--json"]
    try:
        done = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=timeout_s + 15,       # the child enforces the real budget
            cwd=os.path.dirname(os.path.abspath(__file__)))
    except subprocess.TimeoutExpired as e:
        raise MintError(f"mint subprocess did not exit within {timeout_s + 15}s") from e

    if done.returncode != 0:
        detail = (done.stderr or done.stdout or "").strip().splitlines()
        raise MintError(detail[-1] if detail else
                        f"mint subprocess exited {done.returncode}")
    try:
        payload = json.loads(done.stdout)
    except ValueError as e:
        raise MintError(f"mint subprocess printed no token ({e})") from e
    if not payload.get("token"):
        raise MintError(payload.get("error") or "mint subprocess returned no token")
    return payload["token"]


# ---------------------------------------------------------------------------
# The caching half
# ---------------------------------------------------------------------------
class TokenCache:
    """One token, shared by every poller that needs it.

    The shipped config runs two PulsePoint pollers against the same agency
    with different unit filters. They are separate threads and must not mint
    separately -- that would double the browser launches and the load on
    PulsePoint for no benefit, since the token is not per-request.
    """

    def __init__(self, store=None, ttl_s=ASSUMED_TTL_S,
                 refresh_margin_s=REFRESH_MARGIN_S, mint=None, clock=time.time):
        """
        Args:
            store: a store.Store. The token is cached in its kv table so a
                restart can reuse one that still has life in it rather than
                launching a browser before the first poll.
            ttl_s: assumed token lifetime. Only an optimisation; see module docstring.
            refresh_margin_s: how early to re-mint.
            mint: callable returning a token. Defaults to the subprocess minter;
                tests pass their own so the suite never needs a browser.
            clock: wall clock, injectable for tests. Wall rather than monotonic
                because the expiry is persisted across restarts.
        """
        self.store = store
        self.ttl_s = ttl_s
        self.refresh_margin_s = refresh_margin_s
        self._mint = mint or _mint_via_subprocess
        self.clock = clock
        self._lock = threading.Lock()
        self._token = None
        self._expires_at = 0.0
        self.last_error = None
        self.mints = 0
        self._load()

    # -- persistence --------------------------------------------------------
    def _load(self):
        if self.store is None:
            return
        try:
            cached = self.store.kv.get(KV_NAMESPACE, KV_KEY)
        except Exception:
            return          # a cache miss is never worth failing a poll over
        if isinstance(cached, dict) and cached.get("token"):
            self._token = cached["token"]
            self._expires_at = float(cached.get("expires_at") or 0)

    def _save(self):
        if self.store is None:
            return
        try:
            self.store.kv.set(KV_NAMESPACE, KV_KEY,
                              {"token": self._token,
                               "expires_at": self._expires_at})
        except Exception:
            pass

    # -- api ----------------------------------------------------------------
    def _fresh(self):
        return (self._token is not None
                and self.clock() < self._expires_at - self.refresh_margin_s)

    def get(self):
        """A usable token, minting one if needed. None if minting failed.

        None is returned rather than raised because a failed mint is the same
        shape of problem as a failed fetch: the poller records it and tries
        again on the next pass.
        """
        with self._lock:
            if self._fresh():
                return self._token
            try:
                token = self._mint()
            except MintError as e:
                self.last_error = str(e)
                # Keep any existing token. It is probably stale, but the API
                # is the judge of that and a stale token costs one request.
                return self._token
            self._token = token
            self._expires_at = self.clock() + self.ttl_s
            self.last_error = None
            self.mints += 1
            self._save()
            return self._token

    def invalidate(self):
        """Drop the cached token. Called when the API rejects it, which is the
        only authoritative statement about whether it was still valid."""
        with self._lock:
            self._token = None
            self._expires_at = 0.0
            if self.store is not None:
                try:
                    self.store.kv.delete(KV_NAMESPACE, KV_KEY)
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# Standalone mode
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Mint an AWS WAF token for the PulsePoint API")
    parser.add_argument("--mint", action="store_true",
                        help="Mint one token and print it")
    parser.add_argument("--json", action="store_true",
                        help="Print the result as JSON (used by the poller)")
    parser.add_argument("--timeout", type=float, default=MINT_TIMEOUT_S)
    parser.add_argument("--headed", action="store_true",
                        help="Show the browser, for debugging a failing mint")
    parser.add_argument("--check", action="store_true",
                        help="Mint a token, then use it against the live API")
    args = parser.parse_args()

    if not (args.mint or args.check):
        parser.error("nothing to do: pass --mint or --check")

    try:
        token = mint_token(timeout_s=args.timeout, headless=not args.headed)
    except MintError as e:
        if args.json:
            print(json.dumps({"token": None, "error": str(e)}))
        else:
            print(f"[waf] mint failed: {e}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps({"token": token}))
        return

    print(f"[waf] minted a {len(token)} char token")

    if args.check:
        from pulsepoint_poller import fetch_incidents
        result = fetch_incidents("EMS1201", token=token)
        if result.ok:
            print(f"[waf] API accepted it: {len(result.active)} active, "
                  f"{len(result.recent)} recent incident(s)")
        else:
            print(f"[waf] API rejected it: {result.error}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
