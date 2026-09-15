"""
outputs.py — delivery workers that read the event log.

Before this module, emitting an event meant calling Discord and the web feed
inline, synchronously, from the thread that had just finished transcribing:

    out.send_discord(...)   # urlopen, timeout=5, except: pass
    out.send_feed(...)      # urlopen, timeout=5, except: pass

Two outputs at five seconds each meant one slow endpoint could stall the
consumer loop for ten seconds. Meanwhile the audio work queue holds twenty
transmissions and drops what will not fit, so a Discord hiccup cost *audio that
was never transcribed* — and both sends swallowed their failure in a bare
`except: pass`, so nothing said it was happening.

Here each destination is a worker reading forward from its own cursor in the
event log. Emitting is now an append and nothing more. A destination that is
down falls behind and catches up; it cannot reach back and stall capture, and
it cannot lose an event it has not acknowledged.

See docs/architecture.md.

Requirements:
    None (stdlib only)
"""

import json
import threading
import time
import urllib.error
import urllib.request

import events

# How long a worker waits for new events before looking again. The manager
# nudges each worker on append, so this is just a backstop.
IDLE_POLL_S = 2.0

# Events read per pass. Small: a worker catching up after an outage should not
# hold the store lock while it drains hours of backlog in one query.
BATCH = 50

# Retry backoff, seconds, then the last value repeats. An endpoint that is down
# for an hour should not be probed every second for an hour.
BACKOFF_S = (1, 2, 5, 15, 30, 60)

HTTP_TIMEOUT_S = 10

# wildfire.removed means a fire dropped out of the upstream feed. It is worth
# recording — the log keeps it — but it was never announced to Discord or the
# web feed, and making the log complete should not start announcing it.
SUPPRESSED_TYPES = frozenset({events.WILDFIRE_REMOVED})


class PermanentFailure(Exception):
    """This event will never be accepted: a bad token, a deleted webhook, a
    malformed payload. Retrying blocks every event behind it, so the worker
    logs it loudly and moves on."""


class RetryableFailure(Exception):
    """The endpoint is unreachable or broken right now. Worth waiting for."""

    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


# 4xx codes worth waiting out rather than discarding the event. A rotated feed
# token or a revoked permission (401/403) is fixable, and dropping the stream
# while someone fixes it defeats the point of keeping a cursor; 408 is an
# explicit ask to try again. Every other 4xx — a malformed payload, a deleted
# webhook — will read the same after any amount of waiting.
RETRYABLE_CLIENT_CODES = frozenset({401, 403, 408, 429})


def _raise_for_http_error(e):
    """Turn an HTTPError into the one of two kinds the worker acts on.

    The split that matters is "wait and it will work" versus "this will never
    work". Retrying the second kind forever would block every event behind it.
    """
    if e.code == 429:
        # Discord rate-limits webhooks. Honour the wait it asks for rather than
        # hammering, which is what earns a longer ban.
        retry_after = None
        header = e.headers.get("Retry-After") if e.headers else None
        if header:
            try:
                retry_after = float(header)
            except (TypeError, ValueError):
                retry_after = None
        raise RetryableFailure("rate limited (HTTP 429)", retry_after)
    if e.code in RETRYABLE_CLIENT_CODES:
        raise RetryableFailure(f"HTTP {e.code} {e.reason}")
    if 400 <= e.code < 500:
        raise PermanentFailure(f"HTTP {e.code} {e.reason}")
    raise RetryableFailure(f"HTTP {e.code} {e.reason}")


def _post(url, payload, headers):
    """POST JSON, classifying failures into the two kinds that matter."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S)
    except urllib.error.HTTPError as e:
        _raise_for_http_error(e)
    except urllib.error.URLError as e:
        raise RetryableFailure(f"{type(e.reason).__name__}: {e.reason}")
    except (TimeoutError, OSError) as e:
        raise RetryableFailure(f"{type(e).__name__}: {e}")


# ---------------------------------------------------------------------------
# Destinations
# ---------------------------------------------------------------------------
class DiscordDestination:
    kind = "discord"

    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def deliver(self, event):
        _post(self.webhook_url,
              {"content": event["render"]["discord"]},
              {"Content-Type": "application/json",
               "User-Agent": "ScannerFeed/1.0"})


class FeedDestination:
    kind = "feed"

    def __init__(self, feed_url, feed_token):
        self.url = feed_url.rstrip("/") + "/ingest"
        self.token = feed_token or ""

    def deliver(self, event):
        # Still the v0 shape. The Worker learns the envelope in Phase 4; until
        # then the structured event is rendered back down on the way out.
        _post(self.url,
              {"line": event["render"]["plain"],
               "type": events.legacy_line_type(event)},
              {"Content-Type": "application/json",
               "Authorization": f"Bearer {self.token}",
               "User-Agent": "ScannerFeed/1.0"})


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------
class OutputWorker(threading.Thread):
    """Delivers one group's events to one destination, in order.

    One thread per destination, so ordering is preserved without coordination,
    and a stalled destination stalls only itself.
    """

    def __init__(self, log, destination, group, stop_event):
        name = f"{group}:{destination.kind}"
        super().__init__(name=f"output-{name}", daemon=True)
        self.log = log
        self.destination = destination
        self.group = group
        self.cursor_name = name
        self.stop_event = stop_event
        self.wake = threading.Event()
        self.delivered = 0
        self.skipped = 0
        self.consecutive_failures = 0

    def notify(self):
        self.wake.set()

    # -- delivery -----------------------------------------------------------
    def _deliver(self, event):
        """Deliver one event, retrying until it lands or we are shutting down.

        Returns True once the cursor may advance past this event — either it was
        delivered, or it can never be. Returns False only when stopping, which
        leaves the cursor where it is so the event is retried next start.
        """
        attempt = 0
        while not self.stop_event.is_set():
            try:
                self.destination.deliver(event)
            except PermanentFailure as e:
                # Retrying would wedge every event behind this one forever.
                print(f"[error] [{self.cursor_name}] dropping {event['type']} "
                      f"{event['id']}: {e}")
                self.skipped += 1
                return True
            except RetryableFailure as e:
                self.consecutive_failures += 1
                # Loud on the way down, quiet while it stays down.
                if self.consecutive_failures == 1:
                    print(f"[warn] [{self.cursor_name}] delivery failing: {e} "
                          f"(will retry; events are queued, not lost)")
                delay = e.retry_after
                if delay is None:
                    delay = BACKOFF_S[min(attempt, len(BACKOFF_S) - 1)]
                self.stop_event.wait(delay)
                attempt += 1
                continue
            except Exception as e:
                # Anything _post did not anticipate: a URL that lost its scheme
                # (ValueError), an http.client.HTTPException (not an OSError, so
                # it escapes the handlers in _post). Treat it as retryable —
                # these are usually config errors that a corrected restart
                # resolves, and the alternative is discarding the event.
                self.consecutive_failures += 1
                if self.consecutive_failures == 1:
                    print(f"[warn] [{self.cursor_name}] unexpected delivery error "
                          f"({type(e).__name__}: {e}); will retry")
                self.stop_event.wait(BACKOFF_S[min(attempt, len(BACKOFF_S) - 1)])
                attempt += 1
                continue
            else:
                if self.consecutive_failures:
                    print(f"[init] [{self.cursor_name}] delivery recovered after "
                          f"{self.consecutive_failures} failure(s)")
                self.consecutive_failures = 0
                self.delivered += 1
                return True
        return False

    # -- loop ---------------------------------------------------------------
    def run(self):
        cursor = None
        while not self.stop_event.is_set():
            try:
                if cursor is None:
                    # Seed at this group's head, not 0. The log predates any
                    # given destination name, so a fresh cursor at 0 would
                    # re-deliver everything still inside the retention window.
                    cursor = self.log.ensure_cursor(
                        self.cursor_name, self.log.latest_seq(group=self.group))

                batch = self.log.read_after(cursor, limit=BATCH, group=self.group)
                if not batch:
                    self.wake.wait(IDLE_POLL_S)
                    self.wake.clear()
                    continue

                for seq, event in batch:
                    if self.stop_event.is_set():
                        return
                    if event["type"] not in SUPPRESSED_TYPES:
                        if not self._deliver(event):
                            return  # stopping mid-retry; cursor stays put
                    # Advance one at a time: a crash mid-batch re-sends at most
                    # the event in flight, rather than replaying the batch.
                    self.log.advance(self.cursor_name, seq)
                    cursor = seq
            except Exception as e:
                # The thread must survive anything. Dying here leaves this
                # destination silently dead for the life of the process, with
                # nothing watching it — status() is never polled and backlog()
                # only runs at shutdown.
                print(f"[warn] [{self.cursor_name}] worker error "
                      f"({type(e).__name__}: {e}); continuing")
                self.stop_event.wait(IDLE_POLL_S)

    def status(self):
        return (f"{self.cursor_name}: {self.delivered} delivered, "
                f"{self.skipped} dropped, at seq {self.log.cursor(self.cursor_name)}"
                + (f", FAILING ({self.consecutive_failures})"
                   if self.consecutive_failures else ""))


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------
class OutputManager:
    """Owns the workers and the nudge that wakes them on append."""

    def __init__(self, log, stop_event):
        self.log = log
        self.stop_event = stop_event
        self.workers = []

    def add_group(self, group, discord_webhook=None, feed_url=None, feed_token=None):
        if discord_webhook:
            self.workers.append(OutputWorker(
                self.log, DiscordDestination(discord_webhook), group, self.stop_event))
        if feed_url:
            self.workers.append(OutputWorker(
                self.log, FeedDestination(feed_url, feed_token), group, self.stop_event))

    def start(self):
        for worker in self.workers:
            worker.start()

    def notify(self, group=None):
        """Nudge workers that an append happened. Never blocks.

        Scoped to one group by default, so a busy group's traffic does not wake
        every other group's workers into a pointless read.
        """
        for worker in self.workers:
            if group is None or worker.group == group:
                worker.notify()

    def stop(self):
        """Signal shutdown and wake any idle worker so it notices now.

        A worker with nothing to do is parked in wake.wait(IDLE_POLL_S), which
        the stop flag alone does not interrupt — without the nudge, shutdown
        waits out the poll interval for no reason.
        """
        self.stop_event.set()
        self.notify()

    def backlog(self):
        """How far each worker is behind the head of *its own group*.

        Measuring against the global head would report a worker as behind
        whenever any other group appended, which never clears — and would go
        negative after a retention sweep, since cursors keep their seq while
        MAX(seq) drops.
        """
        out = {}
        for worker in self.workers:
            head = self.log.latest_seq(group=worker.group)
            out[worker.cursor_name] = max(
                0, head - self.log.cursor(worker.cursor_name))
        return out

    def drain(self, timeout=5.0):
        """Give the workers a moment to finish in-flight deliveries on exit.

        Best effort: whatever does not make it stays in the log with its cursor
        unadvanced and goes out on the next start, which is the entire point of
        keeping a cursor.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not any(value for value in self.backlog().values()):
                return True
            time.sleep(0.1)
        return False
