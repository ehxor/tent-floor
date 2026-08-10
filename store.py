"""
store.py — shared, durable state for the pollers.

Before this module, every poller kept its change-detection state in process
memory and lost it on restart, and each one solved the problem differently:
PulsePoint diffed nested dicts, BC Wildfire snapshotted a fixed field list,
Nanaimo Fire kept a set of seen ids. Two of them papered over the restart
problem by silently swallowing their first poll; the third re-announced every
active incident every time the process came up.

The duplication was in the diffing *algorithm*, not the storage, so that is
what is shared here. Each source keeps its own table with real columns; the
Reconciler is the one copy of "here is everything I can see right now, tell me
what changed".

Usage:
    store = Store("tentfloor.db")
    recon = store.reconciler("wildfires", key=("guid",),
                             tracked=("stage_code", "size_ha"))
    for change in recon.sync(rows):
        ...

Requirements:
    None (stdlib only)
"""

import json
import sqlite3
import threading
import time

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEFAULT_DB_PATH = "tentfloor.db"
DEFAULT_RETENTION_DAYS = 30

# Columns every reconciled table carries. The Reconciler owns these three;
# every other column belongs to the source.
LIFECYCLE_COLUMNS = ("first_seen", "last_seen", "gone_at")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
def _migration_001(cur):
    """Initial schema: one table per source, plus the kv store.

    Key columns are declared with no type affinity on purpose. The Reconciler
    matches keys by exact value, so a key must come back out of the database
    as the type it went in as. A TEXT column would coerce an integer id to
    "123", the next poll would look up 123, miss, and re-announce the same
    incident forever. BLOB affinity stores each value as given.

    Every table also carries a `scope`, which is part of its primary key. Two
    pollers can watch the same upstream source with different filters — the
    shipped config runs PulsePoint agency EMS1201 twice, once per unit prefix,
    and BC Wildfire twice with different fire centres. Without a scope they
    would share rows, each would read the other's incidents as missing from
    its own snapshot, and the pair would flap between cleared and re-declared
    forever.
    """
    cur.execute("""
        CREATE TABLE pulsepoint_incidents (
            scope           TEXT NOT NULL,
            incident_id,
            agency          TEXT,
            call_type_code  TEXT,
            call_type       TEXT,
            address         TEXT,
            call_time       TEXT,
            raw             TEXT,
            first_seen      REAL,
            last_seen       REAL,
            gone_at         REAL,
            PRIMARY KEY (scope, incident_id)
        )
    """)
    cur.execute("""
        CREATE TABLE pulsepoint_units (
            scope           TEXT NOT NULL,
            incident_id,
            unit_id,
            status_code     TEXT,
            status_label    TEXT,
            first_seen      REAL,
            last_seen       REAL,
            gone_at         REAL,
            PRIMARY KEY (scope, incident_id, unit_id)
        )
    """)
    cur.execute("""
        CREATE TABLE nanaimo_incidents (
            scope           TEXT NOT NULL,
            incident_id,
            call_type       TEXT,
            location        TEXT,
            apparatuses     TEXT,
            timestamp       TEXT,
            time            TEXT,
            date            TEXT,
            raw             TEXT,
            first_seen      REAL,
            last_seen       REAL,
            gone_at         REAL,
            PRIMARY KEY (scope, incident_id)
        )
    """)
    cur.execute("""
        CREATE TABLE wildfires (
            scope           TEXT NOT NULL,
            guid,
            label           TEXT,
            name            TEXT,
            fire_year       INTEGER,
            stage_code      TEXT,
            -- No type affinity on purpose. A REAL column would coerce an
            -- integer hectare figure to 5.0, and NUMERIC would coerce 5.0
            -- back to 5; either way the value the API sent stops round-
            -- tripping and the formatted output drifts ("5 ha" vs "5.0 ha").
            -- BLOB affinity stores ints as ints and floats as floats.
            size_ha,
            fire_of_note    INTEGER,
            fire_centre     TEXT,
            lat             REAL,
            lon             REAL,
            raw             TEXT,
            first_seen      REAL,
            last_seen       REAL,
            gone_at         REAL,
            PRIMARY KEY (scope, guid)
        )
    """)
    cur.execute("""
        CREATE TABLE kv (
            ns          TEXT,
            key         TEXT,
            value       TEXT,
            updated_at  REAL,
            PRIMARY KEY (ns, key)
        )
    """)
    # Retention sweeps scan by gone_at across every reconciled table.
    for table in ("pulsepoint_incidents", "pulsepoint_units",
                  "nanaimo_incidents", "wildfires"):
        cur.execute(f"CREATE INDEX {table}_gone_at ON {table}(gone_at)")


# Ordered. Append only — never edit a migration that has shipped.
MIGRATIONS = [_migration_001]

# Tables the retention sweep applies to.
RECONCILED_TABLES = ("pulsepoint_incidents", "pulsepoint_units",
                     "nanaimo_incidents", "wildfires")


# ---------------------------------------------------------------------------
# Value adaptation
# ---------------------------------------------------------------------------
def _adapt(value):
    """Coerce a Python value into something sqlite stores predictably.

    Booleans matter here: BC Wildfire's fireOfNoteInd arrives as a JSON bool
    but comes back out of an INTEGER column as 0/1. Normalising on the way in
    means the tracked-field comparison is comparing like with like.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return value


# ---------------------------------------------------------------------------
# Change records
# ---------------------------------------------------------------------------
class Change:
    """One difference between the last sync and this one.

    kind is one of:
        appeared     — key not previously in the table
        reappeared   — key was present, had been marked gone, and is back
        changed      — a tracked column differs
        disappeared  — key was live and is absent from this snapshot

    key is the natural key only — the reconciler's scope is a constant of the
    reconciler, so callers index the columns they actually passed in.

    'reappeared' is kept distinct from 'appeared' on purpose: the pollers
    disagree about what it means. A wildfire dropping out of the feed and
    returning is a genuine new declaration, while a Nanaimo incident that
    ages out and comes back is the same incident and must not re-announce.
    """

    __slots__ = ("kind", "key", "before", "after", "changed_fields")

    def __init__(self, kind, key, before, after, changed_fields):
        self.kind = kind
        self.key = key
        self.before = before
        self.after = after
        self.changed_fields = changed_fields

    def __repr__(self):
        return (f"Change(kind={self.kind!r}, key={self.key!r}, "
                f"changed_fields={self.changed_fields!r})")

    def __eq__(self, other):
        if not isinstance(other, Change):
            return NotImplemented
        return (self.kind == other.kind and self.key == other.key
                and self.changed_fields == other.changed_fields)


# ---------------------------------------------------------------------------
# Reconciler
# ---------------------------------------------------------------------------
class Reconciler:
    """Diffs a full snapshot against a table and records the result.

    The caller hands over every row it can currently see for this source;
    sync() reports what changed and leaves the table matching the snapshot.
    """

    def __init__(self, store, table, key, tracked=(), scope=""):
        """
        Args:
            store: the owning Store.
            table: table name. Must already exist (see MIGRATIONS).
            key: tuple of column names forming the key, excluding scope.
            tracked: tuple of columns compared to decide whether a row
                'changed'. Empty means presence-only tracking — rows are
                still updated, but never reported as changed.
            scope: isolates this reconciler's rows from other watchers of the
                same table. Every read and write is confined to it, and the
                caller never has to mention it in a row dict. Use one scope
                per (source, filter) pair — see MIGRATIONS for why.
        """
        self.store = store
        self.table = table
        self.scope = scope
        self.natural_key = tuple(key)
        self.key = ("scope",) + self.natural_key
        self.tracked = tuple(tracked)
        self.columns = store.table_columns(table)

        missing = [c for c in self.key + self.tracked if c not in self.columns]
        if missing:
            raise ValueError(f"{table}: unknown column(s) {missing}")

    # -- internals ----------------------------------------------------------
    def _key_of(self, row):
        return (self.scope,) + tuple(_adapt(row.get(c)) for c in self.natural_key)

    def _row_columns(self, row):
        """The source-owned columns present in this row, adapted."""
        data = {c: _adapt(row[c]) for c in self.columns
                if c in row and c not in LIFECYCLE_COLUMNS}
        data["scope"] = self.scope
        return data

    def _scoped(self, within):
        """Combine the reconciler's scope with a caller-supplied predicate."""
        if within is None:
            return "scope = ?", [self.scope]
        where_sql, params = within
        return f"scope = ? AND ({where_sql})", [self.scope] + list(params)

    def _insert(self, cur, row, now):
        data = self._row_columns(row)
        data.update(first_seen=now, last_seen=now, gone_at=None)
        cols = ", ".join(data)
        marks = ", ".join("?" * len(data))
        cur.execute(f"INSERT INTO {self.table} ({cols}) VALUES ({marks})",
                    list(data.values()))

    def _update(self, cur, key, row, now, revive=False):
        data = self._row_columns(row)
        data["last_seen"] = now
        if revive:
            data["gone_at"] = None
        assignments = ", ".join(f"{c} = ?" for c in data)
        where = " AND ".join(f"{c} = ?" for c in self.key)
        cur.execute(f"UPDATE {self.table} SET {assignments} WHERE {where}",
                    list(data.values()) + list(key))

    def _mark_gone(self, cur, key, now):
        where = " AND ".join(f"{c} = ?" for c in self.key)
        cur.execute(f"UPDATE {self.table} SET gone_at = ? WHERE {where}",
                    [now] + list(key))

    # -- public -------------------------------------------------------------
    def sync(self, rows, within=None, now=None, detect_disappeared=True):
        """Reconcile a full snapshot against the table.

        Args:
            rows: iterable of dicts. Keys are column names; unknown keys are
                ignored, so a poller can pass a slightly wider dict.
            within: optional (where_sql, params) narrowing which existing
                rows take part, on top of this reconciler's own scope. Use it
                when a snapshot only covers a slice of the table — PulsePoint
                units are per-incident, so a poll that sees three incidents
                must not mark every other incident's units as gone.
            now: timestamp override, for tests and replay.
            detect_disappeared: when False, rows absent from the snapshot are
                left alone. PulsePoint needs this: a unit vanishing from an
                incident emits nothing today, and clearing is driven by the
                incident, not the unit.

        Returns:
            list of Change, in a stable order: appeared/reappeared/changed in
            snapshot order, then disappeared sorted by key.
        """
        now = time.time() if now is None else now
        incoming = {}
        for row in rows:
            incoming[self._key_of(row)] = row

        where_sql, where_params = self._scoped(within)

        changes = []
        with self.store.lock:
            cur = self.store.conn.cursor()
            cur.execute(f"SELECT * FROM {self.table} WHERE {where_sql}",
                        list(where_params))
            existing = {}
            for raw_row in cur.fetchall():
                d = dict(raw_row)
                existing[tuple(d[c] for c in self.key)] = d

            for key, row in incoming.items():
                old = existing.get(key)
                if old is None:
                    changes.append(Change("appeared", key[1:], None, row, {}))
                    self._insert(cur, row, now)
                elif old.get("gone_at") is not None:
                    changes.append(Change("reappeared", key[1:], old, row, {}))
                    self._update(cur, key, row, now, revive=True)
                else:
                    diff = {}
                    for field in self.tracked:
                        before = old.get(field)
                        after = _adapt(row.get(field))
                        if before != after:
                            diff[field] = {"old": before, "new": after}
                    if diff:
                        changes.append(Change("changed", key[1:], old, row, diff))
                    self._update(cur, key, row, now)

            if detect_disappeared:
                for key in sorted(existing.keys(), key=lambda k: tuple(map(str, k))):
                    old = existing[key]
                    if key in incoming or old.get("gone_at") is not None:
                        continue
                    changes.append(Change("disappeared", key[1:], old, None, {}))
                    self._mark_gone(cur, key, now)

            self.store.conn.commit()

        return changes

    def mark_gone_where(self, where_sql, params, now=None):
        """Mark live rows matching a predicate as gone. Returns the count.

        Used where disappearance is driven by something other than this
        table's own snapshot — PulsePoint units go when their incident does.
        """
        now = time.time() if now is None else now
        scoped_sql, scoped_params = self._scoped((where_sql, params))
        with self.store.lock:
            cur = self.store.conn.cursor()
            cur.execute(
                f"UPDATE {self.table} SET gone_at = ? "
                f"WHERE gone_at IS NULL AND ({scoped_sql})",
                [now] + scoped_params)
            self.store.conn.commit()
            return cur.rowcount

    def live_keys(self, within=None):
        """Natural keys currently live in this scope, for seeding/inspection."""
        where_sql, where_params = self._scoped(within)
        cols = ", ".join(self.natural_key)
        with self.store.lock:
            cur = self.store.conn.cursor()
            cur.execute(
                f"SELECT {cols} FROM {self.table} "
                f"WHERE gone_at IS NULL AND ({where_sql})", where_params)
            return [tuple(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Key/value store
# ---------------------------------------------------------------------------
class KV:
    """Small durable facts, namespaced. Values are JSON-encoded."""

    def __init__(self, store):
        self.store = store

    def get(self, ns, key, default=None):
        with self.store.lock:
            cur = self.store.conn.cursor()
            cur.execute("SELECT value FROM kv WHERE ns = ? AND key = ?", (ns, key))
            row = cur.fetchone()
        if row is None:
            return default
        try:
            return json.loads(row[0])
        except (TypeError, ValueError):
            return default

    def set(self, ns, key, value, now=None):
        now = time.time() if now is None else now
        payload = json.dumps(value)
        with self.store.lock:
            self.store.conn.execute(
                "INSERT INTO kv (ns, key, value, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(ns, key) DO UPDATE SET value = excluded.value, "
                "updated_at = excluded.updated_at",
                (ns, key, payload, now))
            self.store.conn.commit()

    def delete(self, ns, key):
        with self.store.lock:
            self.store.conn.execute("DELETE FROM kv WHERE ns = ? AND key = ?",
                                    (ns, key))
            self.store.conn.commit()

    def all(self, ns):
        """Every key in a namespace, as a dict."""
        with self.store.lock:
            cur = self.store.conn.cursor()
            cur.execute("SELECT key, value FROM kv WHERE ns = ?", (ns,))
            rows = cur.fetchall()
        out = {}
        for key, value in rows:
            try:
                out[key] = json.loads(value)
            except (TypeError, ValueError):
                continue
        return out


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------
class Store:
    """Owns the sqlite connection, the schema version, and the shared lock.

    One connection shared across the poller threads, serialised by an RLock.
    Volume is a handful of writes per minute, so a dedicated writer thread
    would be premature.
    """

    def __init__(self, path=DEFAULT_DB_PATH, retention_days=DEFAULT_RETENTION_DAYS):
        self.path = path
        self.retention_days = retention_days
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._migrate()
        self.kv = KV(self)
        self._columns = {}

    # -- schema -------------------------------------------------------------
    def _migrate(self):
        with self.lock:
            cur = self.conn.cursor()
            version = cur.execute("PRAGMA user_version").fetchone()[0]
            for index in range(version, len(MIGRATIONS)):
                MIGRATIONS[index](cur)
                cur.execute(f"PRAGMA user_version = {index + 1}")
            self.conn.commit()

    def table_columns(self, table):
        """Column names for a table, cached."""
        if table not in self._columns:
            with self.lock:
                cur = self.conn.cursor()
                cur.execute(f"PRAGMA table_info({table})")
                cols = [r[1] for r in cur.fetchall()]
            if not cols:
                raise ValueError(f"no such table: {table}")
            self._columns[table] = tuple(cols)
        return self._columns[table]

    # -- factories ----------------------------------------------------------
    def reconciler(self, table, key, tracked=(), scope=""):
        return Reconciler(self, table, key, tracked, scope)

    # -- maintenance --------------------------------------------------------
    def sweep(self, now=None):
        """Drop rows gone longer than the retention window.

        This is what bounds growth — without it the tables accumulate every
        incident ever seen, which is the durable version of the unbounded
        seen_ids set this module replaces.
        """
        now = time.time() if now is None else now
        cutoff = now - (self.retention_days * 86400)
        removed = 0
        with self.lock:
            cur = self.conn.cursor()
            for table in RECONCILED_TABLES:
                cur.execute(
                    f"DELETE FROM {table} WHERE gone_at IS NOT NULL AND gone_at < ?",
                    (cutoff,))
                removed += cur.rowcount
            self.conn.commit()
        return removed

    def close(self):
        with self.lock:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# ---------------------------------------------------------------------------
# Standalone mode
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="Inspect the Tent Floor state store")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to the sqlite file")
    parser.add_argument("--sweep", action="store_true",
                        help="Run the retention sweep and exit")
    parser.add_argument("--retention-days", type=int, default=DEFAULT_RETENTION_DAYS)
    args = parser.parse_args()

    store = Store(args.db, retention_days=args.retention_days)

    if args.sweep:
        removed = store.sweep()
        print(f"[store] Swept {removed} row(s) older than {args.retention_days}d")
        return

    with store.lock:
        cur = store.conn.cursor()
        version = cur.execute("PRAGMA user_version").fetchone()[0]
        print(f"[store] {args.db} (schema v{version})")
        print("-" * 52)
        for table in RECONCILED_TABLES:
            row = cur.execute(
                f"SELECT COUNT(*), SUM(gone_at IS NULL) FROM {table}").fetchone()
            total, live = row[0], row[1] or 0
            print(f"  {table:24} {live:5} live / {total:5} total")
        count = cur.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
        print(f"  {'kv':24} {count:5} key(s)")


if __name__ == "__main__":
    main()
