"""SQLite persistence layer.

A single connection is shared between the API threads and the worker
threads and is protected by ``db_lock``. The database is the source of
truth for the delivery state machine, so no delivery state lives only in
memory and a crash can never permanently lose or duplicate an event.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone

from . import config

# Event lifecycle:
#   queued     - waiting for the first attempt or a retry
#   processing - an HTTP attempt is in flight (at most one per endpoint)
#   delivered  - got a 2xx response (terminal)
#   dead       - all attempts of the current round exhausted (dead-letter);
#                terminal unless replayed, which starts a fresh backoff round
TERMINAL_STATUSES = ("delivered", "dead")


class ReplayNotAllowedError(Exception):
    """Raised when a replay is requested for an event that is not dead."""

    def __init__(self, event_id: str, status: str) -> None:
        self.event_id = event_id
        self.status = status
        super().__init__(
            f"event {event_id} is '{status}'; only dead-letter events can be replayed"
        )

db_lock = threading.RLock()
_conn: sqlite3.Connection | None = None


def _iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(os.path.abspath(config.DB_PATH)), exist_ok=True)
        _conn = sqlite3.connect(config.DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA foreign_keys=ON")
        _conn.execute("PRAGMA busy_timeout=5000")
    return _conn


def init_db() -> None:
    with db_lock:
        conn = get_conn()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                id              TEXT PRIMARY KEY,
                idempotency_key TEXT NOT NULL UNIQUE,
                target          TEXT NOT NULL,
                payload         TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'queued',
                attempts_count  INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL,
                next_attempt_at REAL,
                delivered_at    TEXT,
                replay_count    INTEGER NOT NULL DEFAULT 0,
                last_replayed_at TEXT,
                round_base_attempts INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS attempts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id     TEXT NOT NULL REFERENCES events(id),
                attempt_no   INTEGER NOT NULL,
                status_code  INTEGER,
                error        TEXT,
                attempted_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_events_status
                ON events(status, next_attempt_at);
            CREATE INDEX IF NOT EXISTS idx_events_target ON events(target);
            CREATE INDEX IF NOT EXISTS idx_attempts_event ON attempts(event_id);
            """
        )
        _migrate_events(conn)
        conn.commit()


def _migrate_events(conn: sqlite3.Connection) -> None:
    """Add replay columns to databases created before replay existed.

    ``CREATE TABLE IF NOT EXISTS`` leaves pre-existing files untouched, so
    the new columns are added idempotently here. Existing rows receive the
    defaults (``replay_count=0``, ``round_base_attempts=0``), which keeps
    their behaviour identical to the pre-replay state machine.
    """
    existing = {row["name"] for row in conn.execute("PRAGMA table_info(events)")}
    for column, ddl in (
        ("replay_count", "replay_count INTEGER NOT NULL DEFAULT 0"),
        ("last_replayed_at", "last_replayed_at TEXT"),
        ("round_base_attempts", "round_base_attempts INTEGER NOT NULL DEFAULT 0"),
    ):
        if column not in existing:
            conn.execute(f"ALTER TABLE events ADD COLUMN {ddl}")


def reset_stuck_events() -> None:
    """Re-queue events left in 'processing' by a hard crash/restart.

    Such an event becomes the blocking head for its endpoint again and is
    safely redelivered. Delivery is at-least-once, which is why outbound
    requests carry an idempotency header.
    """
    with db_lock:
        conn = get_conn()
        conn.execute(
            "UPDATE events SET status = 'queued', next_attempt_at = ? "
            "WHERE status = 'processing'",
            (time.time(),),
        )
        conn.commit()


def create_event(
    idempotency_key: str, target: str, payload: dict | list
) -> tuple[dict, bool]:
    """Insert an event. Returns ``(event, created)``.

    If the idempotency key already exists the stored event is returned with
    ``created=False``; nothing is inserted and nothing is enqueued again.
    """
    event_id = uuid.uuid4().hex
    payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    with db_lock:
        conn = get_conn()
        existing = conn.execute(
            "SELECT * FROM events WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        if existing is not None:
            return dict(existing), False

        now = time.time()
        conn.execute(
            "INSERT INTO events "
            "(id, idempotency_key, target, payload, status, attempts_count, "
            " created_at, next_attempt_at) VALUES (?, ?, ?, ?, 'queued', 0, ?, ?)",
            (event_id, idempotency_key, target, payload_json, _iso(), now),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
        return dict(row), True


def _hydrate(row: sqlite3.Row) -> dict:
    event = dict(row)
    try:
        event["payload"] = json.loads(event["payload"])
    except (TypeError, ValueError):
        pass
    return event


def claim_due_events() -> list[dict]:
    """Atomically claim the head event of every endpoint that is due.

    An event is claimable only when it is ``queued``, its backoff has
    elapsed, and no earlier (lower rowid = earlier creation) event for the
    same target is still ``queued``/``processing``. Claimed events flip to
    ``processing`` so another tick can never double-claim them. This is the
    ordering guarantee: a later event cannot be picked while an earlier one
    is pending, in flight, or waiting for its retry backoff.
    """
    now = time.time()
    with db_lock:
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT e.* FROM events e
            WHERE e.status = 'queued'
              AND e.next_attempt_at <= ?
              AND NOT EXISTS (
                    SELECT 1 FROM events e2
                    WHERE e2.target = e.target
                      AND e2.rowid < e.rowid
                      AND e2.status IN ('queued', 'processing')
              )
            ORDER BY e.rowid
            """,
            (now,),
        ).fetchall()
        if rows:
            conn.executemany(
                "UPDATE events SET status = 'processing' WHERE id = ?",
                [(r["id"],) for r in rows],
            )
            conn.commit()
        return [_hydrate(r) for r in rows]


def record_attempt(
    event_id: str, status_code: int | None, error: str | None
) -> dict:
    """Persist one finished attempt and advance the event state machine.

    2xx          -> delivered
    other / none -> queued with the next backoff of the current round
                    (1s, 2s, 4s after round attempts 1, 2, 3), or dead
                    after the 4th failed attempt of the round. Replaying
                    a dead event starts a new round: ``attempt_no`` keeps
                    counting up globally, but the backoff schedule and the
                    dead-letter cut-off are relative to the round.
    """
    with db_lock:
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM events WHERE id = ?", (event_id,)
        ).fetchone()
        if row is None:
            raise KeyError(event_id)
        event = dict(row)
        attempt_no = event["attempts_count"] + 1
        round_attempt = attempt_no - event["round_base_attempts"]

        conn.execute(
            "INSERT INTO attempts "
            "(event_id, attempt_no, status_code, error, attempted_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (event_id, attempt_no, status_code, error, _iso()),
        )

        success = status_code is not None and 200 <= status_code < 300
        if success:
            conn.execute(
                "UPDATE events SET status = 'delivered', attempts_count = ?, "
                "next_attempt_at = NULL, delivered_at = ? WHERE id = ?",
                (attempt_no, _iso(), event_id),
            )
        elif round_attempt >= config.MAX_ATTEMPTS:
            conn.execute(
                "UPDATE events SET status = 'dead', attempts_count = ?, "
                "next_attempt_at = NULL WHERE id = ?",
                (attempt_no, event_id),
            )
        else:
            delay = config.RETRY_BACKOFF[round_attempt - 1]
            conn.execute(
                "UPDATE events SET status = 'queued', attempts_count = ?, "
                "next_attempt_at = ? WHERE id = ?",
                (attempt_no, time.time() + delay, event_id),
            )
        conn.commit()
        return get_event(event_id)


def replay_event(event_id: str) -> dict:
    """Re-queue a dead-letter event for one fresh, full backoff round.

    The ``dead -> queued`` flip is a single conditional UPDATE (one
    transaction): concurrent replays — even from another process — cannot
    both win, because the loser updates zero rows and gets
    ``ReplayNotAllowedError``. A replay can therefore never enqueue or
    deliver the event twice.

    Attempt history is kept untouched: ``attempt_no`` keeps counting up
    (5, 6, …), while ``round_base_attempts`` is lifted to the current
    ``attempts_count`` so the new round restarts the 1s/2s/4s schedule and
    grants a full set of attempts again. The idempotency key, event id and
    creation order (rowid) are not touched, so per-endpoint ordering and
    outbound idempotency headers are unchanged.
    """
    with db_lock:
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM events WHERE id = ?", (event_id,)
        ).fetchone()
        if row is None:
            raise KeyError(event_id)
        if row["status"] != "dead":
            raise ReplayNotAllowedError(event_id, row["status"])
        cur = conn.execute(
            "UPDATE events SET status = 'queued', next_attempt_at = ?, "
            "replay_count = replay_count + 1, last_replayed_at = ?, "
            "round_base_attempts = attempts_count "
            "WHERE id = ? AND status = 'dead'",
            (time.time(), _iso(), event_id),
        )
        if cur.rowcount != 1:
            # Lost a cross-process replay race between SELECT and UPDATE.
            conn.rollback()
            raise ReplayNotAllowedError(event_id, "queued")
        conn.commit()
        return get_event(event_id)


def _attempts_for(conn: sqlite3.Connection, event_id: str) -> list[dict]:
    return [
        dict(r)
        for r in conn.execute(
            "SELECT attempt_no, status_code, error, attempted_at "
            "FROM attempts WHERE event_id = ? ORDER BY attempt_no",
            (event_id,),
        ).fetchall()
    ]


def get_event(event_id: str) -> dict | None:
    with db_lock:
        conn = get_conn()
        row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
        if row is None:
            return None
        event = _hydrate(row)
        event["attempts"] = _attempts_for(conn, event_id)
        return event


def get_event_by_idempotency_key(key: str) -> dict | None:
    with db_lock:
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM events WHERE idempotency_key = ?", (key,)
        ).fetchone()
        if row is None:
            return None
        event = _hydrate(row)
        event["attempts"] = _attempts_for(conn, event["id"])
        return event


def list_events(status: str | None = None, limit: int = 50, offset: int = 0) -> list[dict]:
    limit = max(1, min(limit, 200))
    with db_lock:
        conn = get_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM events WHERE status = ? ORDER BY rowid "
                "LIMIT ? OFFSET ?",
                (status, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM events ORDER BY rowid LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        events = []
        for row in rows:
            event = _hydrate(row)
            event["attempts"] = _attempts_for(conn, event["id"])
            events.append(event)
        return events
