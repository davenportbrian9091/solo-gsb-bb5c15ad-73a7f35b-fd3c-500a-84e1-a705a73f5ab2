"""Startup migration: databases created before the replay columns existed."""
from __future__ import annotations

import sqlite3

from app import db

# The events table as shipped before dead-letter replay was introduced.
_OLD_SCHEMA = """
CREATE TABLE events (
    id              TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    target          TEXT NOT NULL,
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'queued',
    attempts_count  INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    next_attempt_at REAL,
    delivered_at    TEXT
);
CREATE TABLE attempts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id     TEXT NOT NULL REFERENCES events(id),
    attempt_no   INTEGER NOT NULL,
    status_code  INTEGER,
    error        TEXT,
    attempted_at TEXT NOT NULL
);
INSERT INTO events
    (id, idempotency_key, target, payload, status, attempts_count, created_at)
VALUES
    ('e1', 'k1', 'http://example.test/hook', '{}', 'dead', 4,
     '2026-01-01T00:00:00+00:00');
"""


def test_apply_migrations_upgrades_pre_replay_database(tmp_path):
    conn = sqlite3.connect(tmp_path / "old.db")
    conn.row_factory = sqlite3.Row
    conn.executescript(_OLD_SCHEMA)
    conn.commit()
    try:
        db._apply_migrations(conn)
        conn.commit()

        columns = {row["name"] for row in conn.execute("PRAGMA table_info(events)")}
        assert {"replay_count", "last_replayed_at", "round_base_attempts"} <= columns

        # Existing rows are preserved and read back with replay defaults.
        row = conn.execute("SELECT * FROM events WHERE id = 'e1'").fetchone()
        assert row["status"] == "dead"
        assert row["attempts_count"] == 4
        assert row["replay_count"] == 0
        assert row["last_replayed_at"] is None
        assert row["round_base_attempts"] == 0

        # The migration is idempotent.
        db._apply_migrations(conn)
        conn.commit()
    finally:
        conn.close()
