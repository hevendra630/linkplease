import asyncio
import datetime
import aiosqlite

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS rules (
    rule_id       TEXT PRIMARY KEY,
    keyword       TEXT NOT NULL,
    keyword_lower TEXT NOT NULL,
    dm_message    TEXT NOT NULL,
    created_at    TEXT NOT NULL
);

-- Every webhook delivery, keyed by the platform's event_id. This is our
-- ingestion-level dedup: a redelivered event_id is rejected by the UNIQUE
-- constraint atomically, no race window, no app-level check-then-insert.
CREATE TABLE IF NOT EXISTS events (
    event_id     TEXT PRIMARY KEY,
    event_type   TEXT NOT NULL,
    payload      TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'received',  -- received -> processed | error
    received_at  TEXT NOT NULL,
    processed_at TEXT
);

CREATE TABLE IF NOT EXISTS comments (
    comment_id TEXT PRIMARY KEY,
    post_id    TEXT,
    text       TEXT,
    user_id    TEXT,
    username   TEXT,
    created_at TEXT,
    deleted    INTEGER NOT NULL DEFAULT 0
);

-- One row per (user, rule) that ever matched -- the UNIQUE constraint IS the
-- "never DM the same user twice for the same rule" guarantee, enforced by
-- SQLite itself rather than an in-app check that could race.
CREATE TABLE IF NOT EXISTS dm_tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    comment_id       TEXT NOT NULL,
    user_id          TEXT NOT NULL,
    rule_id          TEXT NOT NULL,
    message          TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL,
    reattempt_index  INTEGER NOT NULL DEFAULT 0,
    attempts         INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'pending',
        -- pending -> sending -> queued -> delivered
        --                               -> failed (transient exhausted / 400 / reattempts exhausted)
        -- pending -> cancelled (comment.deleted arrived before we sent)
    dm_id            TEXT,
    next_attempt_at  TEXT NOT NULL,
    last_error       TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    UNIQUE(user_id, rule_id)
);

CREATE TABLE IF NOT EXISTS counters (
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_dm_tasks_status_next ON dm_tasks(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_dm_tasks_comment ON dm_tasks(comment_id);
CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
"""

_conn: aiosqlite.Connection | None = None
_lock = asyncio.Lock()  # serialize all writes; SQLite is single-writer anyway


def now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


async def init_db():
    global _conn
    _conn = await aiosqlite.connect(config.DB_PATH)
    _conn.row_factory = aiosqlite.Row
    await _conn.execute("PRAGMA journal_mode=WAL;")
    await _conn.execute("PRAGMA foreign_keys=ON;")
    await _conn.executescript(SCHEMA)
    for name in ("duplicates_blocked",):
        await _conn.execute(
            "INSERT OR IGNORE INTO counters(name, value) VALUES (?, 0)", (name,)
        )
    await _conn.commit()
    await _recover_crashed_state()


async def _recover_crashed_state():
    """
    If the process died mid-send, any row stuck in 'sending' is neither
    confirmed sent nor safe to assume failed. Put it back to 'pending' so the
    sender loop picks it up again -- its idempotency_key is unchanged, so if
    the original HTTP call actually reached the platform, PseudoGram's
    idempotency handling returns the original dm_id instead of sending twice.
    """
    async with _lock:
        await _conn.execute(
            "UPDATE dm_tasks SET status='pending', updated_at=? WHERE status='sending'",
            (now_iso(),),
        )
        await _conn.commit()


def get_conn() -> aiosqlite.Connection:
    assert _conn is not None, "DB not initialized"
    return _conn


def get_lock() -> asyncio.Lock:
    return _lock


async def close_db():
    global _conn
    if _conn is not None:
        await _conn.close()
        _conn = None
