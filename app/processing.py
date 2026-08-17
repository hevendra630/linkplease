import json
import secrets
import sqlite3

from . import db


def new_rule_id() -> str:
    # Opaque, cryptographically random -- not sequential, not guessable from
    # rule count or creation time. Prefixed like the platform's own ids
    # (dm_..., evt_...) rather than a raw UUID. The README only requires
    # "any-string-you-like" for rule_id, so this is a style choice, not a
    # contract requirement.
    return f"rul_{secrets.token_hex(16)}"


async def bump_counter(name: str, by: int = 1):
    conn = db.get_conn()
    await conn.execute("UPDATE counters SET value = value + ? WHERE name = ?", (by, name))


async def get_matching_rules(text: str) -> list[sqlite3.Row]:
    conn = db.get_conn()
    cur = await conn.execute("SELECT * FROM rules")
    rules = await cur.fetchall()
    text_lower = (text or "").lower()
    return [r for r in rules if r["keyword_lower"] in text_lower]


async def process_comment_created(data: dict):
    conn = db.get_conn()
    comment_id = data["comment_id"]
    user_id = data["from"]["user_id"]
    username = data["from"].get("username")
    text = data.get("text", "")

    async with db.get_lock():
        # Preserve an existing `deleted` flag on conflict: if a comment.deleted
        # for this comment_id already arrived (events can arrive out of
        # order), a placeholder row with deleted=1 may already exist here.
        await conn.execute(
            """INSERT INTO comments(comment_id, post_id, text, user_id, username, created_at, deleted)
               VALUES (?, ?, ?, ?, ?, ?, 0)
               ON CONFLICT(comment_id) DO UPDATE SET
                 text=excluded.text, user_id=excluded.user_id, username=excluded.username""",
            (comment_id, data.get("post_id"), text, user_id, username, data.get("created_at")),
        )
        await conn.commit()
        cur = await conn.execute("SELECT deleted FROM comments WHERE comment_id=?", (comment_id,))
        row = await cur.fetchone()

    if row and row["deleted"]:
        # comment.deleted for this comment_id was already processed (it beat
        # comment.created here) -- never mind sending anything.
        return

    matches = await get_matching_rules(text)
    if not matches:
        return

    now = db.now_iso()
    async with db.get_lock():
        for rule in matches:
            idem_key = f"{user_id}:{rule['rule_id']}:0"
            try:
                await conn.execute(
                    """INSERT INTO dm_tasks(
                         comment_id, user_id, rule_id, message, idempotency_key,
                         reattempt_index, attempts, status, next_attempt_at, created_at, updated_at
                       ) VALUES (?, ?, ?, ?, ?, 0, 0, 'pending', ?, ?, ?)""",
                    (comment_id, user_id, rule["rule_id"], rule["dm_message"], idem_key, now, now, now),
                )
            except sqlite3.IntegrityError:
                # This user already has a dm_task for this rule (first comment,
                # a retried comment, or a redelivered event that slipped past
                # event-level dedup by carrying a different event_id). Correctly
                # not sending a second DM.
                await bump_counter("duplicates_blocked")
        await conn.commit()


async def process_comment_deleted(data: dict):
    conn = db.get_conn()
    comment_id = data["comment_id"]
    async with db.get_lock():
        # UPSERT, not UPDATE: comment.deleted can arrive before comment.created
        # (order isn't guaranteed). A placeholder row records the deletion so
        # process_comment_created can check it even if it runs later.
        await conn.execute(
            """INSERT INTO comments(comment_id, deleted) VALUES (?, 1)
               ON CONFLICT(comment_id) DO UPDATE SET deleted=1""",
            (comment_id,),
        )
        # Only cancel tasks that haven't been sent yet. A DM already accepted
        # or delivered by the platform can't be recalled -- see FAILURES.md.
        await conn.execute(
            """UPDATE dm_tasks SET status='cancelled', updated_at=?
               WHERE comment_id=? AND status='pending'""",
            (db.now_iso(), comment_id),
        )
        await conn.commit()


async def process_event_row(row: sqlite3.Row):
    conn = db.get_conn()
    payload = json.loads(row["payload"])
    data = payload.get("data", {})
    try:
        if row["event_type"] == "comment.created":
            await process_comment_created(data)
        elif row["event_type"] == "comment.deleted":
            await process_comment_deleted(data)
        # Unknown event types are accepted (200'd) but simply not actionable.
        async with db.get_lock():
            await conn.execute(
                "UPDATE events SET status='processed', processed_at=? WHERE event_id=?",
                (db.now_iso(), row["event_id"]),
            )
            await conn.commit()
    except Exception as e:  # noqa: BLE001
        async with db.get_lock():
            await conn.execute(
                "UPDATE events SET status='error', processed_at=? WHERE event_id=?",
                (db.now_iso(), row["event_id"]),
            )
            await conn.commit()
        raise


async def create_rule(keyword: str, dm_message: str) -> dict:
    conn = db.get_conn()
    rule_id = new_rule_id()
    now = db.now_iso()
    async with db.get_lock():
        await conn.execute(
            "INSERT INTO rules(rule_id, keyword, keyword_lower, dm_message, created_at) VALUES (?, ?, ?, ?, ?)",
            (rule_id, keyword, keyword.lower(), dm_message, now),
        )
        await conn.commit()
    return {"rule_id": rule_id, "keyword": keyword, "dm_message": dm_message}
