import asyncio
import datetime
import logging
import random

from . import config, db, processing, pseudogram_client as pg

log = logging.getLogger("linkplease.workers")


def _iso_in(seconds: float) -> str:
    return (datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(seconds=seconds)).isoformat()


async def event_processor_loop(stop_event: asyncio.Event):
    """
    Polls the events table for anything not yet processed, in received order.
    Driving this off durable DB rows (rather than an in-memory queue) means a
    process restart loses nothing -- work just resumes from where it left off.
    """
    conn = db.get_conn()
    while not stop_event.is_set():
        try:
            cur = await conn.execute(
                "SELECT * FROM events WHERE status='received' ORDER BY received_at LIMIT ?",
                (config.EVENT_BATCH_SIZE,),
            )
            rows = await cur.fetchall()
            for row in rows:
                try:
                    await processing.process_event_row(row)
                except Exception:
                    log.exception("failed to process event %s", row["event_id"])
            if not rows:
                await asyncio.sleep(config.EVENT_POLL_INTERVAL_SECONDS)
        except Exception:
            log.exception("event_processor_loop iteration failed")
            await asyncio.sleep(1)


async def _schedule_reattempt_or_fail(conn, task, error: str):
    """A logical send attempt is done for good (transient retries exhausted,
    or platform confirmed failure). Either start a fresh attempt with a new
    idempotency key, or give up."""
    if task["reattempt_index"] + 1 >= config.MAX_REATTEMPTS:
        await conn.execute(
            "UPDATE dm_tasks SET status='failed', last_error=?, updated_at=? WHERE id=?",
            (error, db.now_iso(), task["id"]),
        )
        return
    new_reattempt = task["reattempt_index"] + 1
    new_key = f"{task['user_id']}:{task['rule_id']}:{new_reattempt}"
    await conn.execute(
        """UPDATE dm_tasks SET status='pending', reattempt_index=?, attempts=0,
             idempotency_key=?, dm_id=NULL, last_error=?, next_attempt_at=?, updated_at=?
           WHERE id=?""",
        (new_reattempt, new_key, error, _iso_in(config.REATTEMPT_BACKOFF_SECONDS),
         db.now_iso(), task["id"]),
    )


async def sender_loop(stop_event: asyncio.Event):
    conn = db.get_conn()
    while not stop_event.is_set():
        try:
            now = db.now_iso()
            cur = await conn.execute(
                """SELECT * FROM dm_tasks WHERE status='pending' AND next_attempt_at <= ?
                   ORDER BY created_at LIMIT ?""",
                (now, config.SENDER_BATCH_SIZE),
            )
            tasks = await cur.fetchall()
            if not tasks:
                await asyncio.sleep(config.SENDER_POLL_INTERVAL_SECONDS)
                continue

            for task in tasks:
                async with db.get_lock():
                    await conn.execute(
                        "UPDATE dm_tasks SET status='sending', updated_at=? WHERE id=?",
                        (db.now_iso(), task["id"]),
                    )
                    await conn.commit()

                result = await pg.send_dm(
                    task["user_id"], task["message"], task["comment_id"], task["idempotency_key"]
                )

                async with db.get_lock():
                    if result.kind == "accepted":
                        await conn.execute(
                            """UPDATE dm_tasks SET status='queued', dm_id=?, last_error=NULL,
                                 updated_at=? WHERE id=?""",
                            (result.dm_id, db.now_iso(), task["id"]),
                        )
                    elif result.kind == "invalid":
                        # 400 -- retrying will not help, per the API contract.
                        await conn.execute(
                            "UPDATE dm_tasks SET status='failed', last_error=?, updated_at=? WHERE id=?",
                            (result.error, db.now_iso(), task["id"]),
                        )
                    else:
                        attempts = task["attempts"] + 1
                        if attempts >= config.MAX_TRANSIENT_ATTEMPTS:
                            await _schedule_reattempt_or_fail(
                                conn, task, result.error or "transient retries exhausted"
                            )
                        else:
                            if result.kind == "rate_limited" and result.retry_after:
                                delay = result.retry_after
                            else:
                                delay = min(
                                    config.TRANSIENT_BACKOFF_MAX_SECONDS,
                                    config.TRANSIENT_BACKOFF_BASE_SECONDS * (2 ** attempts),
                                ) * random.uniform(0.8, 1.2)
                            await conn.execute(
                                """UPDATE dm_tasks SET status='pending', attempts=?,
                                     last_error=?, next_attempt_at=?, updated_at=? WHERE id=?""",
                                (attempts, result.error, _iso_in(delay), db.now_iso(), task["id"]),
                            )
                    await conn.commit()
        except Exception:
            log.exception("sender_loop iteration failed")
            await asyncio.sleep(1)


async def reconciliation_loop(stop_event: asyncio.Event):
    """
    A 202 from /v1/dm/send only means 'accepted', not delivered (~15% end up
    failed). This loop is the only source of truth for whether a queued DM
    actually landed, and it's what lets us retry platform-confirmed failures.
    """
    conn = db.get_conn()
    while not stop_event.is_set():
        try:
            cur = await conn.execute(
                "SELECT * FROM dm_tasks WHERE status='queued' LIMIT ?",
                (config.RECONCILE_BATCH_SIZE,),
            )
            tasks = await cur.fetchall()
            if not tasks:
                await asyncio.sleep(config.RECONCILE_POLL_INTERVAL_SECONDS)
                continue

            for task in tasks:
                if not task["dm_id"]:
                    continue
                info = await pg.get_dm_status(task["dm_id"])
                if info is None:
                    continue  # transient lookup failure, try again next pass
                status = info.get("status")
                async with db.get_lock():
                    if status == "delivered":
                        await conn.execute(
                            "UPDATE dm_tasks SET status='delivered', updated_at=? WHERE id=?",
                            (db.now_iso(), task["id"]),
                        )
                    elif status == "failed":
                        await _schedule_reattempt_or_fail(
                            conn, task, "platform reported delivery failure"
                        )
                    # status == 'queued' -> still in flight, leave as is
                    await conn.commit()

            await asyncio.sleep(config.RECONCILE_POLL_INTERVAL_SECONDS)
        except Exception:
            log.exception("reconciliation_loop iteration failed")
            await asyncio.sleep(1)
