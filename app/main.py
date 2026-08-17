import asyncio
import json
import logging

from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel

from . import config, db, processing, security
from . import pseudogram_client as pg
from . import workers

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("linkplease")

app = FastAPI(title="LinkPlease")

_stop_event = asyncio.Event()
_bg_tasks: list[asyncio.Task] = []


@app.on_event("startup")
async def startup():
    await db.init_db()
    _bg_tasks.append(asyncio.create_task(workers.event_processor_loop(_stop_event)))
    _bg_tasks.append(asyncio.create_task(workers.sender_loop(_stop_event)))
    _bg_tasks.append(asyncio.create_task(workers.reconciliation_loop(_stop_event)))
    log.info("LinkPlease started, workers running")


@app.on_event("shutdown")
async def shutdown():
    _stop_event.set()
    for t in _bg_tasks:
        t.cancel()
    await pg.close_client()
    await db.close_db()


# ---------------------------------------------------------------- /webhook --

@app.post("/webhook")
async def webhook(request: Request):
    raw_body = await request.body()

    if config.REQUIRE_SIGNATURE:
        sig = request.headers.get(config.WEBHOOK_SIGNATURE_HEADER)
        if not security.verify_signature(raw_body, sig, config.PSEUDOGRAM_API_KEY):
            raise HTTPException(status_code=401, detail="invalid signature")

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid json")

    event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    if not event_id or not event_type:
        raise HTTPException(status_code=400, detail="missing event_id or event_type")

    conn = db.get_conn()
    async with db.get_lock():
        try:
            await conn.execute(
                """INSERT INTO events(event_id, event_type, payload, status, received_at)
                   VALUES (?, ?, ?, 'received', ?)""",
                (event_id, event_type, raw_body.decode("utf-8"), db.now_iso()),
            )
            await conn.commit()
        except Exception:
            # UNIQUE(event_id) violation -- this is a redelivery. The DB
            # constraint is the dedup, atomically, with no check-then-insert
            # race window. Nothing further to do; the original delivery
            # already drives (or drove) the actual send.
            await conn.execute(
                "UPDATE counters SET value = value + 1 WHERE name='duplicates_blocked'"
            )
            await conn.commit()

    # Real work happens in the background workers; we return fast, well under
    # the 5s budget, even under a burst of 500 events.
    return {"ok": True}


# ------------------------------------------------------------------ /rules --

class RuleIn(BaseModel):
    keyword: str
    dm_message: str


@app.post("/rules", status_code=201)
async def create_rule(rule: RuleIn):
    if not rule.keyword.strip() or not rule.dm_message.strip():
        raise HTTPException(status_code=400, detail="keyword and dm_message are required")
    created = await processing.create_rule(rule.keyword, rule.dm_message)
    return created


# ------------------------------------------------------------------ /stats --

@app.get("/stats")
async def stats():
    conn = db.get_conn()
    cur = await conn.execute(
        "SELECT status, COUNT(*) as n FROM dm_tasks GROUP BY status"
    )
    rows = {r["status"]: r["n"] for r in await cur.fetchall()}

    cur = await conn.execute("SELECT value FROM counters WHERE name='duplicates_blocked'")
    dup_row = await cur.fetchone()
    duplicates_blocked = dup_row["value"] if dup_row else 0

    sent = rows.get("delivered", 0)
    failed = rows.get("failed", 0)
    queued = rows.get("pending", 0) + rows.get("sending", 0) + rows.get("queued", 0)

    return {
        "sent": sent,
        "failed": failed,
        "queued": queued,
        "duplicates_blocked": duplicates_blocked,
    }


@app.get("/health")
async def health():
    return {"ok": True}
