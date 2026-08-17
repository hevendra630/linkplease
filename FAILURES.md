# Known failure modes

Specific, not "robust error handling everywhere." These are the ways this
system can still lose a DM, send a duplicate, or misreport a number, and the
conditions under which each happens.

## 1. A crash between the platform accepting a send and us saving the `dm_id` can duplicate a DM

We mark a `dm_task` `sending`, call `POST /v1/dm/send`, then write the
`dm_id` we get back. If the process is killed after the HTTP call lands on
PseudoGram's side but before our write, startup recovery resets that row from
`sending` back to `pending` and it gets retried — with the *same*
`Idempotency-Key`, so PseudoGram's own idempotency handling is what prevents
a duplicate. That only holds if PseudoGram's idempotency cache is still
"warm" for that key by the time we retry. The README/API docs don't state an
idempotency-key TTL, so a long outage (crash, then a slow restart) followed
by a retry could land as a genuinely new send. We'd have no way to detect
this from our side — we'd just get back a fresh `202` and believe it.

## 2. Multiple instances would double-send

The dedup guarantees (`UNIQUE(event_id)`, `UNIQUE(user_id, rule_id)`) are
enforced by SQLite, and the sender loop's "claim a pending task" step
(`SELECT ... WHERE status='pending'` then `UPDATE ... SET status='sending'`)
is only atomic *within one process*, guarded by an in-memory `asyncio.Lock`.
Run two instances of this app against the same database — whether that's two
processes sharing a file, or (worse) two separate SQLite files behind a load
balancer — and both can select the same pending task before either marks it
`sending`, so both send it. This app is built to run as exactly one instance;
it does not horizontally scale as-is.

## 3. A DM that never resolves to `delivered` or `failed` stays invisible in `/stats` forever

The reconciliation loop polls `GET /v1/dm/{dm_id}` for every `queued` task on
a fixed interval, with no timeout or backoff. If PseudoGram never transitions
a DM out of `queued` (or the status endpoint errors indefinitely for that
`dm_id`), that task sits in `queued` permanently — it's not `sent`, so it
undercounts `sent`, and it's not `failed`, so it undercounts `failed` too. It
shows up bucketed into `stats.queued` alongside DMs we haven't even attempted
yet, so from `/stats` alone you can't tell "about to send" from "waiting on
the platform to confirm."

## 4. SQLite on an ephemeral disk loses everything on redeploy

If this is deployed without a persistent volume (e.g. Render's free tier
without an attached disk), every redeploy or dyno restart wipes the SQLite
file: all rules, all dedup history, all in-flight `dm_tasks`. Any DM that was
`queued` at that moment never gets reconciled, and any comment that arrives
right after restart is treated as if the platform had never sent anything
before — there is no cross-restart dedup for `event_id`s that were only ever
recorded in the wiped file. The README says to attach a persistent disk;
nothing in the code enforces that this was actually done.

## 5. `comment.deleted` can't recall a DM that's already in flight

We only cancel a `dm_task` if it's still `pending` when the deletion arrives.
If the comment is deleted after we've already called `/v1/dm/send` (task is
`sending` or `queued`), we let it run to completion — there's no "unsend."
This is a product decision as much as a technical limit, but it means a
deleted comment can still result in a delivered DM if the timing is close.

## 6. No test suite

Correctness here rests on manual `curl` runs (duplicate event, out-of-order
`comment.deleted`, same-user-different-comment dedup, a burst against the
real rate limit) plus reading the code, not on an automated test suite. I'd
add `pytest` coverage — especially for the reattempt/idempotency-key bumping
logic in `workers.py`, which is the part I'm least confident is bug-free
under real concurrent load — before trusting this in front of real traffic.
