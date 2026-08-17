# LinkPlease — Instagram DM automation

A small, hostile-API-hardened service: a comment matching a keyword gets the
matching user exactly one DM, even when the platform redelivers events,
delivers them out of order, rate-limits us, randomly 500s, or lies about
delivery success.

## How it's built (and why)

```
app/
  main.py               FastAPI app: POST /webhook, POST /rules, GET /stats
  config.py              every tunable (retry counts, rate limit, poll intervals)
  db.py                  SQLite schema + connection + crash recovery on boot
  security.py             HMAC signature verification for webhooks
  pseudogram_client.py   HTTP client to the mock API + rate limiter
  processing.py          turns a webhook event into dm_tasks (matching + dedup)
  workers.py             3 background loops: event processor, sender, reconciler
requirements.txt
Dockerfile
FAILURES.md              honest list of known gaps
```

**The core idea:** nothing lives only in memory. A webhook event is written to
SQLite (`events` table) *before* we return `200`. Three background loops then
do the actual work by polling the database, not an in-memory queue — so if
the process crashes and restarts, no event and no in-flight DM is lost; the
loops just pick up where the database says they left off.

**Two dedup guarantees, both enforced by SQLite `UNIQUE` constraints (not
app-level check-then-insert, which would race):**
1. `events.event_id` is the primary key → a redelivered webhook event is
   rejected atomically on insert.
2. `dm_tasks(user_id, rule_id)` is `UNIQUE` → a user can only ever get one
   `dm_task` row per rule, so they can only ever be DMed once for it, no
   matter how many times they comment or how many times an event redelivers.

**Delivery lifecycle of one `dm_task`:**
```
pending → sending → queued → delivered
                  ↘ pending (retry, transient 429/500, same idempotency key)
                  ↘ failed  (400, or transient retries exhausted, or reattempts exhausted)
queued  → delivered      (reconciliation confirms it landed)
queued  → pending        (reconciliation finds a platform-side failure → fresh
                           idempotency key, new send attempt, up to MAX_REATTEMPTS)
pending → cancelled       (comment.deleted arrives before we've sent)
```

**Rate limiting:** a sliding-window limiter caps outbound `/v1/dm/send` calls
at 9/60s (one below the platform's 10/60s limit, for safety margin) — so we
proactively avoid 429s rather than just reacting to them.

**Idempotency:** every send attempt carries an `Idempotency-Key` of
`{user_id}:{rule_id}:{reattempt_index}`. Transient retries (429/500) of the
*same* logical attempt reuse the same key, so even a crash between "the HTTP
call succeeded" and "we wrote that down" can't cause a duplicate DM — a
retried send with the same key just gets the original `dm_id` back. A brand
new attempt after a platform-confirmed failure bumps `reattempt_index` and
gets a genuinely new key.

## Running it locally

Requires Python 3.11+.

```bash
git clone <your-repo-url>
cd linkplease
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# edit .env: set PSEUDOGRAM_API_KEY to the key from your /v1/apply + /v1/keygen flow

uvicorn app.main:app --host 0.0.0.0 --port 8000
```

The app listens on `http://localhost:8000`. Your three graded routes:
- `POST /webhook`
- `POST /rules`
- `GET /stats`

### Getting your API key (do this once)

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/apply \
  -H 'Content-Type: application/json' \
  -d '{"name":"Your Name","email":"you@example.com","phone":"+91...","linkedin_url":"https://linkedin.com/in/you"}'

curl -X POST https://pseudogram-api.onrender.com/v1/keygen \
  -H 'Content-Type: application/json' \
  -d '{"email":"you@example.com"}'
```

Put the returned `api_key` into `.env` as `PSEUDOGRAM_API_KEY`.

### Trying it end-to-end

```bash
# create a rule
curl -X POST localhost:8000/rules -H 'Content-Type: application/json' \
  -d '{"keyword":"PRICE","dm_message":"Here is the price list: ..."}'

# check current numbers
curl localhost:8000/stats
```

To fire a real load test against your locally running server, you'll need it
reachable from the internet (e.g. `ngrok http 8000`), then:

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/simulate/start \
  -H "X-API-Key: $PSEUDOGRAM_API_KEY" -H 'Content-Type: application/json' \
  -d '{"webhook_url":"https://<your-ngrok-url>/webhook","count":500,"duration_seconds":10}'
```

Then compare `GET /stats` on your app against
`GET /v1/simulate/{run_id}/truth` from the mock API.

## Deploying (Render, free tier)

1. Push this repo to GitHub.
2. New Web Service on Render → connect the repo.
3. Build command: `pip install -r requirements.txt`
   Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
4. Set environment variables: `PSEUDOGRAM_API_KEY`, `REQUIRE_SIGNATURE=true`.
5. **Attach a persistent disk** mounted at, say, `/data`, and set
   `DB_PATH=/data/linkplease.db`. Without a persistent disk, Render's
   filesystem is ephemeral and a redeploy wipes your SQLite file — see
   `FAILURES.md`.
6. Your `working_url` for submission is the Render URL Render gives you.

A `Dockerfile` is included if you'd rather deploy anywhere that takes a
container image (Fly.io, Railway, a VPS, etc).

## Configuration (env vars, see `app/config.py`)

| Var | Default | Meaning |
|---|---|---|
| `PSEUDOGRAM_API_KEY` | — | your key; also the HMAC secret for webhook signatures |
| `PSEUDOGRAM_BASE_URL` | `https://pseudogram-api.onrender.com` | mock API base |
| `DB_PATH` | `linkplease.db` | SQLite file path |
| `REQUIRE_SIGNATURE` | `true` | set `false` to test locally without a real key |
| `MAX_TRANSIENT_ATTEMPTS` | `6` | retries for 429/500 on one send attempt |
| `MAX_REATTEMPTS` | `3` | fresh send attempts after platform-confirmed failure |

## Tests / self-check

There's no formal test suite (see `FAILURES.md`) — the app was verified with
manual `curl` runs covering: normal send, redelivered `event_id`, same-user
different-comment duplicate, `comment.deleted` before send, and a rate-limit
burst. For grading-scale confidence, run `/v1/simulate/start` against a
publicly reachable deployment and diff `/stats` against `/v1/simulate/{id}/truth`.
