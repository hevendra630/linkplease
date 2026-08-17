import os

# --- PseudoGram mock API ---
PSEUDOGRAM_BASE_URL = os.getenv("PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com")
PSEUDOGRAM_API_KEY = os.getenv("PSEUDOGRAM_API_KEY", "")  # used both as X-API-Key and HMAC secret

# --- Storage ---
DB_PATH = os.getenv("DB_PATH", "linkplease.db")

# --- Rate limiting (platform rule: 10 requests / rolling 60s on /v1/dm/send) ---
RATE_LIMIT_MAX_REQUESTS = 10
RATE_LIMIT_WINDOW_SECONDS = 60
# Leave one slot of headroom so we never brush the real limit under jitter.
RATE_LIMIT_SAFETY_MARGIN = 1

# --- Retry / backoff policy ---
# "Transient" retries: 429 / 500 on a single logical send attempt (same idempotency key).
MAX_TRANSIENT_ATTEMPTS = 6
TRANSIENT_BACKOFF_BASE_SECONDS = 1.5
TRANSIENT_BACKOFF_MAX_SECONDS = 45

# "Reattempts": the platform accepted a DM, later confirmed it failed. We try sending a
# genuinely new DM (new idempotency key) up to this many times before giving up for good.
MAX_REATTEMPTS = 3
REATTEMPT_BACKOFF_SECONDS = 5

# --- Background worker cadence ---
EVENT_POLL_INTERVAL_SECONDS = 0.25
SENDER_POLL_INTERVAL_SECONDS = 0.25
RECONCILE_POLL_INTERVAL_SECONDS = 3.0
EVENT_BATCH_SIZE = 50
SENDER_BATCH_SIZE = 20
RECONCILE_BATCH_SIZE = 20

# --- Webhook contract ---
WEBHOOK_SIGNATURE_HEADER = "X-PseudoGram-Signature"
# If false, signature verification is skipped (useful for local testing without a real key).
REQUIRE_SIGNATURE = os.getenv("REQUIRE_SIGNATURE", "true").lower() == "true"
