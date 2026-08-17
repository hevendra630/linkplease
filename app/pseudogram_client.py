import asyncio
import time
import httpx

from . import config


class SlidingWindowRateLimiter:
    """
    Enforces 'at most N requests per rolling W seconds' cooperatively, so we
    never hit the platform's 429 by our own doing (we still handle 429
    gracefully if it happens anyway -- clock skew, other processes, etc).
    """

    def __init__(self, max_requests: int, window_seconds: float, margin: int = 0):
        self.max_requests = max_requests - margin
        self.window_seconds = window_seconds
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            while True:
                now = time.monotonic()
                cutoff = now - self.window_seconds
                self._timestamps = [t for t in self._timestamps if t > cutoff]
                if len(self._timestamps) < self.max_requests:
                    self._timestamps.append(now)
                    return
                sleep_for = self._timestamps[0] - cutoff
                await asyncio.sleep(max(sleep_for, 0.05))


rate_limiter = SlidingWindowRateLimiter(
    config.RATE_LIMIT_MAX_REQUESTS,
    config.RATE_LIMIT_WINDOW_SECONDS,
    config.RATE_LIMIT_SAFETY_MARGIN,
)

_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            base_url=config.PSEUDOGRAM_BASE_URL,
            headers={"X-API-Key": config.PSEUDOGRAM_API_KEY},
            timeout=10.0,
        )
    return _client


async def close_client():
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


class SendResult:
    def __init__(self, kind: str, dm_id: str | None = None, retry_after: float | None = None,
                 error: str | None = None):
        self.kind = kind  # "accepted" | "rate_limited" | "server_error" | "invalid"
        self.dm_id = dm_id
        self.retry_after = retry_after
        self.error = error


async def send_dm(recipient_user_id: str, message: str, comment_id: str,
                   idempotency_key: str) -> SendResult:
    await rate_limiter.acquire()
    client = get_client()
    try:
        resp = await client.post(
            "/v1/dm/send",
            json={
                "recipient_user_id": recipient_user_id,
                "message": message,
                "comment_id": comment_id,
            },
            headers={"Idempotency-Key": idempotency_key},
        )
    except httpx.RequestError as e:
        # Network-level failure -- treat as transient, safe to retry with same key.
        return SendResult("server_error", error=f"network error: {e}")

    if resp.status_code == 202:
        data = resp.json()
        return SendResult("accepted", dm_id=data.get("dm_id"))
    if resp.status_code == 429:
        retry_after = float(resp.headers.get("Retry-After", "5"))
        return SendResult("rate_limited", retry_after=retry_after)
    if resp.status_code == 500:
        return SendResult("server_error", error=resp.text[:300])
    if resp.status_code == 400:
        return SendResult("invalid", error=resp.text[:300])
    return SendResult("server_error", error=f"unexpected status {resp.status_code}: {resp.text[:200]}")


async def get_dm_status(dm_id: str) -> dict | None:
    client = get_client()
    try:
        resp = await client.get(f"/v1/dm/{dm_id}")
    except httpx.RequestError:
        return None
    if resp.status_code != 200:
        return None
    return resp.json()
