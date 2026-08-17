import hmac
import hashlib


def verify_signature(raw_body: bytes, signature_header: str | None, secret: str) -> bool:
    """
    Verify X-PseudoGram-Signature: sha256=<hex> against the RAW request body,
    HMAC-SHA256 keyed by our API key. Uses constant-time comparison to avoid
    timing side-channels.
    """
    if not signature_header or not secret:
        return False
    if not signature_header.startswith("sha256="):
        return False
    given_hex = signature_header.split("=", 1)[1].strip()
    expected_hex = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    try:
        return hmac.compare_digest(given_hex, expected_hex)
    except Exception:
        return False
