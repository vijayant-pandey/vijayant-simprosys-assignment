"""HMAC-SHA256 webhook signatures.

The sender computes ``hex(HMAC_SHA256(secret, raw_request_body))`` and sends it in
the ``X-Webhook-Signature`` header, optionally prefixed with ``sha256=``.

The signature is computed over the *raw bytes* we received, never over a
re-serialised JSON object, because re-serialisation is not byte-for-byte stable.
"""
import hashlib
import hmac

from .config import get_settings

_PREFIX = "sha256="


def compute_signature(payload: bytes, secret: str | None = None) -> str:
    key = secret if secret is not None else get_settings().webhook_secret.get_secret_value()
    return hmac.new(key.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def verify_signature(payload: bytes, signature: str | None, secret: str | None = None) -> bool:
    if not signature:
        return False
    candidate = signature.strip()
    if candidate.lower().startswith(_PREFIX):
        candidate = candidate[len(_PREFIX):]
    expected = compute_signature(payload, secret)
    # Constant-time comparison avoids leaking the signature via timing.
    return hmac.compare_digest(expected, candidate.lower())
