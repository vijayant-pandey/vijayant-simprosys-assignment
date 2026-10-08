"""Send a signed sample webhook to a running service.

    python scripts/send_webhook.py                      # evt_<random>
    python scripts/send_webhook.py --event-id evt_10001 # repeat to see duplicate handling
    python scripts/send_webhook.py --bad-signature      # expect 401
"""
import argparse
import json
import sys
import uuid
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.config import get_settings  # noqa: E402
from app.security import compute_signature  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000/webhooks/order")
    parser.add_argument("--event-id", default=f"evt_{uuid.uuid4().hex[:10]}")
    parser.add_argument("--shop-id", default="shop_123")
    parser.add_argument("--amount", type=float, default=2500)
    parser.add_argument("--bad-signature", action="store_true")
    args = parser.parse_args()

    payload = {
        "event_id": args.event_id,
        "event_type": "order.created",
        "shop_id": args.shop_id,
        "timestamp": "2026-08-18T10:30:00Z",
        "data": {"order_id": "ORD-1001", "customer_id": "CUS-1001", "amount": args.amount},
    }
    body = json.dumps(payload).encode()
    signature = "deadbeef" if args.bad_signature else compute_signature(body)
    settings = get_settings()
    response = httpx.post(
        args.url,
        content=body,
        headers={"Content-Type": "application/json", settings.signature_header: f"sha256={signature}"},
    )
    print(response.status_code, response.text)


if __name__ == "__main__":
    main()
