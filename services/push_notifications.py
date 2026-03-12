import json
import os
from typing import Any

from logger_config import setup_logger

logger = setup_logger("push_notifications")


def send_push_notification(subscription: dict[str, Any], payload: dict[str, Any]) -> bool:
    try:
        from pywebpush import webpush  # type: ignore
    except Exception:
        logger.warning("pywebpush not installed; skipping push notification send")
        return False

    vapid_private_key = os.getenv("VAPID_PRIVATE_KEY", "").strip()
    vapid_subject = os.getenv("VAPID_CLAIMS_SUBJECT", "mailto:admin@example.com").strip()
    if not vapid_private_key:
        logger.warning("VAPID_PRIVATE_KEY is missing; skipping push notification send")
        return False

    try:
        webpush(
            subscription_info=subscription,
            data=json.dumps(payload, ensure_ascii=False),
            vapid_private_key=vapid_private_key,
            vapid_claims={"sub": vapid_subject},
        )
        return True
    except Exception as exc:
        logger.warning("Push send failed: %s", exc)
        return False

