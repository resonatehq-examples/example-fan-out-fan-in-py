"""Four independent notification channels.

Each channel has realistic latency — email is slow, push is fast, etc. When
crash mode is enabled, the push channel fails its first attempt; Resonate
retries it. The other three channels are already checkpointed and do NOT
re-send.
"""

from __future__ import annotations

import random
import string
import time
from dataclasses import dataclass
from typing import Any

from resonate import Context


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass
class OrderEvent:
    order_id: str
    user_id: str
    event: str
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "user_id": self.user_id,
            "event": self.event,
            "message": self.message,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> OrderEvent:
        return cls(
            order_id=d["order_id"],
            user_id=d["user_id"],
            event=d["event"],
            message=d["message"],
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _msg_id(prefix: str) -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"msg_{prefix}_{suffix}"


# Track push notification attempts across retries (for the crash demo).
_push_attempts: dict[str, int] = {}


# ---------------------------------------------------------------------------
# Channels
# ---------------------------------------------------------------------------


def send_email(_: Context, event: dict[str, Any]) -> dict[str, Any]:
    start = time.time()
    user_id = event["user_id"]
    print(f"  [email]   Sending order confirmation to user {user_id}...")
    time.sleep(0.4)  # SMTP is slow
    message_id = _msg_id("email")
    print(f"  [email]   Sent — {message_id}")
    return {
        "channel": "email",
        "success": True,
        "message_id": message_id,
        "duration_ms": int((time.time() - start) * 1000),
    }


def send_sms(_: Context, event: dict[str, Any]) -> dict[str, Any]:
    start = time.time()
    user_id = event["user_id"]
    print(f"  [sms]     Sending SMS to user {user_id}...")
    time.sleep(0.25)  # Twilio is fast
    message_id = _msg_id("sms")
    print(f"  [sms]     Sent — {message_id}")
    return {
        "channel": "sms",
        "success": True,
        "message_id": message_id,
        "duration_ms": int((time.time() - start) * 1000),
    }


def send_slack(_: Context, event: dict[str, Any]) -> dict[str, Any]:
    start = time.time()
    print("  [slack]   Posting to #orders channel...")
    time.sleep(0.18)  # Slack webhooks are quick
    message_id = _msg_id("slack")
    print(f"  [slack]   Posted — {message_id}")
    return {
        "channel": "slack",
        "success": True,
        "message_id": message_id,
        "duration_ms": int((time.time() - start) * 1000),
    }


def send_push(
    _: Context,
    event: dict[str, Any],
    simulate_crash: bool,
) -> dict[str, Any]:
    start = time.time()
    order_id = event["order_id"]
    user_id = event["user_id"]

    attempt = _push_attempts.get(order_id, 0) + 1
    _push_attempts[order_id] = attempt

    print(
        f"  [push]    Sending push notification to user {user_id} "
        f"(attempt {attempt})..."
    )
    time.sleep(0.12)

    if simulate_crash and attempt == 1:
        # Push service is temporarily down. Resonate retries this step.
        # Email, SMS, and Slack are already checkpointed — they do NOT re-send.
        raise RuntimeError("Push service unavailable — will retry")

    message_id = _msg_id("push")
    print(f"  [push]    Delivered — {message_id}")
    return {
        "channel": "push",
        "success": True,
        "message_id": message_id,
        "duration_ms": int((time.time() - start) * 1000),
    }
