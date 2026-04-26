"""Fan-out / fan-in notification workflow.

When an order is confirmed, notify the customer through ALL channels
simultaneously: email, SMS, Slack, and push notification.

``ctx.rfi(...)`` starts each channel without blocking — all four start at
once. ``yield <handle>`` collects each result — the fan-in.

Total time approximately equals max(individual channel latencies), not the
sum. If push service is down and retries, email/SMS/Slack are already done.
They do NOT re-send. Each channel is an independent checkpoint.

This is the difference between:

    Sequential: 400ms + 250ms + 180ms + 120ms = 950ms
    Fan-out:    max(400ms, 250ms, 180ms, 120ms) = 400ms
"""

from __future__ import annotations

import time
from typing import Any, Generator

from resonate import Context

from channels import send_email, send_push, send_slack, send_sms


def notify_all(
    ctx: Context,
    event: dict[str, Any],
    simulate_crash: bool,
) -> Generator[Any, Any, dict[str, Any]]:
    start = time.time()

    # Fan-out: start all 4 channels simultaneously.
    # ctx.rfi(...) returns a handle immediately — no blocking.
    email_p = yield ctx.rfi(send_email, event).options(
        id=f"{ctx.id}.email",
    )
    sms_p = yield ctx.rfi(send_sms, event).options(
        id=f"{ctx.id}.sms",
    )
    slack_p = yield ctx.rfi(send_slack, event).options(
        id=f"{ctx.id}.slack",
    )
    push_p = yield ctx.rfi(send_push, event, simulate_crash).options(
        id=f"{ctx.id}.push",
    )

    # Fan-in: await each result.
    # If push fails and retries, the other channels are already checkpointed.
    email = yield email_p
    sms = yield sms_p
    slack = yield slack_p
    push = yield push_p

    results = [email, sms, slack, push]

    return {
        "order_id": event["order_id"],
        "channels_notified": sum(1 for r in results if r.get("success")),
        "total_ms": int((time.time() - start) * 1000),
        "results": results,
    }
