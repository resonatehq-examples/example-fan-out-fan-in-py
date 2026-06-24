"""Fan-out / fan-in notification workflow.

When an order is confirmed, notify the customer through ALL channels
simultaneously: email, SMS, Slack, and push notification.

``ctx.run(...)`` starts each channel without blocking — all four start at
once. ``await <handle>`` collects each result — the fan-in.

Total time approximately equals max(individual channel latencies), not the
sum. If push service is down and retries, email/SMS/Slack are already done.
They do NOT re-send. Each channel is an independent checkpoint.

This is the difference between:

    Sequential: 400ms + 250ms + 180ms + 120ms = 950ms
    Fan-out:    max(400ms, 250ms, 180ms, 120ms) = 400ms
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from resonate.context import Context

from channels import send_email, send_push, send_slack, send_sms


async def notify_all(
    ctx: Context,
    event: dict[str, Any],
    simulate_crash: bool,
) -> dict[str, Any]:
    start = time.time()

    # Fan-out: dispatch all four channels simultaneously.
    # ctx.run(...) returns a handle immediately — no blocking.
    f_email = ctx.run(send_email, event)
    f_sms = ctx.run(send_sms, event)
    f_slack = ctx.run(send_slack, event)
    f_push = ctx.run(send_push, event, simulate_crash)

    # Fan-in: await each result.
    # If push fails and retries, the other channels are already checkpointed.
    email = await f_email
    sms = await f_sms
    slack = await f_slack
    push = await f_push

    results = [email, sms, slack, push]

    return {
        "order_id": event["order_id"],
        "channels_notified": sum(1 for r in results if r.get("success")),
        "total_ms": int((time.time() - start) * 1000),
        "results": results,
    }
