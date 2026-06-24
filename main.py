"""Entry point — run the fan-out / fan-in notification demo.

Usage:

    uv run main.py            # happy path: all 4 channels in parallel
    uv run main.py --crash    # crash mode: push fails first attempt, retries

Requires a running Resonate server (``resonate dev``).
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

from resonate.resonate import Resonate

from channels import send_email, send_push, send_slack, send_sms
from workflow import notify_all


async def main() -> None:
    simulate_crash = "--crash" in sys.argv

    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)

    # Register the workflow and the four channel functions.
    r.register(notify_all)
    r.register(send_email)
    r.register(send_sms)
    r.register(send_slack)
    r.register(send_push)

    order_id = f"ord_{int(time.time() * 1000)}"
    event = {
        "order_id": order_id,
        "user_id": "user_alice",
        "event": "order.confirmed",
        "message": "Your order has been confirmed! Estimated delivery: 2 hours.",
    }

    print("=== Fan-Out / Fan-In Notification Demo ===")
    mode = (
        "CRASH (push service down on first attempt, retries)"
        if simulate_crash
        else "HAPPY PATH (all 4 channels in parallel)"
    )
    print(f"Mode: {mode}")
    print(
        f"\nOrder {event['order_id']} confirmed — "
        f"notifying customer {event['user_id']}...\n"
    )

    try:
        wall_start = time.time()
        handle = r.run(
            f"notify/{event['order_id']}",
            notify_all,
            event,
            simulate_crash,
        )
        result = await handle.result()
        wall_ms = int((time.time() - wall_start) * 1000)

        print("\n=== Result ===")
        print(f"Channels notified: {result['channels_notified']}/4")
        print(f"Wall time: {wall_ms}ms")

        print("\nChannel timings:")
        for r_ in result["results"]:
            channel = r_["channel"].ljust(6)
            print(f"  {channel} {r_['duration_ms']}ms  {r_['message_id']}")

        sequential = sum(r_["duration_ms"] for r_ in result["results"])

        if not simulate_crash:
            print(f"\nFan-out time:   {wall_ms}ms")
            print(f"Sequential est: {sequential}ms")
            print(f"Speedup:        {sequential / wall_ms:.1f}x")

        if simulate_crash:
            print(
                "\nNotice: email/sms/slack each logged once. "
                "Push failed → retried → succeeded."
            )
            print("Email, SMS, and Slack were NOT re-sent during the push retry.")
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
