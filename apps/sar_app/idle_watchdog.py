"""Idle-timeout watchdog for the app's own Databricks Apps compute.

Databricks Apps bill per hour while "Running" and have no built-in
scale-to-zero — a rarely-used app left running racks up cost with no
automatic stop. This module tracks time since the last completed search and
stops the app's own compute once ``IDLE_TIMEOUT_MINUTES`` (default 30)
elapses, using the app's own service principal (auto-injected
DATABRICKS_CLIENT_ID/SECRET) which needs CAN_MANAGE on the app itself.

Activity is defined as a completed search (see ``touch()`` call in app.py),
not general page interaction — adjusting sidebar widgets doesn't reset the
timer.
"""

from __future__ import annotations

import logging
import os
import threading
import time

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

_IDLE_TIMEOUT_SECONDS = int(os.getenv("IDLE_TIMEOUT_MINUTES", "30")) * 60
_POLL_INTERVAL_SECONDS = 15

_lock = threading.Lock()
_last_active = time.monotonic()
_started = False


def touch() -> None:
    """Record activity, resetting the idle countdown."""
    global _last_active
    with _lock:
        _last_active = time.monotonic()


def seconds_remaining() -> int:
    """Seconds left before the watchdog stops the app, floored at 0."""
    with _lock:
        elapsed = time.monotonic() - _last_active
    return max(0, int(_IDLE_TIMEOUT_SECONDS - elapsed))


def stop_app_now() -> None:
    """Stop this app's own compute immediately."""
    WorkspaceClient().apps.stop(name=os.environ["DATABRICKS_APP_NAME"])


def _watchdog_loop() -> None:
    while True:
        time.sleep(_POLL_INTERVAL_SECONDS)
        if seconds_remaining() == 0:
            logger.info(
                "SAR app idle for %s minutes — stopping.",
                _IDLE_TIMEOUT_SECONDS // 60,
            )
            try:
                stop_app_now()
            except Exception:  # noqa: BLE001
                logger.exception("Failed to stop idle app")
            return


def ensure_started() -> None:
    """Start the background watchdog thread once per process."""
    global _started
    with _lock:
        if _started:
            return
        _started = True
    threading.Thread(target=_watchdog_loop, daemon=True, name="idle-watchdog").start()
