"""Runtime liveness and background-task supervision helpers."""

import asyncio
import threading
import time
from typing import Awaitable, Callable, Dict

from loguru import logger


TaskFactory = Callable[[], Awaitable[None]]


# pylint: disable=too-many-instance-attributes
class RuntimeHealth:
    """Thread-safe runtime health state shared by asyncio and Flask threads."""

    def __init__(self):
        self._lock = threading.RLock()
        self.started_at = time.time()
        self.require_user = True
        self.require_bot = False
        self.stale_seconds = 120
        self.startup_grace_seconds = 120
        self.queue_stall_seconds = 300
        self.last_user_rpc_ok = 0.0
        self.last_bot_rpc_ok = 0.0
        self.last_bot_update = 0.0
        self.last_worker_progress = time.time()
        self.queue_size = 0
        self.active_downloads = 0
        self.bot_backoff_until = 0.0
        self.last_error = ""
        self.task_states: Dict[str, dict] = {}

    def configure(
        self,
        require_bot: bool = False,
        stale_seconds: int = 120,
        startup_grace_seconds: int = 120,
        queue_stall_seconds: int = 300,
    ):
        """Configure health requirements for this process."""
        with self._lock:
            self.require_user = True
            self.require_bot = require_bot
            self.stale_seconds = stale_seconds
            self.startup_grace_seconds = startup_grace_seconds
            self.queue_stall_seconds = queue_stall_seconds

    def mark_user_rpc_ok(self):
        """Record a successful user-client Telegram RPC."""
        with self._lock:
            self.last_user_rpc_ok = time.time()

    def mark_bot_rpc_ok(self):
        """Record a successful bot-client Telegram RPC."""
        with self._lock:
            self.last_bot_rpc_ok = time.time()

    def mark_bot_update(self):
        """Record receipt of a Telegram update by the bot dispatcher."""
        with self._lock:
            self.last_bot_update = time.time()

    def mark_worker_progress(self):
        """Record useful progress by a download worker."""
        with self._lock:
            self.last_worker_progress = time.time()

    def set_queue_size(self, queue_size: int):
        """Record the current bounded download queue size."""
        with self._lock:
            self.queue_size = max(0, int(queue_size))

    def download_started(self):
        """Record one active transfer handled by a worker."""
        with self._lock:
            self.active_downloads += 1
            self.last_worker_progress = time.time()

    def download_finished(self):
        """Record completion or release of one active worker transfer."""
        with self._lock:
            self.active_downloads = max(0, self.active_downloads - 1)
            self.last_worker_progress = time.time()

    def set_bot_backoff(self, seconds: int):
        """Suspend non-essential bot status updates during Telegram FloodWait."""
        seconds = max(0, int(seconds))
        with self._lock:
            self.bot_backoff_until = max(
                self.bot_backoff_until, time.time() + seconds
            )

    def bot_backoff_remaining(self) -> int:
        """Return remaining bot FloodWait backoff in seconds."""
        with self._lock:
            return max(0, int(self.bot_backoff_until - time.time()))

    def record_error(self, message: str):
        """Record the latest runtime error for diagnostics."""
        with self._lock:
            self.last_error = str(message)

    def mark_task_running(self, name: str, critical: bool):
        """Record a supervised task as running."""
        with self._lock:
            state = self.task_states.setdefault(
                name, {"failures": 0, "critical": critical}
            )
            state.update(
                {
                    "status": "running",
                    "critical": critical,
                    "last_change": time.time(),
                }
            )

    def mark_task_restarting(self, name: str, critical: bool, error: str):
        """Record a supervised task restart after failure."""
        with self._lock:
            state = self.task_states.setdefault(
                name, {"failures": 0, "critical": critical}
            )
            state["failures"] = int(state.get("failures", 0)) + 1
            state.update(
                {
                    "status": "restarting",
                    "critical": critical,
                    "error": error,
                    "last_change": time.time(),
                }
            )
            self.last_error = f"{name}: {error}"

    def mark_task_failed(self, name: str, critical: bool, error: str):
        """Record a supervised task as permanently failed."""
        with self._lock:
            state = self.task_states.setdefault(
                name, {"failures": 0, "critical": critical}
            )
            state["failures"] = int(state.get("failures", 0)) + 1
            state.update(
                {
                    "status": "failed",
                    "critical": critical,
                    "error": error,
                    "last_change": time.time(),
                }
            )
            self.last_error = f"{name}: {error}"

    def mark_task_completed(self, name: str, critical: bool):
        """Record a supervised task that exited normally."""
        with self._lock:
            state = self.task_states.setdefault(
                name, {"failures": 0, "critical": critical}
            )
            state.update(
                {
                    "status": "completed",
                    "critical": critical,
                    "last_change": time.time(),
                }
            )

    def mark_task_stopped(self, name: str, critical: bool):
        """Record cancellation of a supervised task."""
        with self._lock:
            state = self.task_states.setdefault(
                name, {"failures": 0, "critical": critical}
            )
            state.update(
                {
                    "status": "stopped",
                    "critical": critical,
                    "last_change": time.time(),
                }
            )

    @staticmethod
    def _age(now: float, timestamp: float):
        """Return age in seconds, or None when no observation exists."""
        if timestamp <= 0:
            return None
        return round(now - timestamp, 1)

    def snapshot(self) -> dict:
        """Return health/readiness state suitable for watchdog endpoints."""
        with self._lock:
            now = time.time()
            user_age = self._age(now, self.last_user_rpc_ok)
            bot_age = self._age(now, self.last_bot_rpc_ok)
            bot_update_age = self._age(now, self.last_bot_update)
            worker_age = self._age(now, self.last_worker_progress)
            backoff = max(0, int(self.bot_backoff_until - now))
            in_startup_grace = (
                now - self.started_at <= self.startup_grace_seconds
            )

            user_observed = user_age is not None
            bot_observed = bot_age is not None
            user_ok = (not self.require_user) or (
                (not user_observed and in_startup_grace)
                or (user_observed and user_age <= self.stale_seconds)
            )
            bot_rpc_ok = (
                (not self.require_bot)
                or backoff > 0
                or (not bot_observed and in_startup_grace)
                or (bot_observed and bot_age <= self.stale_seconds)
            )
            work_pending = self.queue_size > 0 or self.active_downloads > 0
            queue_progressing = (not work_pending) or (
                worker_age is not None
                and worker_age <= self.queue_stall_seconds
            )

            critical_ok = True
            critical_running = True
            for state in self.task_states.values():
                if not state.get("critical"):
                    continue
                status = state.get("status")
                if status not in ("running", "restarting"):
                    critical_ok = False
                if status != "running":
                    critical_running = False

            healthy = user_ok and bot_rpc_ok and queue_progressing and critical_ok
            probes_ready = (
                (not self.require_user or user_observed)
                and (not self.require_bot or bot_observed)
            )
            ready = (
                healthy
                and probes_ready
                and critical_running
                and backoff == 0
            )

            return {
                "healthy": healthy,
                "ready": ready,
                "uptime_seconds": int(now - self.started_at),
                "startup_grace": in_startup_grace,
                "user_rpc_alive": user_ok,
                "bot_rpc_alive": bot_rpc_ok,
                "bot_backoff_seconds": backoff,
                "bot_update_age_seconds": bot_update_age,
                "user_rpc_age_seconds": user_age,
                "bot_rpc_age_seconds": bot_age,
                "worker_progress_age_seconds": worker_age,
                "queue_size": self.queue_size,
                "active_downloads": self.active_downloads,
                "queue_progressing": queue_progressing,
                "tasks": {
                    key: dict(value) for key, value in self.task_states.items()
                },
                "last_error": self.last_error,
            }


runtime_health = RuntimeHealth()


async def _supervise(
    factory: TaskFactory,
    name: str,
    restart: bool,
    critical: bool,
    restart_delay: int,
):
    """Run one background task and optionally restart it after failures."""
    while True:
        runtime_health.mark_task_running(name, critical)
        try:
            await factory()
        except asyncio.CancelledError:
            runtime_health.mark_task_stopped(name, critical)
            raise
        except Exception as exc:  # pylint: disable=broad-except
            error = f"{exc.__class__.__name__}: {exc}"
            if not restart:
                runtime_health.mark_task_failed(name, critical, error)
                logger.exception("Background task {} failed", name)
                return
            runtime_health.mark_task_restarting(name, critical, error)
            logger.exception("Background task {} failed; restarting", name)
            await asyncio.sleep(restart_delay)
            continue

        runtime_health.mark_task_completed(name, critical)
        return


# pylint: disable=too-many-arguments
def create_supervised_task(
    loop: asyncio.AbstractEventLoop,
    factory: TaskFactory,
    name: str,
    restart: bool = True,
    critical: bool = True,
    restart_delay: int = 5,
) -> asyncio.Task:
    """Create a task that records failures and optionally restarts on errors."""
    return loop.create_task(
        _supervise(factory, name, restart, critical, restart_delay)
    )
