"""Tests for runtime health state."""

import unittest

from module.runtime_health import RuntimeHealth


class RuntimeHealthTestCase(unittest.TestCase):
    """Validate liveness versus readiness semantics."""

    def test_healthy_after_required_rpc_probes(self):
        health = RuntimeHealth()
        health.configure(require_bot=True)
        startup = health.snapshot()
        self.assertTrue(startup["healthy"])
        self.assertFalse(startup["ready"])

        health.mark_user_rpc_ok()
        health.mark_bot_rpc_ok()

        snapshot = health.snapshot()
        self.assertTrue(snapshot["healthy"])
        self.assertTrue(snapshot["ready"])

    def test_floodwait_is_alive_but_not_ready(self):
        health = RuntimeHealth()
        health.configure(require_bot=True)
        health.mark_user_rpc_ok()
        health.mark_bot_rpc_ok()
        health.set_bot_backoff(120)

        snapshot = health.snapshot()
        self.assertTrue(snapshot["healthy"])
        self.assertFalse(snapshot["ready"])
        self.assertGreater(snapshot["bot_backoff_seconds"], 0)

    def test_failed_critical_task_marks_process_unhealthy(self):
        health = RuntimeHealth()
        health.configure(require_bot=False)
        health.mark_user_rpc_ok()
        health.mark_task_failed("download-worker-0", True, "boom")

        snapshot = health.snapshot()
        self.assertFalse(snapshot["healthy"])

    def test_stalled_active_transfer_marks_process_unhealthy(self):
        health = RuntimeHealth()
        health.configure(require_bot=False, queue_stall_seconds=1)
        health.mark_user_rpc_ok()
        health.download_started()
        health.last_worker_progress -= 10

        snapshot = health.snapshot()
        self.assertFalse(snapshot["healthy"])
        self.assertEqual(snapshot["active_downloads"], 1)
