"""Regression tests for long-running downloader stability."""

import asyncio
import unittest

from module.app import DownloadStatus, TaskNode
from module.pyrogram_extension import record_download_status, reset_download_cache


class Message:
    """Minimal message object for the status-cache decorator."""

    def __init__(self, message_id):
        self.id = message_id


class RuntimeStabilityTestCase(unittest.TestCase):
    """Ensure transient failures do not leave messages permanently Downloading."""

    def setUp(self):
        reset_download_cache()
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()

    def test_exception_does_not_poison_download_cache(self):
        calls = {"count": 0}

        @record_download_status
        async def failing_download(_, __, ___, ____, _____):
            calls["count"] += 1
            raise OSError("connection lost")

        node = TaskNode(chat_id=123)
        message = Message(456)

        first = self.loop.run_until_complete(
            failing_download(None, message, [], {}, node)
        )
        second = self.loop.run_until_complete(
            failing_download(None, message, [], {}, node)
        )

        self.assertEqual(first, (DownloadStatus.FailedDownload, None))
        self.assertEqual(second, (DownloadStatus.FailedDownload, None))
        self.assertEqual(calls["count"], 2)
