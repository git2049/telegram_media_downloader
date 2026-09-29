"""Download runtime statistics."""
import asyncio
import threading
import time
from enum import Enum

from pyrogram import Client

from module.app import TaskNode
from module.runtime_health import runtime_health


class DownloadState(Enum):
    """Download state."""

    Downloading = 1
    StopDownload = 2


_download_result: dict = {}
_download_result_lock = threading.RLock()
_total_download_speed: int = 0
_total_download_size: int = 0
_last_download_time: float = time.time()
_download_state: DownloadState = DownloadState.Downloading
MAX_RESULTS_PER_CHAT = 2000
RESULT_RETENTION_SECONDS = 24 * 60 * 60


def get_download_result() -> dict:
    """Return a snapshot safe for iteration from the Flask thread."""
    with _download_result_lock:
        return {
            chat_id: {
                message_id: dict(value)
                for message_id, value in messages.items()
            }
            for chat_id, messages in _download_result.items()
        }


def remove_task_results(task_id: int):
    """Remove progress rows owned by a completed bot task."""
    with _download_result_lock:
        empty_chats = []
        for chat_id, messages in _download_result.items():
            for message_id in list(messages):
                if messages[message_id].get("task_id") == task_id:
                    messages.pop(message_id, None)
            if not messages:
                empty_chats.append(chat_id)
        for chat_id in empty_chats:
            _download_result.pop(chat_id, None)


def cleanup_download_result(
    retention_seconds: int = RESULT_RETENTION_SECONDS,
    max_results_per_chat: int = MAX_RESULTS_PER_CHAT,
):
    """Expire completed progress rows and cap per-chat runtime memory."""
    now = time.time()
    with _download_result_lock:
        empty_chats = []
        for chat_id, messages in _download_result.items():
            for message_id, value in list(messages.items()):
                completed = value.get("down_byte", 0) >= value.get("total_size", 0)
                end_time = float(value.get("end_time", now))
                if completed and now - end_time > retention_seconds:
                    messages.pop(message_id, None)

            while len(messages) > max_results_per_chat:
                removed = False
                for message_id, value in list(messages.items()):
                    if value.get("down_byte", 0) >= value.get("total_size", 0):
                        messages.pop(message_id, None)
                        removed = True
                        break
                if not removed:
                    break

            if not messages:
                empty_chats.append(chat_id)

        for chat_id in empty_chats:
            _download_result.pop(chat_id, None)


def get_total_download_speed() -> int:
    """Get total download speed."""
    with _download_result_lock:
        return _total_download_speed


def get_download_state() -> DownloadState:
    """Get current global download state."""
    return _download_state


# pylint: disable = W0603
def set_download_state(state: DownloadState):
    """Set global download state."""
    global _download_state
    _download_state = state


async def update_download_status(
    down_byte: int,
    total_size: int,
    message_id: int,
    file_name: str,
    start_time: float,
    node: TaskNode,
    client: Client,
):
    """Update download status and worker liveness."""
    cur_time = time.time()
    # pylint: disable = W0603
    global _total_download_speed
    global _total_download_size
    global _last_download_time

    if node.is_stop_transmission:
        client.stop_transmission()

    while get_download_state() == DownloadState.StopDownload:
        if node.is_stop_transmission:
            client.stop_transmission()
        await asyncio.sleep(1)

    runtime_health.mark_worker_progress()
    chat_id = node.chat_id

    with _download_result_lock:
        messages = _download_result.setdefault(chat_id, {})
        current = messages.get(message_id)

        if current:
            last_download_byte = current["down_byte"]
            last_time = current["end_time"]
            download_speed = current["download_speed"]
            each_second_total_download = current["each_second_total_download"]
            end_time = current["end_time"]

            delta = down_byte - last_download_byte
            _total_download_size += delta
            each_second_total_download += delta

            if cur_time - last_time >= 1.0:
                download_speed = int(
                    each_second_total_download / (cur_time - last_time)
                )
                end_time = cur_time
                each_second_total_download = 0

            current["down_byte"] = down_byte
            current["end_time"] = end_time
            current["download_speed"] = max(download_speed, 0)
            current["each_second_total_download"] = each_second_total_download
        else:
            duration = max(cur_time - start_time, 0.001)
            messages[message_id] = {
                "down_byte": down_byte,
                "total_size": total_size,
                "file_name": file_name,
                "start_time": start_time,
                "end_time": cur_time,
                "download_speed": down_byte / duration,
                "each_second_total_download": down_byte,
                "task_id": node.task_id,
            }
            _total_download_size += down_byte

        if cur_time - _last_download_time >= 1.0:
            _total_download_speed = int(
                _total_download_size / (cur_time - _last_download_time)
            )
            _total_download_speed = max(_total_download_speed, 0)
            _total_download_size = 0
            _last_download_time = cur_time

        while len(messages) > MAX_RESULTS_PER_CHAT:
            removed = False
            for old_message_id, value in list(messages.items()):
                if value.get("down_byte", 0) >= value.get("total_size", 0):
                    messages.pop(old_message_id, None)
                    removed = True
                    break
            if not removed:
                break
