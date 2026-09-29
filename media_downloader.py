"""Downloads media from telegram."""
import asyncio
import itertools
import logging
import os
import shutil
import time
from typing import List, Optional, Tuple, Union

import pyrogram
from loguru import logger
from pyrogram.types import Audio, Document, Photo, Video, VideoNote, Voice
from rich.logging import RichHandler

from module.app import Application, ChatDownloadConfig, DownloadStatus, TaskNode
from module.bot import get_download_bot_client, start_download_bot, stop_download_bot
from module.download_stat import cleanup_download_result, update_download_status
from module.get_chat_history_v2 import get_chat_history_v2
from module.language import _t
from module.pyrogram_extension import (
    HookClient,
    fetch_message,
    get_extension,
    record_download_status,
    report_bot_download_status,
    set_max_concurrent_transmissions,
    set_meta_data,
    update_cloud_upload_stat,
    upload_telegram_chat,
)
from module.runtime_health import create_supervised_task, runtime_health
from module.web import init_web
from utils.format import truncate_filename, validate_title
from utils.log import LogFilter
from utils.meta import print_meta
from utils.meta_data import MetaData

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
)

CONFIG_NAME = "config.yaml"
DATA_FILE_NAME = "data.yaml"
APPLICATION_NAME = "media_downloader"
app = Application(CONFIG_NAME, DATA_FILE_NAME, APPLICATION_NAME)

DOWNLOAD_QUEUE_MAXSIZE = 100
BULK_DOWNLOAD_PRIORITY = 10
MAX_INLINE_FLOOD_WAIT = 30
queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=DOWNLOAD_QUEUE_MAXSIZE)
_queue_sequence = itertools.count()
_initial_state_compacted = False
RETRY_TIME_OUT = 3

logging.getLogger("pyrogram.session.session").addFilter(LogFilter())
logging.getLogger("pyrogram.client").addFilter(LogFilter())

logging.getLogger("pyrogram").setLevel(logging.WARNING)


def _check_download_finish(media_size: int, download_path: str, ui_file_name: str):
    """Check download task if finish

    Parameters
    ----------
    media_size: int
        The size of the downloaded resource
    download_path: str
        Resource download hold path
    ui_file_name: str
        Really show file name

    """
    download_size = os.path.getsize(download_path)
    if media_size == download_size:
        logger.success(f"{_t('Successfully downloaded')} - {ui_file_name}")
    else:
        logger.warning(
            f"{_t('Media downloaded with wrong size')}: "
            f"{download_size}, {_t('actual')}: "
            f"{media_size}, {_t('file name')}: {ui_file_name}"
        )
        os.remove(download_path)
        raise pyrogram.errors.exceptions.bad_request_400.BadRequest()


def _move_to_download_path(temp_download_path: str, download_path: str):
    """Move file to download path

    Parameters
    ----------
    temp_download_path: str
        Temporary download path

    download_path: str
        Download path

    """

    directory, _ = os.path.split(download_path)
    os.makedirs(directory, exist_ok=True)
    shutil.move(temp_download_path, download_path)


def _check_timeout(retry: int, _: int):
    """Check if message download timeout, then add message id into failed_ids

    Parameters
    ----------
    retry: int
        Retry download message times

    message_id: int
        Try to download message 's id

    """
    if retry == 2:
        return True
    return False


def _can_download(_type: str, file_formats: dict, file_format: Optional[str]) -> bool:
    """
    Check if the given file format can be downloaded.

    Parameters
    ----------
    _type: str
        Type of media object.
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types
    file_format: str
        Format of the current file to be downloaded.

    Returns
    -------
    bool
        True if the file format can be downloaded else False.
    """
    if _type in ["audio", "document", "video"]:
        allowed_formats: list = file_formats[_type]
        if not file_format in allowed_formats and allowed_formats[0] != "all":
            return False
    return True


def _is_exist(file_path: str) -> bool:
    """
    Check if a file exists and it is not a directory.

    Parameters
    ----------
    file_path: str
        Absolute path of the file to be checked.

    Returns
    -------
    bool
        True if the file exists else False.
    """
    return not os.path.isdir(file_path) and os.path.exists(file_path)


# pylint: disable = R0912


async def _get_media_meta(
    chat_id: Union[int, str],
    message: pyrogram.types.Message,
    media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice],
    _type: str,
) -> Tuple[str, str, Optional[str]]:
    """Extract file name and file id from media object.

    Parameters
    ----------
    media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice]
        Media object to be extracted.
    _type: str
        Type of media object.

    Returns
    -------
    Tuple[str, str, Optional[str]]
        file_name, file_format
    """
    if _type in ["audio", "document", "video"]:
        # pylint: disable = C0301
        file_format: Optional[str] = media_obj.mime_type.split("/")[-1]  # type: ignore
    else:
        file_format = None

    file_name = None
    temp_file_name = None
    dirname = validate_title(f"{chat_id}")
    if message.chat and message.chat.title:
        dirname = validate_title(f"{message.chat.title}")

    if message.date:
        datetime_dir_name = message.date.strftime(app.date_format)
    else:
        datetime_dir_name = "0"

    if _type in ["voice", "video_note"]:
        # pylint: disable = C0209
        file_format = media_obj.mime_type.split("/")[-1]  # type: ignore
        file_save_path = app.get_file_save_path(_type, dirname, datetime_dir_name)
        file_name = "{} - {}_{}.{}".format(
            message.id,
            _type,
            media_obj.date.isoformat(),  # type: ignore
            file_format,
        )
        file_name = validate_title(file_name)
        temp_file_name = os.path.join(app.temp_save_path, dirname, file_name)

        file_name = os.path.join(file_save_path, file_name)
    else:
        file_name = getattr(media_obj, "file_name", None)
        caption = getattr(message, "caption", None)

        file_name_suffix = ".unknown"
        if not file_name:
            file_name_suffix = get_extension(
                media_obj.file_id, getattr(media_obj, "mime_type", "")
            )
        else:
            # file_name = file_name.split(".")[0]
            _, file_name_without_suffix = os.path.split(os.path.normpath(file_name))
            file_name, file_name_suffix = os.path.splitext(file_name_without_suffix)
            if not file_name_suffix:
                file_name_suffix = get_extension(
                    media_obj.file_id, getattr(media_obj, "mime_type", "")
                )

        if caption:
            caption = validate_title(caption)
            app.set_caption_name(chat_id, message.media_group_id, caption)
            app.set_caption_entities(
                chat_id, message.media_group_id, message.caption_entities
            )
        else:
            caption = app.get_caption_name(chat_id, message.media_group_id)

        if not file_name and message.photo:
            file_name = f"{message.photo.file_unique_id}"

        gen_file_name = (
            app.get_file_name(message.id, file_name, caption) + file_name_suffix
        )

        file_save_path = app.get_file_save_path(_type, dirname, datetime_dir_name)

        temp_file_name = os.path.join(app.temp_save_path, dirname, gen_file_name)

        file_name = os.path.join(file_save_path, gen_file_name)
    return truncate_filename(file_name), truncate_filename(temp_file_name), file_format


async def add_download_task(
    message: pyrogram.types.Message,
    node: TaskNode,
    priority: int = BULK_DOWNLOAD_PRIORITY,
):
    """Add a bounded download task with backpressure."""
    if message.empty:
        return False

    node.download_status[message.id] = DownloadStatus.Downloading
    await queue.put((priority, next(_queue_sequence), message, node))
    node.total_task += 1
    runtime_health.set_queue_size(queue.qsize())
    return True


async def save_msg_to_file(
    app, chat_id: Union[int, str], message: pyrogram.types.Message
):
    """Write message text into file"""
    dirname = validate_title(
        message.chat.title if message.chat and message.chat.title else str(chat_id)
    )
    datetime_dir_name = message.date.strftime(app.date_format) if message.date else "0"

    file_save_path = app.get_file_save_path("msg", dirname, datetime_dir_name)
    file_name = os.path.join(
        app.temp_save_path,
        file_save_path,
        f"{app.get_file_name(message.id, None, None)}.txt",
    )

    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    if _is_exist(file_name):
        return DownloadStatus.SkipDownload, None

    with open(file_name, "w", encoding="utf-8") as f:
        f.write(message.text or "")

    return DownloadStatus.SuccessDownload, file_name


async def download_task(
    client: pyrogram.Client, message: pyrogram.types.Message, node: TaskNode
):
    """Download one message and always close its accounting state."""

    download_status = DownloadStatus.FailedDownload
    file_name: Optional[str] = None
    file_size = 0

    try:
        download_status, file_name = await download_media(
            client, message, app.media_types, app.file_formats, node
        )

        if app.enable_download_txt and message.text and not message.media:
            download_status, file_name = await save_msg_to_file(
                app, node.chat_id, message
            )

        node.download_status[message.id] = download_status
        file_size = os.path.getsize(file_name) if file_name else 0

        try:
            await upload_telegram_chat(
                client,
                node.upload_user if node.upload_user else client,
                app,
                node,
                message,
                download_status,
                file_name,
            )

            if (
                file_name
                and not node.upload_telegram_chat_id
                and download_status is DownloadStatus.SuccessDownload
            ):
                ui_file_name = file_name
                if app.hide_file_name:
                    ui_file_name = f"****{os.path.splitext(file_name)[-1]}"
                if await app.upload_file(
                    file_name,
                    update_cloud_upload_stat,
                    (node, message.id, ui_file_name),
                ):
                    node.upload_success_count += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception(
                "Post-download processing failed for message {}: {}",
                message.id,
                exc,
            )
            runtime_health.record_error(
                f"post-download message {message.id}: {exc}"
            )
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # pylint: disable=broad-except
        download_status = DownloadStatus.FailedDownload
        file_name = None
        logger.exception("Download task {} failed: {}", message.id, exc)
        runtime_health.record_error(f"download message {message.id}: {exc}")
    finally:
        node.download_status[message.id] = download_status

        if not node.bot:
            try:
                app.set_download_id(node, message.id, download_status)
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("Failed to finalize message {}: {}", message.id, exc)
                runtime_health.record_error(
                    f"finalize message {message.id}: {exc}"
                )

        try:
            await report_bot_download_status(
                node.bot,
                node,
                download_status,
                file_size,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception(
                "Failed to report status for message {}: {}", message.id, exc
            )

        runtime_health.mark_worker_progress()


# pylint: disable = R0915,R0914


@record_download_status
async def download_media(
    client: pyrogram.client.Client,
    message: pyrogram.types.Message,
    media_types: List[str],
    file_formats: dict,
    node: TaskNode,
):
    """
    Download media from Telegram.

    Each of the files to download are retried 3 times with a
    delay of 5 seconds each.

    Parameters
    ----------
    client: pyrogram.client.Client
        Client to interact with Telegram APIs.
    message: pyrogram.types.Message
        Message object retrieved from telegram.
    media_types: list
        List of strings of media types to be downloaded.
        Ex : `["audio", "photo"]`
        Supported formats:
            * audio
            * document
            * photo
            * video
            * voice
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types.

    Returns
    -------
    int
        Current message id.
    """

    # pylint: disable = R0912

    file_name: str = ""
    ui_file_name: str = ""
    task_start_time: float = time.time()
    media_size = 0
    _media = None
    message = await fetch_message(client, message)
    if not message:
        return DownloadStatus.FailedDownload, None

    try:
        for _type in media_types:
            _media = getattr(message, _type, None)
            if _media is None:
                continue
            file_name, temp_file_name, file_format = await _get_media_meta(
                node.chat_id, message, _media, _type
            )
            media_size = getattr(_media, "file_size", 0)

            ui_file_name = file_name
            if app.hide_file_name:
                ui_file_name = f"****{os.path.splitext(file_name)[-1]}"

            if _can_download(_type, file_formats, file_format):
                if _is_exist(file_name):
                    file_size = os.path.getsize(file_name)
                    if file_size or file_size == media_size:
                        logger.info(
                            f"id={message.id} {ui_file_name} "
                            f"{_t('already download,download skipped')}.\n"
                        )

                        return DownloadStatus.SkipDownload, None
            else:
                return DownloadStatus.SkipDownload, None

            break
    except Exception as e:
        logger.error(
            f"Message[{message.id}]: "
            f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
            exc_info=True,
        )
        return DownloadStatus.FailedDownload, None
    if _media is None:
        return DownloadStatus.SkipDownload, None

    message_id = message.id

    for retry in range(3):
        try:
            temp_download_path = await client.download_media(
                message,
                file_name=temp_file_name,
                progress=update_download_status,
                progress_args=(
                    message_id,
                    ui_file_name,
                    task_start_time,
                    node,
                    client,
                ),
            )

            if temp_download_path and isinstance(temp_download_path, str):
                _check_download_finish(media_size, temp_download_path, ui_file_name)
                await asyncio.sleep(0.5)
                _move_to_download_path(temp_download_path, file_name)
                # TODO: if not exist file size or media
                return DownloadStatus.SuccessDownload, file_name
        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
            logger.warning(
                f"Message[{message.id}]: {_t('file reference expired, refetching')}..."
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            refreshed_message = await fetch_message(client, message)
            if not refreshed_message:
                break
            message = refreshed_message
            if _check_timeout(retry, message.id):
                # pylint: disable = C0301
                logger.error(
                    f"Message[{message.id}]: "
                    f"{_t('file reference expired for 3 retries, download skipped.')}"
                )
        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
            logger.warning("Message[{}]: FlowWait {}", message.id, wait_err.value)
            if wait_err.value > MAX_INLINE_FLOOD_WAIT:
                runtime_health.record_error(
                    f"download FloodWait {wait_err.value}s for message {message.id}"
                )
                break
            await asyncio.sleep(wait_err.value)
            _check_timeout(retry, message.id)
        except TypeError:
            # pylint: disable = C0301
            logger.warning(
                f"{_t('Timeout Error occurred when downloading Message')}[{message.id}], "
                f"{_t('retrying after')} {RETRY_TIME_OUT} {_t('seconds')}"
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            if _check_timeout(retry, message.id):
                logger.error(
                    f"Message[{message.id}]: {_t('Timing out after 3 reties, download skipped.')}"
                )
        except Exception as e:
            # pylint: disable = C0301
            logger.error(
                f"Message[{message.id}]: "
                f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
                exc_info=True,
            )
            break

    return DownloadStatus.FailedDownload, None


def _load_config():
    """Load config"""
    app.load_config()


def _check_config() -> bool:
    """Check config"""
    print_meta(logger)
    try:
        _load_config()
        logger.add(
            os.path.join(app.log_file_path, "tdl.log"),
            rotation="10 MB",
            retention="10 days",
            level=app.log_level,
        )
    except Exception as e:
        logger.exception(f"load config error: {e}")
        return False

    return True


async def worker(client: pyrogram.client.Client):
    """Work for download task."""
    while app.is_running:
        try:
            item = await queue.get()
        except asyncio.CancelledError:
            raise

        transfer_started = False
        try:
            if len(item) == 4:
                _, _, message, node = item
            else:
                message, node = item

            if node.is_stop_transmission:
                node.download_status[message.id] = DownloadStatus.SkipDownload
                if not node.bot:
                    app.set_download_id(
                        node, message.id, DownloadStatus.SkipDownload
                    )
                await report_bot_download_status(
                    node.bot,
                    node,
                    DownloadStatus.SkipDownload,
                )
                runtime_health.mark_worker_progress()
                continue

            runtime_health.download_started()
            transfer_started = True
            if node.client:
                await download_task(node.client, message, node)
            else:
                await download_task(client, message, node)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Worker error: {}", exc)
            runtime_health.record_error(f"worker: {exc}")
        finally:
            if transfer_started:
                runtime_health.download_finished()
            if hasattr(queue, "task_done"):
                queue.task_done()
            runtime_health.set_queue_size(queue.qsize())


async def download_chat_task(
    client: pyrogram.Client,
    chat_download_config: ChatDownloadConfig,
    node: TaskNode,
):
    """Download all task"""
    node.is_running = True
    node.collection_complete = False
    messages_iter = get_chat_history_v2(
        client,
        node.chat_id,
        limit=node.limit,
        max_id=node.end_offset_id,
        offset_id=chat_download_config.last_read_message_id,
        reverse=True,
    )

    chat_download_config.node = node

    if chat_download_config.ids_to_retry:
        logger.info(f"{_t('Downloading files failed during last run')}...")
        skipped_messages: list = await client.get_messages(  # type: ignore
            chat_id=node.chat_id, message_ids=chat_download_config.ids_to_retry
        )

        for message in skipped_messages:
            await add_download_task(message, node)

    async for message in messages_iter:  # type: ignore
        meta_data = MetaData()

        caption = message.caption
        if caption:
            caption = validate_title(caption)
            app.set_caption_name(node.chat_id, message.media_group_id, caption)
            app.set_caption_entities(
                node.chat_id, message.media_group_id, message.caption_entities
            )
        else:
            caption = app.get_caption_name(node.chat_id, message.media_group_id)
        set_meta_data(meta_data, message, caption)

        if app.need_skip_message(chat_download_config, message.id):
            continue

        if app.exec_filter(chat_download_config, meta_data):
            await add_download_task(message, node)
        else:
            node.download_status[message.id] = DownloadStatus.SkipDownload
            if message.media_group_id:
                await upload_telegram_chat(
                    client,
                    node.upload_user,
                    app,
                    node,
                    message,
                    DownloadStatus.SkipDownload,
                )

    chat_download_config.need_check = True
    chat_download_config.total_task = node.total_task
    node.collection_complete = True
    node.is_running = True


async def download_all_chat(client: pyrogram.Client):
    """Download All chat"""
    for key, value in app.chat_download_config.items():
        value.node = TaskNode(chat_id=key)
        try:
            await download_chat_task(client, value, value.node)
        except Exception as e:
            logger.warning(f"Download {key} error: {e}")
        finally:
            value.need_check = True
            value.node.collection_complete = True


def _compact_finished_initial_state():
    """Persist completed initial scans and release their success-only state."""
    # pylint: disable=global-statement
    global _initial_state_compacted
    if _initial_state_compacted or not app.chat_download_config:
        return

    all_finished = all(
        value.need_check and value.total_task == value.finish_task
        for value in app.chat_download_config.values()
    )
    if not all_finished:
        return

    app.update_config()
    terminal_status = (
        DownloadStatus.SuccessDownload,
        DownloadStatus.SkipDownload,
    )
    for value in app.chat_download_config.values():
        value.node.download_status = {
            message_id: status
            for message_id, status in value.node.download_status.items()
            if status not in terminal_status
        }
        value.node.upload_stat_dict.clear()
        value.node.cloud_drive_upload_stat_dict.clear()

    _initial_state_compacted = True


async def health_monitor(client: pyrogram.Client):
    """Probe Telegram RPC liveness and clean bounded runtime state."""
    while app.is_running:
        runtime_health.set_queue_size(queue.qsize())

        try:
            await asyncio.wait_for(client.get_me(), timeout=15)
            runtime_health.mark_user_rpc_ok()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            runtime_health.record_error(f"user client health: {exc}")
            logger.warning("User client health check failed: {}", exc)

        if app.bot_token:
            bot_client = get_download_bot_client()
            if bot_client:
                dispatcher = getattr(bot_client, "dispatcher", None)
                dispatcher_tasks = getattr(
                    dispatcher, "handler_worker_tasks", []
                )
                dispatcher_alive = (
                    bool(getattr(bot_client, "is_connected", False))
                    and bool(getattr(bot_client, "is_initialized", False))
                    and bool(dispatcher_tasks)
                    and all(not task.done() for task in dispatcher_tasks)
                )
                if dispatcher_alive:
                    runtime_health.mark_task_running("bot-dispatcher", True)
                else:
                    runtime_health.mark_task_failed(
                        "bot-dispatcher",
                        True,
                        "Pyrogram dispatcher worker stopped",
                    )

                if runtime_health.bot_backoff_remaining() == 0:
                    try:
                        await asyncio.wait_for(bot_client.get_me(), timeout=15)
                        runtime_health.mark_bot_rpc_ok()
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # pylint: disable=broad-except
                        runtime_health.record_error(f"bot client health: {exc}")
                        logger.warning("Bot client health check failed: {}", exc)

        cleanup_download_result()
        _compact_finished_initial_state()
        await asyncio.sleep(30)


async def run_until_all_task_finish():
    """Normal download"""
    while True:
        finish: bool = True
        for _, value in app.chat_download_config.items():
            if not value.need_check or value.total_task != value.finish_task:
                finish = False

        if (not app.bot_token and finish) or app.restart_program:
            break

        await asyncio.sleep(1)


def _exec_loop():
    """Exec loop"""

    app.loop.run_until_complete(run_until_all_task_finish())


async def start_server(client: pyrogram.Client):
    """
    Start the server using the provided client.
    """
    await client.start()


async def stop_server(client: pyrogram.Client):
    """
    Stop the server using the provided client.
    """
    if not getattr(client, "is_connected", False):
        return
    try:
        await client.stop()
    except ConnectionError:
        pass


def main():
    """Main function of the downloader."""
    tasks = []
    client = HookClient(
        "media_downloader",
        api_id=app.api_id,
        api_hash=app.api_hash,
        proxy=app.proxy,
        workdir=app.session_file_path,
        start_timeout=app.start_timeout,
        no_updates=True,
    )
    try:
        app.pre_run()
        init_web(app)

        set_max_concurrent_transmissions(client, app.max_concurrent_transmissions)

        runtime_health.configure(require_bot=bool(app.bot_token))
        app.loop.run_until_complete(start_server(client))
        runtime_health.mark_user_rpc_ok()
        logger.success(_t("Successfully started (Press Ctrl+C to stop)"))

        tasks.append(
            create_supervised_task(
                app.loop,
                lambda: download_all_chat(client),
                "initial-chat-scan",
                restart=False,
                critical=False,
            )
        )
        for worker_id in range(app.max_download_task):
            tasks.append(
                create_supervised_task(
                    app.loop,
                    lambda: worker(client),
                    f"download-worker-{worker_id}",
                    restart=True,
                    critical=True,
                )
            )

        if app.bot_token:
            app.loop.run_until_complete(
                start_download_bot(app, client, add_download_task, download_chat_task)
            )

        tasks.append(
            create_supervised_task(
                app.loop,
                lambda: health_monitor(client),
                "health-monitor",
                restart=True,
                critical=True,
            )
        )
        _exec_loop()
    except KeyboardInterrupt:
        logger.info(_t("KeyboardInterrupt"))
    except Exception as e:
        logger.exception("{}", e)
    finally:
        app.is_running = False
        for task in tasks:
            task.cancel()
        if app.bot_token:
            app.loop.run_until_complete(stop_download_bot())
        app.loop.run_until_complete(stop_server(client))
        logger.info(_t("Stopped!"))
        # check_for_updates(app.proxy)
        logger.info(f"{_t('update config')}......")
        app.update_config()
        logger.success(
            f"{_t('Updated last read message_id to config file')},"
            f"{_t('total download')} {app.total_download_task}, "
            f"{_t('total upload file')} "
            f"{app.cloud_drive_config.total_upload_success_file_count}"
        )


if __name__ == "__main__":
    if _check_config():
        main()
