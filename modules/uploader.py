"""
Telegram Uploader Module using direct Bot Token HTTP API.
Does not require api_id or api_hash.

Features:
1. Advanced Caption Templating (supports {title}, {size}, {duration}, custom tags, safe formatting, auto-trimming)
2. Dynamic Stream Chunking for large video files (memory-efficient async chunked streaming)
3. Auto-fallback from sendVideo to sendDocument if format/stream errors occur
4. Rich Error Logging with Telegram error codes, user-friendly descriptions, and actionable troubleshooting tips
5. Rate limit detection (HTTP 429 FloodWait with retry_after backoff)
6. Enforced inter-upload cooldown between consecutive uploads
"""

import os
import time
import string
import logging
import asyncio
import datetime
from typing import Optional, Dict, Any, Tuple, AsyncGenerator, Union
import aiohttp

from modules.config import config

logger = logging.getLogger(__name__)

DEFAULT_API_BASE = config.TELEGRAM_API_BASE
DEFAULT_COOLDOWN = config.UPLOAD_COOLDOWN
DEFAULT_CAPTION_TEMPLATE = config.CAPTION_TEMPLATE
DEFAULT_PARSE_MODE = config.TELEGRAM_PARSE_MODE
TELEGRAM_MAX_CAPTION_LENGTH = 1024


# ==============================================================================
# Helper Utilities: Formatting & Sizing
# ==============================================================================

def format_file_size(size_bytes: Union[int, float]) -> str:
    """Format byte size into human-readable representation (e.g., 45.20 MB, 1.25 GB)."""
    try:
        size_bytes = float(size_bytes)
    except (ValueError, TypeError):
        return "0 B"

    if size_bytes <= 0:
        return "0 B"

    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    while size_bytes >= 1024.0 and unit_index < len(units) - 1:
        size_bytes /= 1024.0
        unit_index += 1

    if unit_index == 0:
        return f"{int(size_bytes)} B"
    return f"{size_bytes:.2f} {units[unit_index]}"


def format_duration(seconds: Union[int, float]) -> str:
    """Format duration in seconds into HH:MM:SS or MM:SS."""
    try:
        total_seconds = int(seconds)
    except (ValueError, TypeError):
        return "00:00"

    if total_seconds <= 0:
        return "00:00"

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    secs = total_seconds % 60

    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def get_dynamic_chunk_size(file_size: int, user_override: Optional[int] = None) -> int:
    """
    Determines optimal upload chunk size dynamically based on file size to balance
    memory consumption and network throughput.
    """
    if user_override and user_override > 0:
        return user_override

    # Dynamic tiered chunk sizing
    if file_size < 10 * 1024 * 1024:          # < 10 MB
        return 64 * 1024                      # 64 KB
    elif file_size < 100 * 1024 * 1024:       # 10 MB - 100 MB
        return 256 * 1024                     # 256 KB
    elif file_size < 500 * 1024 * 1024:       # 100 MB - 500 MB
        return 1024 * 1024                    # 1 MB
    else:                                     # > 500 MB
        return 4 * 1024 * 1024                # 4 MB


class SafeFormatter(string.Formatter):
    """
    Safe string formatter that leaves missing placeholders or replaces them with empty string
    instead of raising KeyError.
    """
    def get_value(self, key, args, kwargs):
        if isinstance(key, str):
            return kwargs.get(key, "")
        return super().get_value(key, args, kwargs)


# ==============================================================================
# Telegram Error Diagnostics & Mapping
# ==============================================================================

class TelegramErrorDetails:
    """Rich structured details about a Telegram API response failure."""

    def __init__(
        self,
        status_code: int,
        raw_description: str,
        friendly_name: str,
        friendly_description: str,
        actionable_solution: str,
        retry_after: Optional[int] = None,
        is_retryable: bool = False,
        is_format_error: bool = False,
    ):
        self.status_code = status_code
        self.raw_description = raw_description
        self.friendly_name = friendly_name
        self.friendly_description = friendly_description
        self.actionable_solution = actionable_solution
        self.retry_after = retry_after
        self.is_retryable = is_retryable
        self.is_format_error = is_format_error

    def format_log(self, context: str = "") -> str:
        """Format a rich multiline diagnostic message."""
        lines = [
            f"╔═══════════════════ TELEGRAM API ERROR [{self.status_code}] ═══════════════════",
            f"║ Issue       : {self.friendly_name}",
            f"║ Context     : {context}" if context else None,
            f"║ Raw Error   : {self.raw_description}",
            f"║ Details     : {self.friendly_description}",
            f"║ Action Plan : {self.actionable_solution}",
        ]
        if self.retry_after:
            lines.append(f"║ Rate Limit  : Retry after {self.retry_after}s")
        lines.append("╚" + "═" * 70)
        return "\n".join(line for line in lines if line is not None)

    def __str__(self) -> str:
        return f"[{self.status_code}] {self.friendly_name}: {self.friendly_description} (Raw: {self.raw_description})"


def parse_telegram_error(
    status_code: int,
    description: str,
    parameters: Optional[Dict[str, Any]] = None
) -> TelegramErrorDetails:
    """Parses raw Telegram error response into rich, friendly actionable diagnostics."""
    parameters = parameters or {}
    retry_after = parameters.get("retry_after")
    desc_lower = (description or "").lower()

    # Format errors that warrant auto-fallback from sendVideo to sendDocument
    format_error_keywords = [
        "wrong file identifier",
        "can't use file",
        "wrong file format",
        "video stream is not suitable",
        "invalid file contents",
        "can't parse video",
        "video_file_invalid",
        "failed to get http url content",
        "wrong remote file id",
        "file is not a video",
        "bad video",
    ]
    is_format_error = any(keyword in desc_lower for keyword in format_error_keywords)

    if status_code == 429 or retry_after:
        return TelegramErrorDetails(
            status_code=429,
            raw_description=description,
            friendly_name="Rate Limit Exceeded (FloodWait)",
            friendly_description="Telegram is temporarily rate-limiting requests from this bot.",
            actionable_solution=f"Wait {retry_after or 15}s before sending next request. Increase UPLOAD_COOLDOWN.",
            retry_after=retry_after or 15,
            is_retryable=True,
            is_format_error=False,
        )

    if status_code == 400:
        if is_format_error:
            return TelegramErrorDetails(
                status_code=400,
                raw_description=description,
                friendly_name="Video Format / Stream Incompatible",
                friendly_description="Telegram video encoder rejected the video container, stream, or format.",
                actionable_solution="Triggering auto-fallback to sendDocument (file mode).",
                retry_after=None,
                is_retryable=False,
                is_format_error=True,
            )
        elif "chat not found" in desc_lower:
            return TelegramErrorDetails(
                status_code=400,
                raw_description=description,
                friendly_name="Target Chat Not Found",
                friendly_description="Telegram could not find the target channel/group with TELEGRAM_CHAT_ID.",
                actionable_solution="Check TELEGRAM_CHAT_ID (e.g. -100xxxxxxxxxx) and ensure bot is added to channel.",
                is_retryable=False,
            )
        elif "media_caption_too_long" in desc_lower or "caption is too long" in desc_lower:
            return TelegramErrorDetails(
                status_code=400,
                raw_description=description,
                friendly_name="Caption Exceeds 1024 Characters",
                friendly_description="The generated caption exceeds Telegram's 1024-character media limit.",
                actionable_solution="The uploader will automatically truncate captions to 1024 characters.",
                is_retryable=True,
            )
        elif "can't parse entities" in desc_lower or "unclosed tag" in desc_lower:
            return TelegramErrorDetails(
                status_code=400,
                raw_description=description,
                friendly_name="Caption Markup Parsing Error",
                friendly_description="The caption template contains invalid HTML or Markdown tags.",
                actionable_solution="Verify HTML/Markdown syntax in CAPTION_TEMPLATE or escape special characters.",
                is_retryable=False,
            )
        else:
            return TelegramErrorDetails(
                status_code=400,
                raw_description=description,
                friendly_name="Bad Request",
                friendly_description=f"Telegram rejected the request parameters: {description}",
                actionable_solution="Check payload parameters, chat ID, and file properties.",
                is_format_error=True,  # Generic 400 during video upload can attempt document fallback
            )

    elif status_code == 401:
        return TelegramErrorDetails(
            status_code=401,
            raw_description=description,
            friendly_name="Unauthorized Bot Token",
            friendly_description="The provided TELEGRAM_BOT_TOKEN is invalid or has been revoked.",
            actionable_solution="Obtain a valid token from @BotFather and update your environment variables.",
            is_retryable=False,
        )

    elif status_code == 403:
        if "blocked by the user" in desc_lower:
            return TelegramErrorDetails(
                status_code=403,
                raw_description=description,
                friendly_name="Bot Blocked by User",
                friendly_description="The destination user has blocked the bot.",
                actionable_solution="Ask user to unblock the bot or send /start.",
                is_retryable=False,
            )
        elif "kicked" in desc_lower:
            return TelegramErrorDetails(
                status_code=403,
                raw_description=description,
                friendly_name="Bot Kicked from Channel/Group",
                friendly_description="The bot was removed from the destination chat.",
                actionable_solution="Re-add the bot to the channel/group.",
                is_retryable=False,
            )
        else:
            return TelegramErrorDetails(
                status_code=403,
                raw_description=description,
                friendly_name="Bot Permission Forbidden",
                friendly_description="The bot does not have permission to post media/messages in the target channel.",
                actionable_solution="Promote the bot to Administrator with 'Post Messages' permission in channel settings.",
                is_retryable=False,
            )

    elif status_code == 404:
        return TelegramErrorDetails(
            status_code=404,
            raw_description=description,
            friendly_name="API Endpoint Not Found",
            friendly_description="The requested Telegram API endpoint or bot URL was not found.",
            actionable_solution="Verify TELEGRAM_API_BASE and check bot token formatting.",
            is_retryable=False,
        )

    elif status_code == 413:
        return TelegramErrorDetails(
            status_code=413,
            raw_description=description,
            friendly_name="Payload Too Large (>50MB Bot Limit)",
            friendly_description="The file size exceeds the Telegram Bot API HTTP limit (50MB).",
            actionable_solution="Use a local Telegram Bot API server for files up to 2GB, or compress the video before upload.",
            is_retryable=False,
        )

    elif status_code in (500, 502, 503, 504):
        return TelegramErrorDetails(
            status_code=status_code,
            raw_description=description,
            friendly_name=f"Telegram Server Error ({status_code})",
            friendly_description="Telegram's servers are experiencing temporary downtime or network issues.",
            actionable_solution="Retrying automatically with exponential backoff.",
            is_retryable=True,
        )

    return TelegramErrorDetails(
        status_code=status_code,
        raw_description=description,
        friendly_name=f"Telegram API Error ({status_code})",
        friendly_description=description,
        actionable_solution="Review logs and verify network connection and parameters.",
        is_retryable=False,
    )


# ==============================================================================
# Async Stream Chunking Helper
# ==============================================================================

async def async_file_chunk_streamer(file_path: str, chunk_size: int) -> AsyncGenerator[bytes, None]:
    """
    Asynchronously yields chunks of a file without reading the whole file into RAM.
    Uses run_in_executor to avoid blocking the asyncio event loop during disk reads.
    """
    loop = asyncio.get_running_loop()
    with open(file_path, "rb") as f:
        while True:
            chunk = await loop.run_in_executor(None, f.read, chunk_size)
            if not chunk:
                break
            yield chunk


# ==============================================================================
# Telegram Bot Uploader Class
# ==============================================================================

class TelegramBotUploader:
    """
    High-performance async Telegram Uploader using direct Bot Token HTTP API.
    Features stream chunking, advanced caption templating, auto-fallback, and rich diagnostics.
    """

    def __init__(
        self,
        bot_token: Optional[str] = None,
        chat_id: Optional[str] = None,
        api_base: str = DEFAULT_API_BASE,
        cooldown: int = DEFAULT_COOLDOWN,
        default_template: str = DEFAULT_CAPTION_TEMPLATE,
        parse_mode: Optional[str] = DEFAULT_PARSE_MODE,
        chunk_size_override: Optional[int] = None
    ):
        self.bot_token = (bot_token if bot_token is not None else config.TELEGRAM_BOT_TOKEN).strip()
        self.chat_id = (chat_id if chat_id is not None else config.TELEGRAM_CHAT_ID).strip()
        self.api_base = api_base.rstrip("/")
        self.cooldown = cooldown
        self.default_template = default_template
        self.parse_mode = parse_mode
        self.chunk_size_override = chunk_size_override
        self.last_upload_time = 0.0
        self.formatter = SafeFormatter()

    @property
    def bot_url(self) -> str:
        return f"{self.api_base}/bot{self.bot_token}"

    async def verify_bot_token(self) -> Tuple[bool, str]:
        """Verify bot token and connectivity by calling Telegram getMe."""
        if not self.bot_token:
            err = TelegramErrorDetails(
                status_code=401,
                raw_description="TELEGRAM_BOT_TOKEN is empty",
                friendly_name="Bot Token Missing",
                friendly_description="No TELEGRAM_BOT_TOKEN found in environment or constructor.",
                actionable_solution="Set TELEGRAM_BOT_TOKEN in your .env or Railway environment variables."
            )
            logger.error(err.format_log())
            return False, err.friendly_description

        url = f"{self.bot_url}/getMe"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    data = await resp.json()
                    if data.get("ok"):
                        bot_username = data.get("result", {}).get("username", "Unknown")
                        bot_name = data.get("result", {}).get("first_name", "")
                        logger.info(f"Telegram Bot verified successfully: @{bot_username} ({bot_name})")
                        return True, bot_username
                    else:
                        code = data.get("error_code", resp.status)
                        desc = data.get("description", "Failed to verify bot token")
                        err = parse_telegram_error(code, desc, data.get("parameters"))
                        logger.error(err.format_log("getMe verification"))
                        return False, str(err)
        except Exception as e:
            logger.error(f"Network error while verifying Telegram bot token: {e}")
            return False, f"Network error verifying bot token: {e}"

    async def _wait_cooldown(self) -> None:
        """Enforces inter-upload cooldown to protect bot against Telegram rate limits."""
        elapsed = time.time() - self.last_upload_time
        if elapsed < self.cooldown and self.last_upload_time > 0:
            wait_time = self.cooldown - elapsed
            logger.info(f"Enforcing upload cooldown: sleeping {wait_time:.1f}s...")
            await asyncio.sleep(wait_time)

    def format_caption(
        self,
        file_path: str,
        title: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        custom_tags: Optional[Dict[str, Any]] = None,
        template: Optional[str] = None,
        max_length: int = TELEGRAM_MAX_CAPTION_LENGTH
    ) -> str:
        """
        Renders an advanced templated caption supporting {title}, {size}, {duration},
        {filename}, {resolution}, {width}, {height}, {id}, {url}, and custom tags.
        Automatically safe-formats and truncates to Telegram's caption limit.
        """
        metadata = metadata or {}
        custom_tags = custom_tags or {}
        template_str = template or self.default_template

        # Determine file size
        file_size = metadata.get("file_size", 0)
        if not file_size and os.path.exists(file_path):
            file_size = os.path.getsize(file_path)

        # Determine duration
        duration_sec = metadata.get("duration", 0)

        # Determine dimensions
        width = metadata.get("width", 0)
        height = metadata.get("height", 0)
        resolution = f"{width}x{height}" if width and height else ""

        # Base title fallback
        base_title = title or metadata.get("title") or os.path.splitext(os.path.basename(file_path))[0]

        now = datetime.datetime.now()

        # Build comprehensive context mapping
        context = {
            "title": str(base_title),
            "size": format_file_size(file_size),
            "size_bytes": file_size,
            "duration": format_duration(duration_sec),
            "duration_sec": duration_sec,
            "duration_raw": duration_sec,
            "filename": os.path.basename(file_path),
            "ext": os.path.splitext(file_path)[1],
            "width": width,
            "height": height,
            "resolution": resolution,
            "id": metadata.get("id", metadata.get("video_id", "")),
            "video_id": metadata.get("video_id", metadata.get("id", "")),
            "url": metadata.get("video_url", metadata.get("url", "")),
            "video_url": metadata.get("video_url", metadata.get("url", "")),
            "date": now.strftime("%Y-%m-%d"),
            "time": now.strftime("%H:%M:%S"),
        }

        # Include custom tags and any extra metadata fields
        for k, v in metadata.items():
            if k not in context:
                context[k] = v
        for k, v in custom_tags.items():
            context[k] = v

        try:
            rendered = self.formatter.vformat(template_str, (), context)
        except Exception as e:
            logger.warning(f"Error rendering caption template ({e}), falling back to simple title")
            rendered = str(base_title)

        rendered = rendered.strip()

        # Enforce Telegram caption limit (1024 chars for media)
        if len(rendered) > max_length:
            logger.info(f"Caption length ({len(rendered)}) exceeds max ({max_length}). Truncating safely.")
            rendered = rendered[: max_length - 3] + "..."

        return rendered

    async def upload_video(
        self,
        file_path: str,
        title: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        target_chat_id: Optional[str] = None,
        max_retries: int = 5,
        custom_tags: Optional[Dict[str, Any]] = None,
        caption_template: Optional[str] = None,
        parse_mode: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Uploads a video to the specified Telegram chat/channel with stream chunking,
        advanced caption templating, and auto-fallback from sendVideo to sendDocument.

        Returns: (success_bool, error_message_if_failed)
        """
        destination_chat = target_chat_id or self.chat_id
        if not destination_chat:
            err_msg = "Target chat ID is missing. Set TELEGRAM_CHAT_ID in environment."
            logger.error(err_msg)
            return False, err_msg

        if not os.path.exists(file_path):
            err_msg = f"File does not exist: {file_path}"
            logger.error(err_msg)
            return False, err_msg

        metadata = metadata or {}
        custom_tags = custom_tags or {}
        active_parse_mode = parse_mode if parse_mode is not None else self.parse_mode

        # Generate advanced templated caption
        caption = self.format_caption(
            file_path=file_path,
            title=title,
            metadata=metadata,
            custom_tags=custom_tags,
            template=caption_template
        )

        await self._wait_cooldown()

        # 1. Attempt upload via sendVideo
        logger.info(f"Initiating sendVideo upload for: {os.path.basename(file_path)}...")
        success, error_details = await self._send_media_request(
            endpoint="sendVideo",
            field_name="video",
            file_path=file_path,
            chat_id=destination_chat,
            caption=caption,
            metadata=metadata,
            parse_mode=active_parse_mode,
            max_retries=max_retries
        )

        # 2. Auto-fallback to sendDocument if format/stream errors occur
        if not success and error_details and error_details.is_format_error:
            logger.warning(
                f"sendVideo failed due to format/stream constraints ({error_details.friendly_name}). "
                f"Triggering auto-fallback to sendDocument..."
            )
            success, error_details = await self._send_media_request(
                endpoint="sendDocument",
                field_name="document",
                file_path=file_path,
                chat_id=destination_chat,
                caption=caption,
                metadata=metadata,
                parse_mode=active_parse_mode,
                max_retries=max_retries
            )
            if success:
                logger.info("Auto-fallback to sendDocument succeeded!")

        if success:
            self.last_upload_time = time.time()
            return True, None

        error_summary = str(error_details) if error_details else "Upload failed due to unknown error"
        return False, error_summary

    async def _send_media_request(
        self,
        endpoint: str,
        field_name: str,
        file_path: str,
        chat_id: str,
        caption: str,
        metadata: Dict[str, Any],
        parse_mode: Optional[str] = None,
        max_retries: int = 5
    ) -> Tuple[bool, Optional[TelegramErrorDetails]]:
        """
        Executes multipart media upload with dynamic stream chunking, rich error diagnostics,
        and automatic rate limit backoff.
        """
        url = f"{self.bot_url}/{endpoint}"
        file_size = os.path.getsize(file_path)
        chunk_size = get_dynamic_chunk_size(file_size, self.chunk_size_override)

        # Upload timeout dynamically calculated based on file size (min 5 mins, max 60 mins)
        calc_timeout = max(300, int(file_size / (100 * 1024)))  # ~100KB/s lower bound
        client_timeout = aiohttp.ClientTimeout(total=calc_timeout, connect=30, sock_read=calc_timeout)

        retries = 0
        last_error: Optional[TelegramErrorDetails] = None

        while retries < max_retries:
            thumb_fp = None
            try:
                form_data = aiohttp.FormData()
                form_data.add_field("chat_id", str(chat_id))
                form_data.add_field("caption", caption)

                if parse_mode:
                    form_data.add_field("parse_mode", parse_mode)

                if endpoint == "sendVideo":
                    form_data.add_field("supports_streaming", "true")
                    if metadata.get("duration"):
                        form_data.add_field("duration", str(metadata["duration"]))
                    if metadata.get("width"):
                        form_data.add_field("width", str(metadata["width"]))
                    if metadata.get("height"):
                        form_data.add_field("height", str(metadata["height"]))

                # Attach thumbnail if available
                thumb_path = metadata.get("thumbnail")
                if thumb_path and os.path.exists(thumb_path):
                    try:
                        thumb_fp = open(thumb_path, "rb")
                        form_data.add_field(
                            "thumb",
                            thumb_fp,
                            filename=os.path.basename(thumb_path),
                            content_type="image/jpeg"
                        )
                    except Exception as e:
                        logger.warning(f"Could not attach thumbnail {thumb_path}: {e}")

                # Attach media payload with dynamic stream chunking
                content_type = "video/mp4" if field_name == "video" else "application/octet-stream"
                form_data.add_field(
                    field_name,
                    async_file_chunk_streamer(file_path, chunk_size),
                    filename=os.path.basename(file_path),
                    content_type=content_type
                )

                logger.info(
                    f"Streaming {os.path.basename(file_path)} ({format_file_size(file_size)}) "
                    f"via {endpoint} [Chunk: {format_file_size(chunk_size)}] to chat {chat_id}..."
                )

                async with aiohttp.ClientSession(timeout=client_timeout) as session:
                    async with session.post(url, data=form_data) as resp:
                        try:
                            result = await resp.json()
                        except Exception:
                            raw_text = await resp.text()
                            result = {"ok": False, "error_code": resp.status, "description": raw_text}

                        if thumb_fp:
                            thumb_fp.close()
                            thumb_fp = None

                        if result.get("ok"):
                            logger.info(f"Successfully uploaded {os.path.basename(file_path)} to Telegram!")
                            return True, None

                        # Parse Telegram error code & description
                        error_code = result.get("error_code", resp.status)
                        description = result.get("description", "Unknown Telegram Error")
                        parameters = result.get("parameters", {})
                        error_details = parse_telegram_error(error_code, description, parameters)
                        last_error = error_details

                        # Print rich error box in logs
                        logger.error(error_details.format_log(f"{endpoint} attempt {retries + 1}/{max_retries}"))

                        # Handle rate limiting / FloodWait
                        if error_details.status_code == 429 or error_details.retry_after:
                            sleep_time = int(error_details.retry_after or 15) + 2
                            logger.warning(f"Rate limit hit. Pausing execution for {sleep_time}s before retry...")
                            await asyncio.sleep(sleep_time)
                            retries += 1
                            continue

                        # If error is format-related, stop retrying sendVideo so caller can fallback immediately
                        if error_details.is_format_error:
                            return False, error_details

                        # Retry if server error or retryable
                        if error_details.is_retryable:
                            retries += 1
                            backoff = min(30, 2 ** retries + 2)
                            logger.info(f"Retryable error encountered. Backing off for {backoff}s...")
                            await asyncio.sleep(backoff)
                            continue

                        return False, error_details

            except asyncio.TimeoutError:
                retries += 1
                timeout_err = TelegramErrorDetails(
                    status_code=504,
                    raw_description="Upload socket timeout",
                    friendly_name="Upload Request Timed Out",
                    friendly_description=f"Streaming upload timed out after {calc_timeout}s.",
                    actionable_solution="Network throughput may be low. Retrying with exponential backoff.",
                    is_retryable=True,
                )
                last_error = timeout_err
                logger.warning(timeout_err.format_log(f"Attempt {retries}/{max_retries}"))
                await asyncio.sleep(5)

            except Exception as e:
                retries += 1
                network_err = TelegramErrorDetails(
                    status_code=0,
                    raw_description=str(e),
                    friendly_name="Network / Streaming Exception",
                    friendly_description=f"Client connection error during upload: {e}",
                    actionable_solution="Check Internet connection and DNS availability.",
                    is_retryable=True,
                )
                last_error = network_err
                logger.error(network_err.format_log(f"Attempt {retries}/{max_retries}"))
                await asyncio.sleep(5)

            finally:
                if thumb_fp:
                    try:
                        thumb_fp.close()
                    except Exception:
                        pass

        return False, last_error


# Global uploader instance
uploader = TelegramBotUploader()

