"""
Telegram Bot Administration Module for Bulk Video Scraper & Uploader.
Enables direct management for Admin ID 6649712542 and authorized administrators.

Features:
1. Long-polling Telegram message listener running asynchronously in the background.
2. Strict authorization check: Only responds to Admin ID 6649712542 (or IDs in ADMIN_USER_IDS).
3. Interactive dashboard with summary stats and inline keyboard controls (/start, /menu).
4. Command handlers:
   - /stats: Formatted queue statistics and metrics
   - /setchat <chat_id>: Dynamically update destination Telegram channel/chat ID
   - /seturl <url> [mode]: Dynamically update scrape target URL and mode
   - /scrape [url] [mode] [pages]: Trigger crawler and enqueue discovered video links
   - /add <url> [title]: Instantly add a video link to the queue
   - /retry: Retry all failed tasks
   - /pause and /resume: Toggle worker queue processing
   - /web: Return link to the Web Admin Panel
   - /help: Display command reference guide
5. Full inline keyboard callback support (cb:stats, cb:settings, cb:retry, cb:toggle_pause, cb:menu, etc.)
6. Clean async lifecycle start/stop methods.
"""

import os
import sys
import html
import time
import asyncio
import logging
from typing import Optional, Dict, Any, List, Tuple, Union
import aiohttp

from modules.config import config, ADMIN_USER_IDS, WEB_PANEL_URL, VALID_CRAWL_MODES
from modules.database import db_manager
from modules.crawler import UniversalCrawler
from modules.uploader import format_file_size

logger = logging.getLogger(__name__)

PRIMARY_ADMIN_ID = 6649712542


# ==============================================================================
# Helper Formatting Utilities
# ==============================================================================

def escape_html(text: Optional[str]) -> str:
    """Safely escape text for Telegram HTML parse mode."""
    if not text:
        return ""
    return html.escape(str(text))


def format_dashboard_text(
    stats: Dict[str, Any],
    is_paused: bool,
    chat_id: str,
    target_url: str,
    crawl_mode: str
) -> str:
    """Formats the main administrative dashboard message."""
    total = stats.get("TOTAL", 0)
    pending = stats.get("PENDING", 0)
    downloading = stats.get("DOWNLOADING", 0)
    uploading = stats.get("UPLOADING", 0)
    completed = stats.get("COMPLETED", 0)
    failed = stats.get("FAILED", 0)

    total_bytes = stats.get("total_completed_bytes", 0)
    size_str = format_file_size(total_bytes)

    status_str = "⏸️ <b>PAUSED</b>" if is_paused else "🟢 <b>ACTIVE / PROCESSING</b>"

    clean_chat = escape_html(chat_id) if chat_id else "<i>&lt;NOT SET&gt;</i>"
    clean_url = escape_html(target_url) if target_url else "<i>&lt;NOT SET&gt;</i>"
    clean_mode = escape_html(crawl_mode) if crawl_mode else "auto"

    return (
        "🤖 <b>Bulk Scraper &amp; Uploader Admin Dashboard</b>\n\n"
        "📊 <b>Queue Overview:</b>\n"
        f"• ⏳ Pending: <code>{pending}</code>\n"
        f"• ⬇️ Downloading: <code>{downloading}</code>\n"
        f"• ⬆️ Uploading: <code>{uploading}</code>\n"
        f"• ✅ Completed: <code>{completed}</code> ({size_str})\n"
        f"• ❌ Failed: <code>{failed}</code>\n"
        f"• 📦 Total Items: <code>{total}</code>\n\n"
        f"⚙️ <b>Worker Status:</b> {status_str}\n"
        f"🎯 <b>Target Chat:</b> <code>{clean_chat}</code>\n"
        f"🌐 <b>Scrape Target:</b> <code>{clean_url}</code> (Mode: <code>{clean_mode}</code>)\n\n"
        "<i>Use the buttons below or send commands like /stats, /scrape, /add.</i>"
    )


def format_stats_text(stats: Dict[str, Any], is_paused: bool) -> str:
    """Formats detailed queue statistics and performance metrics."""
    total = stats.get("TOTAL", 0)
    pending = stats.get("PENDING", 0)
    downloading = stats.get("DOWNLOADING", 0)
    uploading = stats.get("UPLOADING", 0)
    completed = stats.get("COMPLETED", 0)
    failed = stats.get("FAILED", 0)
    in_progress = downloading + uploading

    total_bytes = stats.get("total_completed_bytes", 0)
    avg_bytes = stats.get("avg_completed_bytes", 0)
    db_size = stats.get("db_size_bytes", 0)

    rate = (completed / total * 100.0) if total > 0 else 0.0
    filled = int(rate / 10)
    bar = "█" * filled + "░" * (10 - filled)

    status_str = "⏸️ PAUSED" if is_paused else "🟢 ACTIVE"

    return (
        "📊 <b>Detailed Queue Statistics</b>\n\n"
        f"<b>Status:</b> {status_str}\n"
        f"<b>Completion Rate:</b> <code>[{bar}] {rate:.1f}%</code>\n\n"
        f"• ⏳ <b>Pending:</b> <code>{pending}</code>\n"
        f"• ⬇️ <b>Downloading:</b> <code>{downloading}</code>\n"
        f"• ⬆️ <b>Uploading:</b> <code>{uploading}</code>\n"
        f"• 🔄 <b>In-Progress:</b> <code>{in_progress}</code>\n"
        f"• ✅ <b>Completed:</b> <code>{completed}</code>\n"
        f"• ❌ <b>Failed:</b> <code>{failed}</code>\n"
        f"• 📦 <b>Total:</b> <code>{total}</code>\n\n"
        "📈 <b>Data Volume:</b>\n"
        f"• Total Transferred: <code>{format_file_size(total_bytes)}</code>\n"
        f"• Avg Video Size: <code>{format_file_size(avg_bytes)}</code>\n"
        f"• Database Size: <code>{format_file_size(db_size)}</code>\n"
    )


def format_settings_text(
    is_paused: bool,
    chat_id: str,
    target_url: str,
    crawl_mode: str
) -> str:
    """Formats dynamic application settings view."""
    status_str = "⏸️ <b>PAUSED</b>" if is_paused else "🟢 <b>ACTIVE</b>"
    clean_chat = escape_html(chat_id) if chat_id else "<i>&lt;NOT SET&gt;</i>"
    clean_url = escape_html(target_url) if target_url else "<i>&lt;NOT SET&gt;</i>"

    return (
        "⚙️ <b>Active System Settings</b>\n\n"
        f"• <b>Worker State:</b> {status_str}\n"
        f"• <b>Destination Chat ID:</b> <code>{clean_chat}</code>\n"
        f"• <b>Scrape Target URL:</b> <code>{clean_url}</code>\n"
        f"• <b>Crawl Mode:</b> <code>{escape_html(crawl_mode or 'auto')}</code>\n"
        f"• <b>Upload Cooldown:</b> <code>{config.UPLOAD_COOLDOWN}s</code>\n"
        f"• <b>Max Retries:</b> <code>{config.MAX_RETRIES}</code>\n"
        f"• <b>Web Admin Panel:</b> <a href=\"{WEB_PANEL_URL}\">{WEB_PANEL_URL}</a>\n\n"
        "<i>To change settings, use <code>/setchat &lt;id&gt;</code> or <code>/seturl &lt;url&gt; [mode]</code>.</i>"
    )


def format_help_text() -> str:
    """Formats the command reference help guide."""
    return (
        "📖 <b>Bot Admin Command Reference</b>\n\n"
        "• <code>/start</code> or <code>/menu</code> — Show interactive control dashboard\n"
        "• <code>/stats</code> — View detailed queue statistics and metrics\n"
        "• <code>/setchat &lt;chat_id&gt;</code> — Dynamically update destination Telegram chat/channel\n"
        "• <code>/seturl &lt;url&gt; [mode]</code> — Update target scrape URL and crawl mode\n"
        "• <code>/scrape [url] [mode] [pages]</code> — Trigger web crawler and enqueue discovered media\n"
        "• <code>/add &lt;url&gt; [title]</code> — Manually add a video link to the queue\n"
        "• <code>/retry</code> — Retry all failed tasks in the queue\n"
        "• <code>/pause</code> — Pause worker downloading and uploading\n"
        "• <code>/resume</code> — Resume worker queue processing\n"
        "• <code>/web</code> — Get link to the Web Admin Panel\n"
        "• <code>/help</code> — Display this command reference\n"
    )


# ==============================================================================
# Inline Keyboard Layouts
# ==============================================================================

def get_menu_keyboard(is_paused: bool, web_url: str = WEB_PANEL_URL) -> Dict[str, Any]:
    """Generates the main interactive dashboard inline keyboard."""
    pause_text = "▶️ Resume Worker" if is_paused else "⏸️ Pause Worker"
    return {
        "inline_keyboard": [
            [
                {"text": "📊 Stats", "callback_data": "cb:stats"},
                {"text": "⚙️ Settings", "callback_data": "cb:settings"}
            ],
            [
                {"text": "🔁 Retry Failed", "callback_data": "cb:retry"},
                {"text": pause_text, "callback_data": "cb:toggle_pause"}
            ],
            [
                {"text": "🔄 Refresh Dashboard", "callback_data": "cb:refresh_menu"}
            ],
            [
                {"text": "🌐 Open Web Panel", "url": web_url}
            ]
        ]
    }


def get_stats_keyboard() -> Dict[str, Any]:
    """Generates the stats view inline keyboard."""
    return {
        "inline_keyboard": [
            [
                {"text": "🔄 Refresh Stats", "callback_data": "cb:refresh_stats"},
                {"text": "🔁 Retry Failed", "callback_data": "cb:retry"}
            ],
            [
                {"text": "🔙 Back to Menu", "callback_data": "cb:menu"}
            ]
        ]
    }


def get_settings_keyboard(is_paused: bool) -> Dict[str, Any]:
    """Generates the settings view inline keyboard."""
    pause_text = "▶️ Resume Worker" if is_paused else "⏸️ Pause Worker"
    return {
        "inline_keyboard": [
            [
                {"text": pause_text, "callback_data": "cb:toggle_pause"},
                {"text": "🕷️ Scrape Target", "callback_data": "cb:scrape_current"}
            ],
            [
                {"text": "🔙 Back to Menu", "callback_data": "cb:menu"}
            ]
        ]
    }


# ==============================================================================
# Telegram Admin Bot Controller
# ==============================================================================

class TelegramAdminBot:
    """
    Async Telegram Bot controller providing long-polling command execution,
    inline button interactions, dynamic settings management, and worker control.
    """

    def __init__(
        self,
        bot_token: Optional[str] = None,
        api_base: Optional[str] = None,
        admin_user_ids: Optional[List[int]] = None,
        web_panel_url: Optional[str] = None
    ):
        self.bot_token = (bot_token if bot_token is not None else config.TELEGRAM_BOT_TOKEN).strip()
        self.api_base = (api_base if api_base is not None else config.TELEGRAM_API_BASE).rstrip("/")
        self.admin_user_ids: List[int] = admin_user_ids if admin_user_ids is not None else list(ADMIN_USER_IDS)
        if PRIMARY_ADMIN_ID not in self.admin_user_ids:
            self.admin_user_ids.append(PRIMARY_ADMIN_ID)

        self.web_panel_url = web_panel_url or WEB_PANEL_URL
        self.running = False
        self._poller_task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_offset = 0

    @property
    def bot_url(self) -> str:
        return f"{self.api_base}/bot{self.bot_token}"

    def is_authorized(self, user_id: Union[int, str, None]) -> bool:
        """
        Verifies if a user ID is authorized to administer the bot.
        Allows Admin ID 6649712542 or any user ID in admin_user_ids.
        """
        if user_id is None:
            return False
        try:
            uid = int(user_id)
            return uid == PRIMARY_ADMIN_ID or uid in self.admin_user_ids
        except (ValueError, TypeError):
            return False

    # ==========================================================================
    # Telegram Bot API HTTP Methods
    # ==========================================================================

    async def _api_call(
        self,
        endpoint: str,
        payload: Dict[str, Any],
        session: Optional[aiohttp.ClientSession] = None
    ) -> Optional[Dict[str, Any]]:
        """Makes an asynchronous POST request to the Telegram Bot API."""
        if not self.bot_token:
            return None

        url = f"{self.bot_url}/{endpoint}"
        close_local = False
        client = session or self._session

        if client is None or client.closed:
            client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
            close_local = True

        try:
            async with client.post(url, json=payload) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    desc = data.get("description", "Unknown error")
                    err_code = data.get("error_code", resp.status)
                    logger.warning(f"Telegram API [{endpoint}] returned error {err_code}: {desc}")
                return data
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Telegram API request [{endpoint}] failed: {e}")
            return None
        finally:
            if close_local and client and not client.closed:
                await client.close()

    async def send_message(
        self,
        chat_id: Union[int, str],
        text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
        parse_mode: str = "HTML",
        disable_web_page_preview: bool = True,
        session: Optional[aiohttp.ClientSession] = None
    ) -> Optional[Dict[str, Any]]:
        """Sends a text message to the specified chat."""
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": disable_web_page_preview,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        return await self._api_call("sendMessage", payload, session=session)

    async def edit_message_text(
        self,
        chat_id: Union[int, str],
        message_id: int,
        text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
        parse_mode: str = "HTML",
        disable_web_page_preview: bool = True,
        session: Optional[aiohttp.ClientSession] = None
    ) -> Optional[Dict[str, Any]]:
        """Edits an existing text message."""
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": disable_web_page_preview,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        return await self._api_call("editMessageText", payload, session=session)

    async def answer_callback_query(
        self,
        callback_query_id: str,
        text: Optional[str] = None,
        show_alert: bool = False,
        session: Optional[aiohttp.ClientSession] = None
    ) -> Optional[Dict[str, Any]]:
        """Responds to an inline keyboard callback query."""
        payload: Dict[str, Any] = {
            "callback_query_id": callback_query_id,
            "show_alert": show_alert,
        }
        if text:
            payload["text"] = text
        return await self._api_call("answerCallbackQuery", payload, session=session)

    async def get_updates(
        self,
        offset: int,
        timeout: int = 20,
        session: Optional[aiohttp.ClientSession] = None
    ) -> List[Dict[str, Any]]:
        """Fetches pending updates via Telegram getUpdates long-polling."""
        payload = {
            "offset": offset,
            "timeout": timeout,
            "allowed_updates": ["message", "callback_query"]
        }
        data = await self._api_call("getUpdates", payload, session=session)
        if data and data.get("ok"):
            return data.get("result", [])
        return []

    # ==========================================================================
    # Command Handlers
    # ==========================================================================

    async def handle_start_or_menu(self, chat_id: int, message_id: Optional[int] = None, is_edit: bool = False) -> None:
        """Handles /start, /menu, and menu refresh callbacks."""
        stats = db_manager.get_detailed_stats()
        is_paused = db_manager.is_paused()
        target_chat = db_manager.get_active_chat_id()
        target_url, crawl_mode = db_manager.get_active_crawl_target()

        text = format_dashboard_text(stats, is_paused, target_chat, target_url, crawl_mode)
        reply_markup = get_menu_keyboard(is_paused, self.web_panel_url)

        if is_edit and message_id:
            await self.edit_message_text(chat_id, message_id, text, reply_markup=reply_markup)
        else:
            await self.send_message(chat_id, text, reply_markup=reply_markup)

    async def handle_stats(self, chat_id: int, message_id: Optional[int] = None, is_edit: bool = False) -> None:
        """Handles /stats and stats refresh callbacks."""
        stats = db_manager.get_detailed_stats()
        is_paused = db_manager.is_paused()
        text = format_stats_text(stats, is_paused)
        reply_markup = get_stats_keyboard()

        if is_edit and message_id:
            await self.edit_message_text(chat_id, message_id, text, reply_markup=reply_markup)
        else:
            await self.send_message(chat_id, text, reply_markup=reply_markup)

    async def handle_settings(self, chat_id: int, message_id: Optional[int] = None, is_edit: bool = False) -> None:
        """Handles settings display view."""
        is_paused = db_manager.is_paused()
        target_chat = db_manager.get_active_chat_id()
        target_url, crawl_mode = db_manager.get_active_crawl_target()

        text = format_settings_text(is_paused, target_chat, target_url, crawl_mode)
        reply_markup = get_settings_keyboard(is_paused)

        if is_edit and message_id:
            await self.edit_message_text(chat_id, message_id, text, reply_markup=reply_markup)
        else:
            await self.send_message(chat_id, text, reply_markup=reply_markup)

    async def handle_setchat(self, chat_id: int, args_text: str) -> None:
        """Handles /setchat <chat_id> to dynamically update target Telegram destination."""
        args_text = args_text.strip()
        if not args_text:
            current = db_manager.get_active_chat_id()
            clean_curr = escape_html(current) if current else "&lt;NOT SET&gt;"
            await self.send_message(
                chat_id,
                f"🎯 <b>Current Target Chat ID:</b> <code>{clean_curr}</code>\n\n"
                "<b>Usage:</b> <code>/setchat &lt;chat_id&gt;</code>\n"
                "<i>Example:</i> <code>/setchat -1001234567890</code> or <code>/setchat @MyChannel</code>"
            )
            return

        new_chat_id = args_text.split()[0].strip()
        db_manager.set_active_chat_id(new_chat_id)
        logger.info(f"Admin updated target Telegram chat ID to: {new_chat_id}")

        await self.send_message(
            chat_id,
            f"✅ <b>Destination Chat Updated!</b>\n\n"
            f"• <b>New Chat ID:</b> <code>{escape_html(new_chat_id)}</code>\n"
            "<i>All future video uploads will be delivered to this destination.</i>"
        )

    async def handle_seturl(self, chat_id: int, args_text: str) -> None:
        """Handles /seturl <url> [mode] to dynamically update crawl target."""
        parts = args_text.strip().split()
        if not parts:
            url, mode = db_manager.get_active_crawl_target()
            clean_url = escape_html(url) if url else "&lt;NOT SET&gt;"
            await self.send_message(
                chat_id,
                f"🌐 <b>Current Scrape Target:</b> <code>{clean_url}</code>\n"
                f"• <b>Mode:</b> <code>{escape_html(mode or 'auto')}</code>\n\n"
                "<b>Usage:</b> <code>/seturl &lt;url&gt; [mode]</code>\n"
                f"<i>Allowed modes:</i> <code>{', '.join(sorted(VALID_CRAWL_MODES))}</code>\n"
                "<i>Example:</i> <code>/seturl https://example.com/feed.xml rss</code>"
            )
            return

        new_url = parts[0].strip()
        if not (new_url.startswith("http://") or new_url.startswith("https://")):
            await self.send_message(
                chat_id,
                "❌ <b>Invalid URL:</b> URL must begin with <code>http://</code> or <code>https://</code>."
            )
            return

        new_mode = parts[1].lower().strip() if len(parts) > 1 else "auto"
        if new_mode not in VALID_CRAWL_MODES:
            new_mode = "auto"

        db_manager.set_active_crawl_target(new_url, new_mode)
        logger.info(f"Admin updated target scrape URL to: {new_url} (mode: {new_mode})")

        await self.send_message(
            chat_id,
            f"✅ <b>Scrape Target Updated!</b>\n\n"
            f"• <b>URL:</b> <code>{escape_html(new_url)}</code>\n"
            f"• <b>Mode:</b> <code>{escape_html(new_mode)}</code>\n"
            "<i>You can now run <code>/scrape</code> to discover and enqueue videos.</i>"
        )

    async def handle_scrape(self, chat_id: int, args_text: str) -> None:
        """
        Handles /scrape [url] [count] [mode] [pages] to trigger deep full-video crawler.
        Examples:
        - /scrape https://example.com 30 (extracts exactly 30 full videos, skips 3-5s previews)
        - /scrape https://example.com 50 deep
        - /scrape 30 (uses saved target URL and extracts 30 full videos)
        """
        parts = args_text.strip().split()
        target_url = ""
        mode = "deep"
        max_videos = 30
        max_pages = 10

        if parts:
            if parts[0].startswith("http://") or parts[0].startswith("https://"):
                target_url = parts[0].strip()
                if len(parts) > 1 and parts[1].isdigit():
                    max_videos = max(1, min(500, int(parts[1])))
                elif len(parts) > 1:
                    mode = parts[1].lower().strip()

                if len(parts) > 2 and parts[2].isdigit():
                    max_pages = int(parts[2])
                elif len(parts) > 2:
                    mode = parts[2].lower().strip()
            elif parts[0].isdigit():
                max_videos = max(1, min(500, int(parts[0])))
                if len(parts) > 1:
                    mode = parts[1].lower().strip()

        if not target_url:
            saved_url, saved_mode = db_manager.get_active_crawl_target()
            target_url = saved_url
            if not parts or not mode or mode == "auto":
                mode = saved_mode or "deep"

        if not target_url:
            await self.send_message(
                chat_id,
                "⚠️ <b>No Scrape Target Configured</b>\n\n"
                "<b>Usage:</b> <code>/scrape &lt;url&gt; [count] [mode]</code>\n"
                "<i>Example:</i> <code>/scrape https://example.com 30</code>\n"
                "<i>Or set a default target first with <code>/seturl &lt;url&gt;</code>.</i>"
            )
            return

        status_msg = await self.send_message(
            chat_id,
            f"🕷️ <b>Deep Video Scraper Started...</b>\n\n"
            f"• <b>Target Website:</b> <code>{escape_html(target_url)}</code>\n"
            f"• <b>Requested Videos:</b> <code>{max_videos}</code> (Full Length)\n"
            f"• <b>Mode:</b> <code>{escape_html(mode)}</code> (Skip 3-5s Previews)\n"
            f"• <b>Max Pages:</b> <code>{max_pages}</code>\n\n"
            "<i>Deep crawling each watch page to extract full player streams...</i>"
        )

        try:
            crawler = UniversalCrawler()
            discovered = await crawler.discover(target_url, mode=mode, max_pages=max_pages, max_videos=max_videos)

            if not discovered:
                res_text = (
                    f"⚠️ <b>Discovery Complete: 0 Full Videos Found</b>\n\n"
                    f"• <b>Target:</b> <code>{escape_html(target_url)}</code>\n"
                    f"• <b>Mode:</b> <code>{escape_html(mode)}</code>\n"
                    "<i>No valid full-length videos found. Check URL or try another category.</i>"
                )
            else:
                inserted, ignored = db_manager.enqueue_batch(discovered, media_type="video")
                res_text = (
                    f"✅ <b>Discovery Complete!</b>\n\n"
                    f"• <b>Target:</b> <code>{escape_html(target_url)}</code>\n"
                    f"• <b>Full Videos Found:</b> <code>{len(discovered)}</code>\n"
                    f"• <b>Newly Enqueued:</b> <code>{inserted}</code>\n"
                    f"• <b>Previews/Duplicates Skipped:</b> <code>{ignored}</code>\n\n"
                    "<i>Worker is downloading and uploading full videos to Telegram sequentially.</i>"
                )

            stats_btn = {"inline_keyboard": [[{"text": "📊 View Stats", "callback_data": "cb:stats"}]]}
            await self.send_message(chat_id, res_text, reply_markup=stats_btn)

        except Exception as e:
            logger.error(f"Error executing crawler from /scrape: {e}", exc_info=True)
            await self.send_message(
                chat_id,
                f"❌ <b>Scrape Failed:</b>\n<code>{escape_html(str(e))}</code>"
            )

    async def handle_add(self, chat_id: int, args_text: str) -> None:
        """Handles /add <url> [title] to manually enqueue a video."""
        parts = args_text.strip().split(maxsplit=1)
        if not parts:
            await self.send_message(
                chat_id,
                "⚠️ <b>Usage:</b> <code>/add &lt;url&gt; [title]</code>\n\n"
                "<i>Example:</i> <code>/add https://example.com/video.mp4 Awesome Trailer</code>"
            )
            return

        url = parts[0].strip()
        title = parts[1].strip() if len(parts) > 1 else "Direct Video"

        if not (url.startswith("http://") or url.startswith("https://")):
            await self.send_message(
                chat_id,
                "❌ <b>Invalid URL:</b> Video link must start with <code>http://</code> or <code>https://</code>."
            )
            return

        is_new, item_id = db_manager.enqueue_one(url, title=title)
        if is_new:
            await self.send_message(
                chat_id,
                f"✅ <b>Successfully Enqueued Task #{item_id}!</b>\n\n"
                f"• <b>Title:</b> {escape_html(title)}\n"
                f"• <b>URL:</b> <code>{escape_html(url)}</code>\n"
                f"• <b>Status:</b> <code>PENDING</code>"
            )
        else:
            existing = db_manager.get_task(item_id) if item_id else None
            st = existing.get("status", "UNKNOWN") if existing else "UNKNOWN"
            await self.send_message(
                chat_id,
                f"⚠️ <b>URL Already in Queue (Task #{item_id})</b>\n\n"
                f"• <b>Title:</b> {escape_html(existing.get('title', title) if existing else title)}\n"
                f"• <b>Current Status:</b> <code>{st}</code>\n"
                f"• <b>URL:</b> <code>{escape_html(url)}</code>"
            )

    async def handle_retry(self, chat_id: int) -> None:
        """Handles /retry to reset failed tasks."""
        count = db_manager.retry_all_failed()
        stats = db_manager.get_detailed_stats()
        is_paused = db_manager.is_paused()

        if count > 0:
            msg = (
                f"🔁 <b>Retry Triggered!</b>\n\n"
                f"Successfully reset <b>{count}</b> failed task(s) back to <code>PENDING</code>."
            )
        else:
            msg = (
                "ℹ️ <b>No Failed Tasks:</b>\n\n"
                "There are currently no tasks in <code>FAILED</code> status to retry."
            )

        reply_markup = get_stats_keyboard()
        await self.send_message(chat_id, msg, reply_markup=reply_markup)

    async def handle_pause(self, chat_id: int) -> None:
        """Handles /pause to halt worker processing."""
        db_manager.set_worker_paused(True)
        await self.send_message(
            chat_id,
            "⏸️ <b>Worker Pipeline Paused</b>\n\n"
            "Queue downloads and uploads have been halted.\n"
            "<i>Send <code>/resume</code> or click Resume to resume processing.</i>",
            reply_markup=get_menu_keyboard(True, self.web_panel_url)
        )

    async def handle_resume(self, chat_id: int) -> None:
        """Handles /resume to continue worker processing."""
        db_manager.set_worker_paused(False)
        await self.send_message(
            chat_id,
            "▶️ <b>Worker Pipeline Resumed</b>\n\n"
            "Queue processing has resumed.\n"
            "<i>The worker will automatically process pending items.</i>",
            reply_markup=get_menu_keyboard(False, self.web_panel_url)
        )

    async def handle_web(self, chat_id: int) -> None:
        """Handles /web to return link to the Web Admin Panel."""
        reply_markup = {
            "inline_keyboard": [
                [{"text": "🌐 Open Web Panel", "url": self.web_panel_url}]
            ]
        }
        await self.send_message(
            chat_id,
            f"🌐 <b>Web Admin Panel:</b>\n<a href=\"{self.web_panel_url}\">{self.web_panel_url}</a>\n\n"
            "<i>Manage and monitor the queue in your browser.</i>",
            reply_markup=reply_markup
        )

    async def handle_help(self, chat_id: int) -> None:
        """Handles /help to display command reference."""
        await self.send_message(chat_id, format_help_text())

    # ==========================================================================
    # Update Dispatcher & Callback Query Handlers
    # ==========================================================================

    async def process_update(self, update: Dict[str, Any]) -> None:
        """Processes a single incoming Telegram update."""
        # 1. Handle Message Updates
        if "message" in update:
            await self._process_message(update["message"])
            return

        # 2. Handle Callback Query Updates
        if "callback_query" in update:
            await self._process_callback_query(update["callback_query"])
            return

    async def _process_message(self, message: Dict[str, Any]) -> None:
        """Handles incoming text messages and commands."""
        from_user = message.get("from", {})
        user_id = from_user.get("id")
        chat_id = message.get("chat", {}).get("id")
        text = (message.get("text") or "").strip()

        if not chat_id or not text:
            return

        # Authorization Verification
        if not self.is_authorized(user_id):
            logger.warning(f"Unauthorized message from user ID {user_id} (@{from_user.get('username', 'Unknown')}) in chat {chat_id}")
            polite_rejection = (
                "⛔️ <b>Access Denied</b>\n\n"
                "You are not authorized to use this administrative bot.\n"
                f"Your Telegram User ID (<code>{user_id}</code>) is not on the administrator allowlist."
            )
            await self.send_message(chat_id, polite_rejection)
            return

        # Parse command and arguments
        parts = text.split(maxsplit=1)
        raw_cmd = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""

        # Strip bot username suffix e.g. /start@MyBot -> /start
        cmd = raw_cmd.split("@")[0]

        logger.info(f"Admin command received: '{cmd}' with args: '{args}' from user {user_id}")

        if cmd in ("/start", "/menu"):
            await self.handle_start_or_menu(chat_id)
        elif cmd == "/stats":
            await self.handle_stats(chat_id)
        elif cmd == "/setchat":
            await self.handle_setchat(chat_id, args)
        elif cmd == "/seturl":
            await self.handle_seturl(chat_id, args)
        elif cmd == "/scrape":
            await self.handle_scrape(chat_id, args)
        elif cmd == "/add":
            await self.handle_add(chat_id, args)
        elif cmd == "/retry":
            await self.handle_retry(chat_id)
        elif cmd == "/pause":
            await self.handle_pause(chat_id)
        elif cmd == "/resume":
            await self.handle_resume(chat_id)
        elif cmd == "/web":
            await self.handle_web(chat_id)
        elif cmd == "/help":
            await self.handle_help(chat_id)
        else:
            # If user sent a direct URL, treat it as /add <url>
            if text.startswith("http://") or text.startswith("https://"):
                await self.handle_add(chat_id, text)
            else:
                await self.send_message(
                    chat_id,
                    f"❓ <b>Unknown command:</b> <code>{escape_html(cmd)}</code>\n\n"
                    "<i>Type <code>/help</code> to view available admin commands or <code>/menu</code> for the dashboard.</i>"
                )

    async def _process_callback_query(self, cb_query: Dict[str, Any]) -> None:
        """Handles inline keyboard button clicks."""
        query_id = cb_query.get("id")
        from_user = cb_query.get("from", {})
        user_id = from_user.get("id")
        data = cb_query.get("data", "")
        message = cb_query.get("message", {})
        chat_id = message.get("chat", {}).get("id")
        message_id = message.get("message_id")

        if not self.is_authorized(user_id):
            logger.warning(f"Unauthorized callback query from user ID {user_id}: {data}")
            if query_id:
                await self.answer_callback_query(
                    query_id,
                    text="⛔️ Access Denied. You are not an authorized administrator.",
                    show_alert=True
                )
            return

        if query_id:
            # Immediate feedback acknowledgment
            await self.answer_callback_query(query_id)

        if not chat_id or not message_id:
            return

        logger.info(f"Admin callback query received: '{data}' from user {user_id}")

        if data in ("cb:menu", "cb:refresh_menu"):
            await self.handle_start_or_menu(chat_id, message_id=message_id, is_edit=True)

        elif data in ("cb:stats", "cb:refresh_stats"):
            await self.handle_stats(chat_id, message_id=message_id, is_edit=True)

        elif data == "cb:settings":
            await self.handle_settings(chat_id, message_id=message_id, is_edit=True)

        elif data == "cb:toggle_pause":
            current_paused = db_manager.is_paused()
            new_paused = not current_paused
            db_manager.set_worker_paused(new_paused)
            alert_text = "⏸️ Worker Paused" if new_paused else "▶️ Worker Resumed"
            if query_id:
                await self.answer_callback_query(query_id, text=alert_text, show_alert=False)
            await self.handle_start_or_menu(chat_id, message_id=message_id, is_edit=True)

        elif data == "cb:retry":
            count = db_manager.retry_all_failed()
            if query_id:
                await self.answer_callback_query(
                    query_id,
                    text=f"🔁 Reset {count} failed tasks to PENDING",
                    show_alert=True
                )
            await self.handle_start_or_menu(chat_id, message_id=message_id, is_edit=True)

        elif data == "cb:scrape_current":
            url, mode = db_manager.get_active_crawl_target()
            if not url:
                if query_id:
                    await self.answer_callback_query(
                        query_id,
                        text="⚠️ No scrape URL configured. Use /seturl <url>",
                        show_alert=True
                    )
            else:
                if query_id:
                    await self.answer_callback_query(
                        query_id,
                        text="🕷️ Scrape triggered in background!",
                        show_alert=False
                    )
                asyncio.create_task(self.handle_scrape(chat_id, url))

    # ==========================================================================
    # Long-Polling Lifecycle Management
    # ==========================================================================

    async def _poll_loop(self) -> None:
        """Internal long-polling loop fetching updates from Telegram."""
        logger.info(f"Telegram Admin Bot poller started for Admin ID {PRIMARY_ADMIN_ID}.")
        session_timeout = aiohttp.ClientTimeout(total=35)

        consecutive_errors = 0

        async with aiohttp.ClientSession(timeout=session_timeout) as session:
            self._session = session
            while self.running:
                try:
                    updates = await self.get_updates(
                        offset=self._last_offset,
                        timeout=20,
                        session=session
                    )
                    consecutive_errors = 0

                    if updates:
                        for update in updates:
                            update_id = update.get("update_id", 0)
                            self._last_offset = max(self._last_offset, update_id + 1)
                            try:
                                await self.process_update(update)
                            except Exception as e:
                                logger.error(f"Error processing update #{update_id}: {e}", exc_info=True)
                    else:
                        await asyncio.sleep(0.05)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    consecutive_errors += 1
                    backoff = min(30, 2 ** min(consecutive_errors, 5))
                    logger.warning(f"Telegram long-polling connection error ({e}). Retrying in {backoff}s...")
                    try:
                        await asyncio.sleep(backoff)
                    except asyncio.CancelledError:
                        break

        logger.info("Telegram Admin Bot poller terminated cleanly.")

    async def start(self) -> None:
        """Starts the long-polling loop in the current coroutine."""
        if not self.bot_token:
            logger.warning("Cannot start Telegram Admin Bot: TELEGRAM_BOT_TOKEN is not configured.")
            return

        self.running = True
        await self._poll_loop()

    def start_background(self) -> Optional[asyncio.Task]:
        """Launches the long-polling loop as a background asyncio Task."""
        if not self.bot_token:
            logger.warning("Cannot start Telegram Admin Bot in background: TELEGRAM_BOT_TOKEN is empty.")
            return None

        if self.running and self._poller_task and not self._poller_task.done():
            logger.info("Telegram Admin Bot is already running in background.")
            return self._poller_task

        self.running = True
        self._poller_task = asyncio.create_task(self._poll_loop())
        logger.info("Telegram Admin Bot background task created.")
        return self._poller_task

    async def stop(self) -> None:
        """Gracefully halts the Telegram Admin Bot long-polling loop."""
        if not self.running:
            return

        logger.info("Stopping Telegram Admin Bot...")
        self.running = False

        if self._poller_task and not self._poller_task.done():
            self._poller_task.cancel()
            try:
                await asyncio.wait_for(self._poller_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            self._poller_task = None

        if self._session and not self._session.closed:
            try:
                await self._session.close()
            except Exception as e:
                logger.debug(f"Error closing bot session: {e}")
            self._session = None

        logger.info("Telegram Admin Bot stopped.")


# Global convenience instance
bot_admin = TelegramAdminBot()


async def run_bot_admin_listener(shutdown_event: Optional[asyncio.Event] = None) -> None:
    """
    Runs the Telegram Admin Bot polling listener until shutdown_event is set or cancelled.
    """
    bot = bot_admin
    task = bot.start_background()
    if not task:
        return
    if shutdown_event:
        await shutdown_event.wait()
        await bot.stop()
    else:
        try:
            await task
        except asyncio.CancelledError:
            await bot.stop()


# ==============================================================================
# Standalone Execution Entrypoint
# ==============================================================================


async def _main_standalone() -> None:
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger.info("Starting Telegram Admin Bot in standalone mode...")
    config.ensure_directories()
    db_manager.init_db()

    bot = TelegramAdminBot()
    try:
        await bot.start()
    except (KeyboardInterrupt, SystemExit):
        await bot.stop()


if __name__ == "__main__":
    try:
        asyncio.run(_main_standalone())
    except (KeyboardInterrupt, SystemExit):
        pass
