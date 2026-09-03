"""
Comprehensive Test Suite for modules/bot_admin.py (Telegram Admin Bot Controller).
Tests:
1. Authorization check (Admin ID 6649712542, extra admin IDs, unauthorized users).
2. UI formatting (escape_html, dashboard text, stats text, settings text, help text, keyboard generators).
3. Command handling (/start, /menu, /stats, /setchat, /seturl, /add, /retry, /pause, /resume, /web, /help, /scrape).
4. Inline keyboard callback query handling (cb:stats, cb:settings, cb:retry, cb:toggle_pause, cb:menu).
5. Background lifecycle (start_background, stop, run_bot_admin_listener).
"""

import os
import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

from modules.config import config, ADMIN_USER_IDS, WEB_PANEL_URL
from modules.database import DatabaseManager, db_manager
from modules.crawler import UniversalCrawler
from modules.bot_admin import (
    TelegramAdminBot,
    PRIMARY_ADMIN_ID,
    escape_html,
    format_dashboard_text,
    format_stats_text,
    format_settings_text,
    format_help_text,
    get_menu_keyboard,
    get_stats_keyboard,
    get_settings_keyboard,
    run_bot_admin_listener,
)


@pytest.fixture
def temp_db(tmp_path):
    """Fixture providing an isolated SQLite database manager."""
    db_file = str(tmp_path / "test_admin_bot.db")
    mgr = DatabaseManager(db_path=db_file)
    mgr.init_db()
    return mgr


@pytest.fixture
def test_bot(temp_db, monkeypatch):
    """Fixture providing a configured TelegramAdminBot instance with mocked database."""
    monkeypatch.setattr("modules.bot_admin.db_manager", temp_db)
    bot = TelegramAdminBot(
        bot_token="123456789:ABCdefGhIJKlmNoPQRsTUVwxyZ-1234567",
        api_base="https://api.telegram.org",
        admin_user_ids=[PRIMARY_ADMIN_ID, 99887766],
        web_panel_url="https://test-panel.railway.app"
    )
    return bot


# ==============================================================================
# 1. Authorization Verification Tests
# ==============================================================================

def test_authorization_primary_admin(test_bot):
    """Verify primary admin ID 6649712542 is always authorized."""
    assert test_bot.is_authorized(6649712542) is True
    assert test_bot.is_authorized("6649712542") is True
    assert test_bot.is_authorized(PRIMARY_ADMIN_ID) is True


def test_authorization_extra_admin(test_bot):
    """Verify admin user in admin_user_ids is authorized."""
    assert test_bot.is_authorized(99887766) is True
    assert test_bot.is_authorized("99887766") is True


def test_authorization_unauthorized_user(test_bot):
    """Verify unauthorized users are rejected."""
    assert test_bot.is_authorized(111222333) is False
    assert test_bot.is_authorized("111222333") is False
    assert test_bot.is_authorized(None) is False
    assert test_bot.is_authorized("invalid_string") is False


# ==============================================================================
# 2. Text & Keyboard Formatting Tests
# ==============================================================================

def test_escape_html():
    """Verify HTML special characters are properly escaped."""
    assert escape_html("<b>Test & 'Title' <123></b>") == "&lt;b&gt;Test &amp; &#x27;Title&#x27; &lt;123&gt;&lt;/b&gt;"
    assert escape_html(None) == ""
    assert escape_html("") == ""


def test_format_dashboard_text():
    """Verify dashboard text rendering with stats and status."""
    stats = {
        "TOTAL": 25,
        "PENDING": 10,
        "DOWNLOADING": 2,
        "UPLOADING": 1,
        "COMPLETED": 10,
        "FAILED": 2,
        "total_completed_bytes": 104857600
    }
    text_active = format_dashboard_text(stats, is_paused=False, chat_id="-100123456", target_url="https://example.com", crawl_mode="rss")
    assert "Bulk Scraper" in text_active
    assert "Pending: <code>10</code>" in text_active
    assert "ACTIVE / PROCESSING" in text_active
    assert "-100123456" in text_active
    assert "https://example.com" in text_active

    text_paused = format_dashboard_text(stats, is_paused=True, chat_id="", target_url="", crawl_mode="")
    assert "PAUSED" in text_paused
    assert "&lt;NOT SET&gt;" in text_paused


def test_format_stats_text():
    """Verify detailed stats text formatting."""
    stats = {
        "TOTAL": 10,
        "PENDING": 2,
        "DOWNLOADING": 1,
        "UPLOADING": 0,
        "COMPLETED": 6,
        "FAILED": 1,
        "total_completed_bytes": 52428800,
        "avg_completed_bytes": 8738133,
        "db_size_bytes": 16384
    }
    text = format_stats_text(stats, is_paused=False)
    assert "Detailed Queue Statistics" in text
    assert "Completion Rate:" in text
    assert "60.0%" in text
    assert "50.00 MB" in text


def test_format_settings_text():
    """Verify dynamic settings text formatting."""
    text = format_settings_text(is_paused=False, chat_id="-100999", target_url="https://site.com/sitemap.xml", crawl_mode="sitemap")
    assert "Active System Settings" in text
    assert "-100999" in text
    assert "https://site.com/sitemap.xml" in text
    assert "sitemap" in text


def test_format_help_text():
    """Verify command reference documentation."""
    text = format_help_text()
    assert "/start" in text
    assert "/stats" in text
    assert "/setchat" in text
    assert "/seturl" in text
    assert "/scrape" in text
    assert "/add" in text
    assert "/retry" in text
    assert "/pause" in text
    assert "/resume" in text
    assert "/web" in text


def test_keyboards_structure():
    """Verify inline keyboard structures and buttons."""
    menu_active = get_menu_keyboard(is_paused=False, web_url="https://test-panel.railway.app")
    assert any("Pause Worker" in btn["text"] for row in menu_active["inline_keyboard"] for btn in row)
    assert any(btn.get("url") == "https://test-panel.railway.app" for row in menu_active["inline_keyboard"] for btn in row)

    menu_paused = get_menu_keyboard(is_paused=True)
    assert any("Resume Worker" in btn["text"] for row in menu_paused["inline_keyboard"] for btn in row)

    stats_kb = get_stats_keyboard()
    assert any("Refresh Stats" in btn["text"] for row in stats_kb["inline_keyboard"] for btn in row)

    settings_kb = get_settings_keyboard(is_paused=False)
    assert any("Pause Worker" in btn["text"] for row in settings_kb["inline_keyboard"] for btn in row)


# ==============================================================================
# 3. Command Handlers & Message Processing Tests
# ==============================================================================

@pytest.mark.asyncio
async def test_unauthorized_user_rejection(test_bot):
    """Verify unauthorized users receive access denied response."""
    test_bot.send_message = AsyncMock(return_value={"ok": True})
    update = {
        "message": {
            "message_id": 1,
            "from": {"id": 1234567, "username": "intruder"},
            "chat": {"id": 1234567},
            "text": "/start"
        }
    }
    await test_bot.process_update(update)
    test_bot.send_message.assert_called_once()
    args, kwargs = test_bot.send_message.call_args
    assert "Access Denied" in args[1]


@pytest.mark.asyncio
async def test_handle_start_command(test_bot):
    """Verify /start displays dashboard menu."""
    test_bot.send_message = AsyncMock(return_value={"ok": True})
    update = {
        "message": {
            "message_id": 10,
            "from": {"id": PRIMARY_ADMIN_ID, "username": "admin"},
            "chat": {"id": PRIMARY_ADMIN_ID},
            "text": "/start"
        }
    }
    await test_bot.process_update(update)
    test_bot.send_message.assert_called_once()
    chat_id, text = test_bot.send_message.call_args[0][:2]
    assert chat_id == PRIMARY_ADMIN_ID
    assert "Admin Dashboard" in text


@pytest.mark.asyncio
async def test_handle_stats_command(test_bot):
    """Verify /stats displays detailed queue statistics."""
    test_bot.send_message = AsyncMock(return_value={"ok": True})
    update = {
        "message": {
            "message_id": 11,
            "from": {"id": PRIMARY_ADMIN_ID},
            "chat": {"id": PRIMARY_ADMIN_ID},
            "text": "/stats"
        }
    }
    await test_bot.process_update(update)
    test_bot.send_message.assert_called_once()
    text = test_bot.send_message.call_args[0][1]
    assert "Detailed Queue Statistics" in text


@pytest.mark.asyncio
async def test_handle_setchat_command(test_bot, temp_db):
    """Verify /setchat dynamically updates target destination chat."""
    test_bot.send_message = AsyncMock(return_value={"ok": True})

    # 1. No arguments -> show current and usage
    await test_bot.handle_setchat(PRIMARY_ADMIN_ID, "")
    assert "Current Target Chat ID" in test_bot.send_message.call_args[0][1]

    # 2. Update with new chat ID
    await test_bot.handle_setchat(PRIMARY_ADMIN_ID, "-1001987654321")
    assert "Destination Chat Updated" in test_bot.send_message.call_args[0][1]
    assert temp_db.get_active_chat_id() == "-1001987654321"


@pytest.mark.asyncio
async def test_handle_seturl_command(test_bot, temp_db):
    """Verify /seturl dynamically updates scrape target URL and crawl mode."""
    test_bot.send_message = AsyncMock(return_value={"ok": True})

    # 1. No arguments -> show current
    await test_bot.handle_seturl(PRIMARY_ADMIN_ID, "")
    assert "Current Scrape Target" in test_bot.send_message.call_args[0][1]

    # 2. Invalid URL
    await test_bot.handle_seturl(PRIMARY_ADMIN_ID, "invalid-url")
    assert "Invalid URL" in test_bot.send_message.call_args[0][1]

    # 3. Valid URL and mode
    await test_bot.handle_seturl(PRIMARY_ADMIN_ID, "https://example.com/feed.xml rss")
    assert "Scrape Target Updated" in test_bot.send_message.call_args[0][1]
    url, mode = temp_db.get_active_crawl_target()
    assert url == "https://example.com/feed.xml"
    assert mode == "rss"


@pytest.mark.asyncio
async def test_handle_add_command(test_bot, temp_db):
    """Verify /add manually enqueues single video links."""
    test_bot.send_message = AsyncMock(return_value={"ok": True})

    # 1. No arguments
    await test_bot.handle_add(PRIMARY_ADMIN_ID, "")
    assert "Usage:" in test_bot.send_message.call_args[0][1]

    # 2. Enqueue new video
    await test_bot.handle_add(PRIMARY_ADMIN_ID, "https://example.com/vid1.mp4 My Awesome Video")
    assert "Successfully Enqueued" in test_bot.send_message.call_args[0][1]
    task = temp_db.get_task(1)
    assert task["video_url"] == "https://example.com/vid1.mp4"
    assert task["title"] == "My Awesome Video"

    # 3. Enqueue duplicate video
    await test_bot.handle_add(PRIMARY_ADMIN_ID, "https://example.com/vid1.mp4 Another Title")
    assert "Already in Queue" in test_bot.send_message.call_args[0][1]


@pytest.mark.asyncio
async def test_handle_retry_command(test_bot, temp_db):
    """Verify /retry resets failed tasks."""
    test_bot.send_message = AsyncMock(return_value={"ok": True})

    # Enqueue a task and set to FAILED
    temp_db.enqueue_one("https://example.com/fail.mp4", "Failing Video")
    temp_db.set_status(1, "FAILED", error_message="Network Timeout")

    stats_before = temp_db.get_stats()
    assert stats_before["FAILED"] == 1

    await test_bot.handle_retry(PRIMARY_ADMIN_ID)
    assert "<b>1</b> failed task(s)" in test_bot.send_message.call_args[0][1]

    stats_after = temp_db.get_stats()
    assert stats_after["FAILED"] == 0
    assert stats_after["PENDING"] == 1


@pytest.mark.asyncio
async def test_handle_pause_and_resume_commands(test_bot, temp_db):
    """Verify /pause and /resume toggle worker processing."""
    test_bot.send_message = AsyncMock(return_value={"ok": True})

    assert temp_db.is_paused() is False

    await test_bot.handle_pause(PRIMARY_ADMIN_ID)
    assert temp_db.is_paused() is True
    assert "Worker Pipeline Paused" in test_bot.send_message.call_args[0][1]

    await test_bot.handle_resume(PRIMARY_ADMIN_ID)
    assert temp_db.is_paused() is False
    assert "Worker Pipeline Resumed" in test_bot.send_message.call_args[0][1]


@pytest.mark.asyncio
async def test_handle_web_command(test_bot):
    """Verify /web returns link to Web Admin Panel."""
    test_bot.send_message = AsyncMock(return_value={"ok": True})
    await test_bot.handle_web(PRIMARY_ADMIN_ID)
    assert "Web Admin Panel" in test_bot.send_message.call_args[0][1]


@pytest.mark.asyncio
async def test_handle_help_command(test_bot):
    """Verify /help returns command guide."""
    test_bot.send_message = AsyncMock(return_value={"ok": True})
    await test_bot.handle_help(PRIMARY_ADMIN_ID)
    assert "Command Reference" in test_bot.send_message.call_args[0][1]


@pytest.mark.asyncio
async def test_handle_scrape_command(test_bot, temp_db):
    """Verify /scrape triggers crawler discovery and enqueues items."""
    test_bot.send_message = AsyncMock(return_value={"ok": True})

    mock_discovered = [
        {"url": "https://example.com/stream1.m3u8", "title": "Stream 1"},
        {"url": "https://example.com/stream2.m3u8", "title": "Stream 2"}
    ]

    with patch.object(UniversalCrawler, "discover", new=AsyncMock(return_value=mock_discovered)):
        await test_bot.handle_scrape(PRIMARY_ADMIN_ID, "https://example.com/feed.xml rss 3")

    # Verify discovery summary was sent
    assert test_bot.send_message.call_count >= 2
    final_call_text = test_bot.send_message.call_args[0][1]
    assert "Discovery Complete!" in final_call_text
    assert "Newly Enqueued" in final_call_text

    stats = temp_db.get_stats()
    assert stats["PENDING"] == 2


# ==============================================================================
# 4. Inline Keyboard Callback Query Tests
# ==============================================================================

@pytest.mark.asyncio
async def test_unauthorized_callback_rejection(test_bot):
    """Verify unauthorized callback queries are rejected."""
    test_bot.answer_callback_query = AsyncMock(return_value={"ok": True})
    cb_update = {
        "callback_query": {
            "id": "query_123",
            "from": {"id": 888777666},
            "data": "cb:stats",
            "message": {"message_id": 50, "chat": {"id": 888777666}}
        }
    }
    await test_bot.process_update(cb_update)
    test_bot.answer_callback_query.assert_called_once_with(
        "query_123",
        text="⛔️ Access Denied. You are not an authorized administrator.",
        show_alert=True
    )


@pytest.mark.asyncio
async def test_callback_menu_refresh(test_bot):
    """Verify cb:menu and cb:refresh_menu edit message to dashboard."""
    test_bot.answer_callback_query = AsyncMock(return_value={"ok": True})
    test_bot.edit_message_text = AsyncMock(return_value={"ok": True})

    cb_update = {
        "callback_query": {
            "id": "query_menu",
            "from": {"id": PRIMARY_ADMIN_ID},
            "data": "cb:refresh_menu",
            "message": {"message_id": 100, "chat": {"id": PRIMARY_ADMIN_ID}}
        }
    }
    await test_bot.process_update(cb_update)
    test_bot.answer_callback_query.assert_called_once_with("query_menu")
    test_bot.edit_message_text.assert_called_once()
    assert "Admin Dashboard" in test_bot.edit_message_text.call_args[0][2]


@pytest.mark.asyncio
async def test_callback_stats_refresh(test_bot):
    """Verify cb:stats and cb:refresh_stats edit message to stats."""
    test_bot.answer_callback_query = AsyncMock(return_value={"ok": True})
    test_bot.edit_message_text = AsyncMock(return_value={"ok": True})

    cb_update = {
        "callback_query": {
            "id": "query_stats",
            "from": {"id": PRIMARY_ADMIN_ID},
            "data": "cb:stats",
            "message": {"message_id": 101, "chat": {"id": PRIMARY_ADMIN_ID}}
        }
    }
    await test_bot.process_update(cb_update)
    test_bot.edit_message_text.assert_called_once()
    assert "Detailed Queue Statistics" in test_bot.edit_message_text.call_args[0][2]


@pytest.mark.asyncio
async def test_callback_toggle_pause(test_bot, temp_db):
    """Verify cb:toggle_pause toggles pause state and updates view."""
    test_bot.answer_callback_query = AsyncMock(return_value={"ok": True})
    test_bot.edit_message_text = AsyncMock(return_value={"ok": True})

    assert temp_db.is_paused() is False

    cb_update = {
        "callback_query": {
            "id": "query_pause",
            "from": {"id": PRIMARY_ADMIN_ID},
            "data": "cb:toggle_pause",
            "message": {"message_id": 102, "chat": {"id": PRIMARY_ADMIN_ID}}
        }
    }
    await test_bot.process_update(cb_update)
    assert temp_db.is_paused() is True


@pytest.mark.asyncio
async def test_callback_retry(test_bot, temp_db):
    """Verify cb:retry retries failed tasks and updates view."""
    test_bot.answer_callback_query = AsyncMock(return_value={"ok": True})
    test_bot.edit_message_text = AsyncMock(return_value={"ok": True})

    temp_db.enqueue_one("https://example.com/failed.mp4")
    temp_db.set_status(1, "FAILED", error_message="Mock Error")

    cb_update = {
        "callback_query": {
            "id": "query_retry",
            "from": {"id": PRIMARY_ADMIN_ID},
            "data": "cb:retry",
            "message": {"message_id": 103, "chat": {"id": PRIMARY_ADMIN_ID}}
        }
    }
    await test_bot.process_update(cb_update)
    stats = temp_db.get_stats()
    assert stats["FAILED"] == 0
    assert stats["PENDING"] == 1


# ==============================================================================
# 5. Lifecycle Management Tests
# ==============================================================================

@pytest.mark.asyncio
async def test_bot_start_background_and_stop(test_bot):
    """Verify clean background startup and graceful shutdown."""
    test_bot.get_updates = AsyncMock(return_value=[])

    task = test_bot.start_background()
    assert task is not None
    assert test_bot.running is True

    await asyncio.sleep(0.05)
    await test_bot.stop()
    assert test_bot.running is False
    assert task.done() or task.cancelled()


@pytest.mark.asyncio
async def test_run_bot_admin_listener_helper(monkeypatch):
    """Verify run_bot_admin_listener starts and terminates with shutdown_event."""
    shutdown_event = asyncio.Event()

    mock_bot = MagicMock()
    mock_task = asyncio.create_task(asyncio.sleep(10))
    mock_bot.start_background.return_value = mock_task
    mock_bot.stop = AsyncMock()

    monkeypatch.setattr("modules.bot_admin.bot_admin", mock_bot)

    listener_task = asyncio.create_task(run_bot_admin_listener(shutdown_event))
    await asyncio.sleep(0.02)
    shutdown_event.set()

    await listener_task
    mock_bot.stop.assert_called_once()
    mock_task.cancel()
