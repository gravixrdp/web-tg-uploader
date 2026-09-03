"""
Unit Tests for Admin Web Dashboard & REST API Module (modules/admin_web.py).
Tests all REST endpoints, CORS, dynamic settings, worker pause/resume, task lifecycle,
crawler trigger, test bot ping, in-memory log buffer, and error handling.
"""

import os
import shutil
import tempfile
import asyncio
import logging
from unittest.mock import patch, MagicMock, AsyncMock
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from modules.config import config
from modules.database import DatabaseManager, db_manager
from modules.crawler import UniversalCrawler
from modules.admin_web import (
    create_admin_app,
    start_admin_server,
    admin_state,
    log_buffer_handler,
    is_worker_paused,
    set_worker_paused,
    set_current_task,
    update_current_task_stage,
    clear_current_task,
    get_current_task,
    format_uptime
)


@pytest.fixture
def temp_db(tmp_path):
    """Provides a fresh temporary SQLite database for testing."""
    test_db_path = str(tmp_path / "test_admin_queue.db")
    test_db = DatabaseManager(test_db_path)
    old_db = db_manager.db_path
    db_manager.db_path = test_db_path
    db_manager.init_db()
    admin_state.init_from_db()
    yield test_db
    db_manager.db_path = old_db


@pytest.mark.asyncio
async def test_admin_root_html_and_json(temp_db):
    """Tests GET / returns HTML by default, and JSON when Accept: application/json."""
    app = create_admin_app()
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()

    try:
        # Default request returns static HTML dashboard
        resp = await client.get("/")
        assert resp.status == 200
        assert "text/html" in resp.headers.get("Content-Type", "")

        # Request with Accept: application/json returns JSON health summary
        resp_json = await client.get("/", headers={"Accept": "application/json"})
        assert resp_json.status == 200
        data = await resp_json.json()
        assert data["status"] == "healthy"
        assert "queue" in data
        assert "uptime_seconds" in data
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_health_endpoints(temp_db):
    """Tests GET /health and GET /healthz."""
    app = create_admin_app()
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()

    try:
        for path in ("/health", "/healthz"):
            resp = await client.get(path)
            assert resp.status == 200
            data = await resp.json()
            assert data["status"] == "healthy"
            assert "queue" in data
            assert "uptime_seconds" in data
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_api_stats(temp_db):
    """Tests GET /api/stats endpoint."""
    app = create_admin_app()
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()

    try:
        # Set task in progress
        set_current_task(101, "https://example.com/live.mp4", "Live Video", stage="DOWNLOADING")
        assert get_current_task() is not None

        resp = await client.get("/api/stats")
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "ok"
        assert "stats" in data
        assert data["worker_state"] == "BUSY"
        assert data["current_task"]["id"] == 101

        # Clear task
        clear_current_task()
        assert get_current_task() is None

        resp2 = await client.get("/api/stats")
        data2 = await resp2.json()
        assert data2["worker_state"] == "IDLE"
        assert data2["current_task"] is None
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_tasks_list_and_pagination(temp_db):
    """Tests GET /api/tasks with pagination, status filter, and search query."""
    # Seed tasks
    db_manager.enqueue_batch([
        {"url": "https://example.com/v1.mp4", "title": "Nature 4K Video"},
        {"url": "https://example.com/v2.mp4", "title": "Wildlife Documentary"},
        {"url": "https://example.com/v3.mp4", "title": "City Skyline"},
    ])

    app = create_admin_app()
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()

    try:
        # 1. Fetch all
        resp = await client.get("/api/tasks")
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "ok"
        assert data["total"] == 3
        assert len(data["tasks"]) == 3

        # 2. Pagination
        resp_p = await client.get("/api/tasks?limit=2&offset=0")
        assert resp_p.status == 200
        data_p = await resp_p.json()
        assert data_p["limit"] == 2
        assert len(data_p["tasks"]) == 2

        # 3. Search query
        resp_s = await client.get("/api/tasks?search=Wildlife")
        assert resp_s.status == 200
        data_s = await resp_s.json()
        assert data_s["total"] == 1
        assert "Wildlife" in data_s["tasks"][0]["title"]

        # 4. Status filter
        resp_st = await client.get("/api/tasks?status=COMPLETED")
        assert resp_st.status == 200
        data_st = await resp_st.json()
        assert data_st["total"] == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_task_enqueue(temp_db):
    """Tests POST /api/tasks/enqueue."""
    app = create_admin_app()
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()

    try:
        # Valid enqueue
        resp = await client.post("/api/tasks/enqueue", json={"url": "https://example.com/test.mp4", "title": "Test Enqueue"})
        assert resp.status == 201
        data = await resp.json()
        assert data["status"] == "ok"
        assert data["is_new"] is True
        assert data["task_id"] is not None

        # Duplicate enqueue returns 200 is_new=False
        resp_dup = await client.post("/api/tasks/enqueue", json={"url": "https://example.com/test.mp4"})
        assert resp_dup.status == 200
        data_dup = await resp_dup.json()
        assert data_dup["is_new"] is False

        # Invalid URL
        resp_inv = await client.post("/api/tasks/enqueue", json={"url": "not-a-url"})
        assert resp_inv.status == 400

        # Missing body
        resp_empty = await client.post("/api/tasks/enqueue", json={})
        assert resp_empty.status == 400
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_task_retry_and_delete(temp_db):
    """Tests POST /api/tasks/retry and POST /api/tasks/delete."""
    db_manager.enqueue_batch([
        {"url": "https://example.com/fail1.mp4", "title": "Failed Task 1"},
        {"url": "https://example.com/fail2.mp4", "title": "Failed Task 2"}
    ])
    t1 = db_manager.get_next_pending()
    t2 = db_manager.get_next_pending()
    db_manager.set_status(t1["id"], "FAILED", error_message="HTTP 500")
    db_manager.set_status(t2["id"], "FAILED", error_message="Timeout")

    app = create_admin_app()
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()

    try:
        # 1. Retry single task by JSON body
        resp = await client.post("/api/tasks/retry", json={"id": t1["id"]})
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "ok"
        assert data["task_id"] == t1["id"]

        t1_updated = db_manager.get_task(t1["id"])
        assert t1_updated["status"] == "PENDING"

        # 2. Retry all remaining failed tasks
        resp_all = await client.post("/api/tasks/retry", json={})
        assert resp_all.status == 200
        data_all = await resp_all.json()
        assert data_all["reset_count"] == 1

        # 3. Delete task
        resp_del = await client.post("/api/tasks/delete", json={"id": t1["id"]})
        assert resp_del.status == 200
        data_del = await resp_del.json()
        assert data_del["deleted"] is True
        assert db_manager.get_task(t1["id"]) is None

        # 4. Delete non-existent task returns 404
        resp_del404 = await client.post("/api/tasks/delete", json={"id": 99999})
        assert resp_del404.status == 404

        # 5. Delete via DELETE /api/tasks/{id}
        resp_del_rest = await client.delete(f"/api/tasks/{t2['id']}")
        assert resp_del_rest.status == 200
        assert db_manager.get_task(t2["id"]) is None
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_crawler_trigger(temp_db):
    """Tests POST /api/crawler/trigger."""
    app = create_admin_app()
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()

    try:
        with patch.object(UniversalCrawler, "discover", new_callable=AsyncMock) as mock_discover:
            mock_discover.return_value = [{"url": "https://example.com/crawled.mp4", "title": "Crawled 1"}]

            resp = await client.post("/api/crawler/trigger", json={
                "url": "https://example.com/feed.xml",
                "mode": "rss",
                "max_pages": 5
            })
            assert resp.status == 202
            data = await resp.json()
            assert data["status"] == "ok"
            assert data["job"]["url"] == "https://example.com/feed.xml"

            # Allow background job coroutine to run
            await asyncio.sleep(0.1)

            job = admin_state.get_crawler_job()
            assert job is not None
            assert job["status"] == "completed"
            assert job["discovered"] == 1
            assert job["enqueued"] == 1

        # Test invalid mode
        resp_inv = await client.post("/api/crawler/trigger", json={
            "url": "https://example.com",
            "mode": "invalid_mode_xyz"
        })
        assert resp_inv.status == 400
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_settings_get_and_post(temp_db):
    """Tests GET /api/settings and POST /api/settings."""
    app = create_admin_app()
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()

    try:
        # GET default settings
        resp = await client.get("/api/settings")
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "ok"
        assert "settings" in data

        # POST updated settings
        payload = {
            "chat_id": "-1009876543210",
            "target_url": "https://newsite.com/videos",
            "mode": "sitemap",
            "cooldown": 30,
            "interval": 600,
            "worker_paused": True,
            "max_pages": 25
        }
        resp_post = await client.post("/api/settings", json=payload)
        assert resp_post.status == 200
        data_post = await resp_post.json()
        assert data_post["status"] == "ok"
        assert data_post["settings"]["chat_id"] == "-1009876543210"
        assert data_post["settings"]["mode"] == "sitemap"
        assert data_post["settings"]["cooldown"] == 30
        assert data_post["settings"]["worker_paused"] is True

        # Verify persisted in DB
        assert db_manager.get_setting("chat_id") == "-1009876543210"
        assert db_manager.get_setting("mode") == "sitemap"
        assert db_manager.is_worker_paused() is True

        # Verify memory config updated
        assert config.TELEGRAM_CHAT_ID == "-1009876543210"
        assert config.CRAWL_MODE == "sitemap"
        assert config.UPLOAD_COOLDOWN == 30
        assert is_worker_paused() is True
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_worker_toggle(temp_db):
    """Tests POST /api/worker/toggle."""
    app = create_admin_app()
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()

    try:
        set_worker_paused(False)
        assert is_worker_paused() is False

        # Toggle to True
        resp1 = await client.post("/api/worker/toggle")
        assert resp1.status == 200
        data1 = await resp1.json()
        assert data1["worker_paused"] is True
        assert is_worker_paused() is True

        # Toggle to False
        resp2 = await client.post("/api/worker/toggle")
        assert resp2.status == 200
        data2 = await resp2.json()
        assert data2["worker_paused"] is False
        assert is_worker_paused() is False

        # Explicit set via body
        resp3 = await client.post("/api/worker/toggle", json={"paused": True})
        assert resp3.status == 200
        data3 = await resp3.json()
        assert data3["worker_paused"] is True
        assert is_worker_paused() is True
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_bot_test_ping(temp_db):
    """Tests POST /api/bot/test with mocked Telegram API."""
    app = create_admin_app()
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()

    try:
        # Mock verify_bot_token and aiohttp ClientSession POST
        with patch.object(config, "TELEGRAM_BOT_TOKEN", "123456789:ABCdefGHIjklMNOpqrsTUVwxyz1234567890"), \
             patch.object(config, "TELEGRAM_CHAT_ID", "-1001234567890"), \
             patch("modules.uploader.TelegramBotUploader.verify_bot_token", new_callable=AsyncMock) as mock_verify:

            mock_verify.return_value = (True, "TestScraperBot")

            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json = AsyncMock(return_value={"ok": True, "result": {"message_id": 999}})

            mock_post_ctx = MagicMock()
            mock_post_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_post_ctx.__aexit__ = AsyncMock(return_value=None)

            mock_session_inst = MagicMock()
            mock_session_inst.__aenter__ = AsyncMock(return_value=mock_session_inst)
            mock_session_inst.__aexit__ = AsyncMock(return_value=None)
            mock_session_inst.post.return_value = mock_post_ctx

            with patch("aiohttp.ClientSession", return_value=mock_session_inst):
                resp = await client.post("/api/bot/test", json={"chat_id": "-1001234567890"})
                assert resp.status == 200
                data = await resp.json()
                assert data["status"] == "ok"
                assert "Test ping sent successfully" in data["message"]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_api_logs(temp_db):
    """Tests GET /api/logs endpoint and log buffer filtering."""
    app = create_admin_app()
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()

    try:
        # Emit some test logs
        test_logger = logging.getLogger("TestLogger")
        test_logger.setLevel(logging.INFO)
        test_logger.info("Informational message for test")
        test_logger.warning("Warning message for test")
        test_logger.error("Error message for test")

        # 1. Fetch all logs
        resp = await client.get("/api/logs?limit=50")
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "ok"
        assert data["count"] >= 3

        # 2. Filter by level
        resp_err = await client.get("/api/logs?level=ERROR")
        assert resp_err.status == 200
        data_err = await resp_err.json()
        for item in data_err["logs"]:
            assert item["level"] == "ERROR"

        # 3. Clear logs
        resp_clear = await client.get("/api/logs?clear=true")
        assert resp_clear.status == 200
        
        # Next fetch should be empty or near empty
        resp_after = await client.get("/api/logs")
        data_after = await resp_after.json()
        assert data_after["count"] == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_cors_options(temp_db):
    """Tests CORS middleware handling OPTIONS preflight."""
    app = create_admin_app()
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()

    try:
        resp = await client.options("/api/tasks")
        assert resp.status == 204
        assert resp.headers.get("Access-Control-Allow-Origin") == "*"
        assert "POST" in resp.headers.get("Access-Control-Allow-Methods", "")
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_admin_server_lifecycle():
    """Tests start_admin_server lifecycle."""
    runner = await start_admin_server("127.0.0.1", 18099)
    assert runner is not None
    await runner.cleanup()


def test_format_uptime():
    """Tests format_uptime helper."""
    assert format_uptime(45) == "45s"
    assert format_uptime(125) == "2m 5s"
    assert format_uptime(3665) == "1h 1m 5s"
    assert format_uptime(90065) == "1d 1h 1m 5s"
