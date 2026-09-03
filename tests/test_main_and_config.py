import os
import sys
import asyncio
import pytest
import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from modules.config import Config, config
from main import (
    create_health_app,
    start_health_server,
    run_worker_phase,
    run_discovery_phase
)


@pytest.mark.asyncio
async def test_config_defaults():
    cfg = Config()
    assert cfg.PORT == 8080 or cfg.HTTP_PORT == 8080
    assert cfg.HOST == "0.0.0.0"
    assert cfg.UPLOAD_COOLDOWN == 20
    assert cfg.MAX_RETRIES == 5
    assert cfg.CRAWL_MODE == "auto"


@pytest.mark.asyncio
async def test_config_summary():
    summary = config.get_safe_summary()
    assert "PORT" in summary
    assert "DB_PATH" in summary
    assert "CRAWL_MODE" in summary
    assert "TELEGRAM_BOT_TOKEN" in summary


@pytest.mark.asyncio
async def test_health_check_endpoints():
    app = create_health_app()
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()

    try:
        # Test GET / (with JSON accept header)
        resp = await client.get("/", headers={"Accept": "application/json"})
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "healthy"
        assert "uptime_seconds" in data
        assert "queue" in data

        # Test GET /health
        resp = await client.get("/health")
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "healthy"
        assert "queue" in data

        # Test GET /healthz
        resp = await client.get("/healthz")
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "healthy"

        # Test GET /stats
        resp = await client.get("/stats")
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "ok"
        assert "stats" in data
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_health_server_lifecycle():
    runner = await start_health_server("127.0.0.1", 18088)
    assert runner is not None
    await runner.cleanup()


@pytest.mark.asyncio
async def test_worker_responsive_shutdown():
    shutdown_event = asyncio.Event()

    async def trigger_shutdown_soon():
        await asyncio.sleep(0.1)
        shutdown_event.set()

    shutdown_task = asyncio.create_task(trigger_shutdown_soon())
    await asyncio.wait_for(run_worker_phase(shutdown_event), timeout=3.0)
    await shutdown_task


@pytest.mark.asyncio
async def test_concurrent_health_and_worker():
    # Start health server on test port
    runner = await start_health_server("127.0.0.1", 18089)
    assert runner is not None

    shutdown_event = asyncio.Event()

    # Run worker in background
    worker_task = asyncio.create_task(run_worker_phase(shutdown_event))

    # Query health server while worker is running
    async with aiohttp.ClientSession() as session:
        async with session.get("http://127.0.0.1:18089/health") as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["status"] == "healthy"

    # Stop worker and server
    shutdown_event.set()
    await worker_task
    await runner.cleanup()


def test_cli_stats(capsys):
    import subprocess
    result = subprocess.run(
        [sys.executable, "main.py", "--stats"],
        capture_output=True,
        text=True,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    assert result.returncode == 0
    assert "Current Queue Statistics" in result.stdout

