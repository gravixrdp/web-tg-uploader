"""
Web Admin & REST API Module for Bulk Video Scraper & Telegram Uploader.
Provides full HTTP backend for the responsive Admin Dashboard, REST APIs, and Railway health probes.
"""

import os
import time
import asyncio
import logging
from typing import Optional, Dict, Any, List, Tuple, Union
from collections import deque
from aiohttp import web

from modules.config import config, WEB_PANEL_URL, VALID_CRAWL_MODES
from modules.database import db_manager
from modules.crawler import UniversalCrawler
from modules.uploader import uploader
from modules.admin_ui import get_admin_html, STATIC_DIR

logger = logging.getLogger("AdminWeb")

_START_TIME = time.time()


def format_uptime(seconds: float) -> str:
    """Format seconds into human-readable duration (e.g. '1d 2h 3m 4s')."""
    sec = int(seconds)
    days = sec // 86400
    hours = (sec % 86400) // 3600
    minutes = (sec % 3600) // 60
    s = sec % 60

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if s > 0 or not parts:
        parts.append(f"{s}s")

    return " ".join(parts)


def is_worker_paused() -> bool:
    """Returns whether the sequential worker has been paused via dynamic settings or admin dashboard."""
    return db_manager.is_worker_paused()


def set_worker_paused(paused: bool) -> bool:
    """Sets the sequential worker paused state in database."""
    db_manager.set_worker_paused(paused)
    return db_manager.is_worker_paused()


def get_uptime_seconds() -> float:
    """Returns total service uptime in seconds."""
    return round(time.time() - _START_TIME, 2)


# ==============================================================================
# In-Memory Log Buffer & State Tracking
# ==============================================================================

class InMemoryLogHandler(logging.Handler):
    """Circular buffer capturing the most recent log entries for the Web Admin dashboard."""

    def __init__(self, capacity: int = 200):
        super().__init__()
        self.capacity = capacity
        self.buffer: deque = deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            entry = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created)),
                "level": record.levelname,
                "name": record.name,
                "message": record.getMessage(),
                "formatted": msg
            }
            self.buffer.append(entry)
        except Exception:
            self.handleError(record)

    def get_logs(self, limit: int = 50, level: Optional[str] = None) -> List[Dict[str, Any]]:
        logs = list(self.buffer)
        if level:
            level_upper = level.upper()
            logs = [entry for entry in logs if entry["level"] == level_upper]
        return logs[-limit:]

    def clear(self) -> None:
        self.buffer.clear()


log_buffer_handler = InMemoryLogHandler(capacity=300)
log_buffer_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s"))
logging.getLogger().addHandler(log_buffer_handler)


class AdminRuntimeState:
    """Tracks active crawler jobs and currently downloading/uploading video task."""

    def __init__(self):
        self.current_task: Optional[Dict[str, Any]] = None
        self.crawler_job: Optional[Dict[str, Any]] = None
        self.active_scrape_tasks: List[asyncio.Task] = []

    def init_from_db(self) -> None:
        """Resets runtime memory cache from database settings."""
        self.current_task = None
        self.crawler_job = None

    def set_current_task(self, task_id: int, url: str, title: str, stage: str = "DOWNLOADING", file_size: int = 0) -> None:
        self.current_task = {
            "id": task_id,
            "url": url,
            "title": title,
            "stage": stage,
            "file_size": file_size,
            "started_at": time.time()
        }

    def update_current_task_stage(self, stage: str, file_size: Optional[int] = None) -> None:
        if self.current_task:
            self.current_task["stage"] = stage
            if file_size is not None:
                self.current_task["file_size"] = file_size

    def clear_current_task(self) -> None:
        self.current_task = None

    def get_current_task(self) -> Optional[Dict[str, Any]]:
        return self.current_task

    def set_crawler_job(self, url: str, mode: str, status: str = "running", discovered: int = 0, enqueued: int = 0, error: Optional[str] = None) -> None:
        self.crawler_job = {
            "url": url,
            "mode": mode,
            "status": status,
            "discovered": discovered,
            "enqueued": enqueued,
            "error": error,
            "updated_at": time.time()
        }

    def get_crawler_job(self) -> Optional[Dict[str, Any]]:
        return self.crawler_job


admin_state = AdminRuntimeState()

# Convenient module-level aliases
set_current_task = admin_state.set_current_task
update_current_task_stage = admin_state.update_current_task_stage
clear_current_task = admin_state.clear_current_task
get_current_task = admin_state.get_current_task


# ==============================================================================
# CORS Middleware
# ==============================================================================

@web.middleware
async def cors_middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        response = web.Response(status=204)
    else:
        try:
            response = await handler(request)
        except web.HTTPException as ex:
            response = ex

    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Requested-With"
    return response


# ==============================================================================
# Route Handlers
# ==============================================================================

async def handle_root(request: web.Request) -> web.Response:
    """Root endpoint (GET /) serving Admin Dashboard or JSON for API clients."""
    accept_header = request.headers.get("Accept", "")
    if "application/json" in accept_header and "text/html" not in accept_header:
        return await handle_health(request)

    html_content = get_admin_html()
    return web.Response(text=html_content, content_type="text/html", charset="utf-8")


async def handle_admin(request: web.Request) -> web.Response:
    """Explicit Admin Dashboard endpoint (GET /admin, GET /dashboard)."""
    html_content = get_admin_html()
    return web.Response(text=html_content, content_type="text/html", charset="utf-8")


async def handle_health(request: web.Request) -> web.Response:
    """Health check endpoint (GET /health, GET /healthz)."""
    uptime = get_uptime_seconds()
    stats = db_manager.get_stats()
    return web.json_response({
        "status": "healthy",
        "service": "Bulk Video Scraper & Telegram Uploader",
        "uptime_seconds": uptime,
        "uptime_human": format_uptime(uptime),
        "worker_paused": db_manager.is_worker_paused(),
        "queue": stats,
    })


async def handle_stats(request: web.Request) -> web.Response:
    """Detailed statistics endpoint (GET /stats, GET /api/stats)."""
    stats = db_manager.get_detailed_stats()
    uptime = get_uptime_seconds()
    safe_config = config.get_safe_summary()
    paused = db_manager.is_worker_paused()
    curr_task = admin_state.get_current_task()
    worker_state = "PAUSED" if paused else ("BUSY" if curr_task else "IDLE")

    return web.json_response({
        "status": "ok",
        "uptime_seconds": uptime,
        "uptime_human": format_uptime(uptime),
        "worker_paused": paused,
        "worker_state": worker_state,
        "current_task": curr_task,
        "crawler_job": admin_state.get_crawler_job(),
        "stats": stats,
        "config": safe_config
    })


async def handle_get_tasks(request: web.Request) -> web.Response:
    """Paginated tasks list endpoint (GET /api/tasks)."""
    params = request.query
    status = params.get("status", "ALL").strip()
    search = params.get("search", "").strip() or None
    order_desc = params.get("order", "desc").lower() != "asc"

    try:
        limit = max(1, min(200, int(params.get("limit", 25))))
    except ValueError:
        limit = 25

    # Check offset vs page
    if "offset" in params:
        try:
            offset = max(0, int(params.get("offset", 0)))
            page = (offset // limit) + 1
        except ValueError:
            offset = 0
            page = 1
    else:
        try:
            page = max(1, int(params.get("page", 1)))
        except ValueError:
            page = 1
        offset = (page - 1) * limit

    # Query tasks using database manager
    status_filter = None if status.upper() in ("ALL", "") else status.upper()
    tasks, total = db_manager.get_tasks(
        status=status_filter,
        search=search,
        limit=limit,
        offset=offset
    )

    total_pages = (total + limit - 1) // limit if total > 0 else 1

    return web.json_response({
        "status": "ok",
        "tasks": tasks,
        "total": total,
        "page": page,
        "pages": total_pages,
        "limit": limit,
        "offset": offset
    })


async def handle_get_task_by_id(request: web.Request) -> web.Response:
    """Fetch single task by ID (GET /api/tasks/{id})."""
    task_id_raw = request.match_info.get("id")
    try:
        task_id = int(task_id_raw)
    except (ValueError, TypeError):
        return web.json_response({"status": "error", "message": f"Invalid task id: {task_id_raw}"}, status=400)

    task = db_manager.get_task(task_id)
    if task:
        return web.json_response({"status": "ok", "task": task})
    return web.json_response({"status": "error", "message": f"Task #{task_id} not found"}, status=404)


async def handle_create_task(request: web.Request) -> web.Response:
    """Enqueue single video endpoint (POST /api/tasks, POST /api/tasks/enqueue)."""
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "Invalid JSON body"}, status=400)

    url = (body.get("url") or body.get("video_url") or "").strip()
    if not url or not (url.startswith("http://") or url.startswith("https://")):
        return web.json_response({"status": "error", "message": "Valid URL (http/https) is required"}, status=400)

    title = (body.get("title") or "").strip() or None
    inserted, task_id = db_manager.enqueue_one(url, title=title)

    if inserted:
        logger.info(f"Task #{task_id} manually enqueued: {url}")
        return web.json_response({
            "status": "ok",
            "message": f"Task #{task_id} successfully added to queue",
            "id": task_id,
            "task_id": task_id,
            "is_new": True,
            "inserted": True
        }, status=201)
    else:
        logger.info(f"URL already in queue as Task #{task_id}: {url}")
        return web.json_response({
            "status": "ok",
            "message": f"URL already exists in queue as Task #{task_id}",
            "id": task_id,
            "task_id": task_id,
            "is_new": False,
            "inserted": False
        }, status=200)


async def handle_retry_task(request: web.Request) -> web.Response:
    """Reset task(s) to PENDING (POST /api/tasks/retry, POST /api/tasks/{id}/retry)."""
    task_id = None
    task_id_raw = request.match_info.get("id")
    if task_id_raw:
        try:
            task_id = int(task_id_raw)
        except (ValueError, TypeError):
            return web.json_response({"status": "error", "message": f"Invalid task id: {task_id_raw}"}, status=400)
    else:
        try:
            body = await request.json()
            if "id" in body and body["id"] is not None:
                task_id = int(body["id"])
        except Exception:
            pass

    if task_id is not None:
        success = db_manager.retry_task(task_id)
        if success:
            return web.json_response({"status": "ok", "message": f"Task #{task_id} reset to PENDING", "task_id": task_id, "reset_count": 1})
        else:
            return web.json_response({"status": "error", "message": f"Task #{task_id} not found"}, status=404)
    else:
        count = db_manager.retry_all_failed()
        return web.json_response({"status": "ok", "message": f"Reset {count} failed task(s) to PENDING", "reset_count": count})


async def handle_delete_task(request: web.Request) -> web.Response:
    """Delete a task (POST /api/tasks/delete, DELETE /api/tasks/{id})."""
    task_id = None
    task_id_raw = request.match_info.get("id")
    if task_id_raw:
        try:
            task_id = int(task_id_raw)
        except (ValueError, TypeError):
            return web.json_response({"status": "error", "message": f"Invalid task id: {task_id_raw}"}, status=400)
    else:
        try:
            body = await request.json()
            if "id" in body and body["id"] is not None:
                task_id = int(body["id"])
        except Exception:
            pass

    if task_id is None:
        return web.json_response({"status": "error", "message": "Task ID is required"}, status=400)

    deleted = db_manager.delete_task(task_id)
    if deleted:
        return web.json_response({"status": "ok", "message": f"Task #{task_id} deleted successfully", "deleted": True})
    else:
        return web.json_response({"status": "error", "message": f"Task #{task_id} not found", "deleted": False}, status=404)


async def handle_retry_failed_queue(request: web.Request) -> web.Response:
    """Reset failed tasks (POST /api/queue/retry-failed)."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    max_retries = int(body.get("max_retries", 5))
    count = db_manager.retry_failed_tasks(max_retries=max_retries)
    return web.json_response({
        "status": "ok",
        "message": f"Reset {count} failed task(s) back to PENDING",
        "reset_count": count
    })


async def handle_reset_stalled_queue(request: web.Request) -> web.Response:
    """Reset stalled tasks (POST /api/queue/reset-stalled, POST /api/tasks/reset-stalled)."""
    count = db_manager.reset_stalled_tasks()
    return web.json_response({
        "status": "ok",
        "message": f"Reset {count} stranded task(s) back to PENDING",
        "reset_count": count
    })


async def handle_purge_completed_queue(request: web.Request) -> web.Response:
    """Purge completed tasks older than N days (POST /api/queue/purge-completed)."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    days = int(body.get("days", 0))
    count = db_manager.purge_completed(days=days)
    return web.json_response({
        "status": "ok",
        "message": f"Purged {count} completed task(s)",
        "deleted_count": count
    })


async def handle_clear_queue(request: web.Request) -> web.Response:
    """Clear queue items (POST /api/tasks/clear, POST /api/queue/clear)."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    status = body.get("status", "ALL")
    count = db_manager.clear_queue(status=status)
    return web.json_response({
        "status": "ok",
        "message": f"Cleared {count} task(s) from queue",
        "deleted_count": count
    })


async def _background_scrape_job(url: str, mode: str, max_pages: int) -> None:
    """Executes a scraper discovery job in the background and enqueues found items."""
    admin_state.set_crawler_job(url, mode, status="running")
    logger.info(f"Background scrape job started: URL={url}, Mode={mode}, MaxPages={max_pages}")
    crawler = UniversalCrawler()
    try:
        items = await crawler.discover(url, mode=mode, max_pages=max_pages)
        if items:
            inserted, ignored = db_manager.enqueue_batch(items)
            admin_state.set_crawler_job(url, mode, status="completed", discovered=len(items), enqueued=inserted)
            logger.info(f"Background scrape job finished for {url}: {len(items)} found, {inserted} enqueued, {ignored} duplicates skipped.")
        else:
            admin_state.set_crawler_job(url, mode, status="completed", discovered=0, enqueued=0)
            logger.warning(f"Background scrape job for {url} completed with 0 items.")
    except Exception as e:
        err_msg = str(e)
        admin_state.set_crawler_job(url, mode, status="failed", error=err_msg)
        logger.error(f"Error during background scrape job for {url}: {e}", exc_info=True)


async def handle_scrape(request: web.Request) -> web.Response:
    """Trigger manual website scraping job (POST /api/scrape, POST /api/crawl, POST /api/crawler/trigger)."""
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "Invalid JSON body"}, status=400)

    url = (body.get("url") or "").strip()
    if not url:
        return web.json_response({"status": "error", "message": "Target URL is required"}, status=400)

    mode = (body.get("mode") or "auto").strip().lower()
    if mode not in VALID_CRAWL_MODES:
        return web.json_response({
            "status": "error",
            "message": f"Invalid mode '{mode}'. Allowed: {', '.join(sorted(VALID_CRAWL_MODES))}"
        }, status=400)

    try:
        max_pages = max(1, min(100, int(body.get("max_pages", 10))))
    except ValueError:
        max_pages = 10

    # Launch background crawler task
    task = asyncio.create_task(_background_scrape_job(url, mode, max_pages))
    admin_state.active_scrape_tasks.append(task)
    task.add_done_callback(lambda t: admin_state.active_scrape_tasks.remove(t) if t in admin_state.active_scrape_tasks else None)

    return web.json_response({
        "status": "ok",
        "message": f"Discovery crawl initiated in background for {url} (mode: {mode}, max_pages: {max_pages})",
        "job": {
            "url": url,
            "mode": mode,
            "max_pages": max_pages,
            "status": "queued"
        }
    }, status=202)


async def handle_get_settings(request: web.Request) -> web.Response:
    """Retrieve runtime settings and effective values (GET /api/settings)."""
    db_settings = db_manager.get_all_settings()
    active_chat_id = db_manager.get_active_chat_id()
    active_url, active_mode = db_manager.get_active_crawl_target()
    active_cooldown = db_manager.get_active_cooldown()
    active_max_pages = db_manager.get_active_max_pages()
    active_interval = db_manager.get_active_periodic_crawl_interval()
    paused = db_manager.is_worker_paused()

    safe_summary = config.get_safe_summary()
    safe_summary["TELEGRAM_CHAT_ID"] = active_chat_id
    safe_summary["CRAWL_TARGET_URL"] = active_url
    safe_summary["CRAWL_MODE"] = active_mode
    safe_summary["UPLOAD_COOLDOWN"] = active_cooldown
    safe_summary["PERIODIC_CRAWL_INTERVAL"] = active_interval

    return web.json_response({
        "status": "ok",
        "settings": {
            "chat_id": active_chat_id,
            "target_url": active_url,
            "mode": active_mode,
            "cooldown": active_cooldown,
            "max_pages": active_max_pages,
            "interval": active_interval,
            "worker_paused": paused,
            **db_settings
        },
        "effective": {
            "chat_id": active_chat_id,
            "target_url": active_url,
            "crawl_mode": active_mode,
            "cooldown": active_cooldown,
            "max_pages": active_max_pages,
            "periodic_interval": active_interval,
            "worker_paused": paused
        },
        **safe_summary
    })


async def handle_update_settings(request: web.Request) -> web.Response:
    """Update runtime settings dynamically (POST /api/settings)."""
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"status": "error", "message": "Invalid JSON body"}, status=400)

    updated_keys = []

    # Format: { "chat_id": "...", "target_url": "...", "mode": "...", "cooldown": 20, "interval": 0, "worker_paused": bool }
    mapping = {
        "TELEGRAM_CHAT_ID": "chat_id",
        "chat_id": "chat_id",
        "CRAWL_TARGET_URL": "target_url",
        "target_url": "target_url",
        "crawl_target_url": "target_url",
        "CRAWL_MODE": "mode",
        "crawl_mode": "mode",
        "mode": "mode",
        "UPLOAD_COOLDOWN": "cooldown",
        "upload_cooldown": "cooldown",
        "cooldown": "cooldown",
        "PERIODIC_CRAWL_INTERVAL": "interval",
        "periodic_crawl_interval": "interval",
        "interval": "interval",
        "MAX_PAGES": "max_pages",
        "max_pages": "max_pages",
        "worker_paused": "worker_paused"
    }

    for key, db_key in mapping.items():
        if key in body:
            val = body[key]
            str_val = str(val).strip()
            db_manager.set_setting(db_key, str_val)
            if db_key not in updated_keys:
                updated_keys.append(db_key)

            # Sync in-memory config
            if db_key == "chat_id":
                config.TELEGRAM_CHAT_ID = str_val
                uploader.chat_id = str_val
            elif db_key == "target_url":
                config.CRAWL_TARGET_URL = str_val
            elif db_key == "mode":
                config.CRAWL_MODE = str_val
            elif db_key == "cooldown":
                try:
                    config.UPLOAD_COOLDOWN = max(0, int(val))
                    uploader.cooldown = config.UPLOAD_COOLDOWN
                except ValueError:
                    pass
            elif db_key == "interval":
                try:
                    config.PERIODIC_CRAWL_INTERVAL = max(0, int(val))
                except ValueError:
                    pass
            elif db_key == "worker_paused":
                db_manager.set_worker_paused(str_val.lower() in ("true", "1", "yes"))

    logger.info(f"Runtime settings updated: {updated_keys}")
    return web.json_response({
        "status": "ok",
        "message": "Settings updated successfully",
        "updated": updated_keys,
        "settings": {
            "chat_id": db_manager.get_active_chat_id(),
            "target_url": db_manager.get_active_crawl_target()[0],
            "mode": db_manager.get_active_crawl_target()[1],
            "cooldown": db_manager.get_active_cooldown(),
            "max_pages": db_manager.get_active_max_pages(),
            "interval": db_manager.get_active_periodic_crawl_interval(),
            "worker_paused": db_manager.is_worker_paused()
        }
    })


async def handle_test_telegram(request: web.Request) -> web.Response:
    """Sends a live test message to the configured Telegram destination chat (POST /api/bot/test, POST /api/test-telegram)."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    target_chat = body.get("chat_id") or db_manager.get_active_chat_id()
    if not target_chat:
        return web.json_response({
            "status": "error",
            "message": "Telegram Chat ID is not configured. Please specify a Chat ID."
        }, status=400)

    if not config.TELEGRAM_BOT_TOKEN:
        return web.json_response({
            "status": "error",
            "message": "TELEGRAM_BOT_TOKEN is not set in environment."
        }, status=400)

    bot_ok, bot_name = await uploader.verify_bot_token()
    if not bot_ok:
        return web.json_response({
            "status": "error",
            "message": f"Telegram Bot Token verification failed: {bot_name}"
        }, status=400)

    test_text = (
        "🤖 <b>TeleScraper Admin Test Ping</b>\n\n"
        "✅ Telegram Bot connection is operating normally!\n"
        f"⏱ <i>Timestamp:</i> <code>{time.strftime('%Y-%m-%d %H:%M:%S')}</code>"
    )

    url = f"{uploader.bot_url}/sendMessage"
    payload = {
        "chat_id": target_chat,
        "text": test_text,
        "parse_mode": "HTML"
    }

    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
                if data.get("ok"):
                    logger.info(f"Test Telegram ping sent successfully to {target_chat} via @{bot_name}")
                    return web.json_response({
                        "status": "ok",
                        "message": f"Test ping sent successfully to {target_chat} via @{bot_name}",
                        "bot": bot_name
                    })
                else:
                    error_desc = data.get("description", "Unknown Telegram error")
                    logger.error(f"Telegram sendMessage test failed: {error_desc}")
                    return web.json_response({
                        "status": "error",
                        "message": f"Telegram API error: {error_desc}"
                    }, status=400)
    except Exception as e:
        logger.error(f"Error testing Telegram connection: {e}", exc_info=True)
        return web.json_response({
            "status": "error",
            "message": f"Connection error: {str(e)}"
        }, status=500)


async def handle_worker_status(request: web.Request) -> web.Response:
    """Returns worker running / paused state (GET /api/worker, GET /api/worker/status)."""
    paused = db_manager.is_worker_paused()
    curr_task = admin_state.get_current_task()
    return web.json_response({
        "status": "ok",
        "worker_paused": paused,
        "worker_state": "PAUSED" if paused else ("BUSY" if curr_task else "IDLE"),
        "current_task": curr_task
    })


async def handle_worker_toggle(request: web.Request) -> web.Response:
    """Toggles worker running / paused state (POST /api/worker/toggle, POST /api/worker/pause, POST /api/worker/resume)."""
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    if "paused" in body:
        new_state = bool(body["paused"])
    else:
        new_state = not db_manager.is_worker_paused()

    db_manager.set_worker_paused(new_state)
    logger.info(f"Worker state set to: {'PAUSED' if new_state else 'RUNNING'}")
    return web.json_response({
        "status": "ok",
        "worker_paused": new_state,
        "message": f"Worker pipeline {'paused' if new_state else 'resumed'}"
    })


async def handle_get_logs(request: web.Request) -> web.Response:
    """Returns recent log events (GET /api/logs)."""
    params = request.query
    if params.get("clear", "").lower() in ("true", "1"):
        log_buffer_handler.clear()
        return web.json_response({"status": "ok", "message": "Log buffer cleared", "count": 0, "logs": []})

    limit = min(200, max(1, int(params.get("limit", 50))))
    level = params.get("level")
    logs = log_buffer_handler.get_logs(limit=limit, level=level)
    return web.json_response({
        "status": "ok",
        "count": len(logs),
        "logs": logs
    })


# ==============================================================================
# Web Application Factory
# ==============================================================================

def create_admin_app() -> web.Application:
    """Factory creating complete aiohttp web application with Admin Dashboard and REST API."""
    app = web.Application(middlewares=[cors_middleware])

    # 1. HTML Dashboard
    app.router.add_get("/", handle_root)
    app.router.add_get("/admin", handle_admin)
    app.router.add_get("/dashboard", handle_admin)

    # 2. Health Check & Stats
    app.router.add_get("/health", handle_health)
    app.router.add_get("/healthz", handle_health)
    app.router.add_get("/stats", handle_stats)
    app.router.add_get("/api/stats", handle_stats)

    # 3. Tasks REST API
    app.router.add_get("/api/tasks", handle_get_tasks)
    app.router.add_get("/api/tasks/{id}", handle_get_task_by_id)
    app.router.add_post("/api/tasks", handle_create_task)
    app.router.add_post("/api/tasks/enqueue", handle_create_task)
    app.router.add_post("/api/tasks/{id}/retry", handle_retry_task)
    app.router.add_post("/api/tasks/retry", handle_retry_task)
    app.router.add_post("/api/tasks/{id}/delete", handle_delete_task)
    app.router.add_post("/api/tasks/delete", handle_delete_task)
    app.router.add_delete("/api/tasks/{id}", handle_delete_task)
    app.router.add_post("/api/tasks/reset-stalled", handle_reset_stalled_queue)
    app.router.add_post("/api/tasks/clear", handle_clear_queue)

    # 4. Queue Control Operations
    app.router.add_post("/api/queue/retry-failed", handle_retry_failed_queue)
    app.router.add_post("/api/queue/reset-stalled", handle_reset_stalled_queue)
    app.router.add_post("/api/queue/purge-completed", handle_purge_completed_queue)
    app.router.add_post("/api/queue/clear", handle_clear_queue)

    # 5. Crawler / Scraper Trigger
    app.router.add_post("/api/scrape", handle_scrape)
    app.router.add_post("/api/crawl", handle_scrape)
    app.router.add_post("/api/crawler/trigger", handle_scrape)

    # 6. Settings & Diagnostics
    app.router.add_get("/api/settings", handle_get_settings)
    app.router.add_post("/api/settings", handle_update_settings)
    app.router.add_post("/api/test-telegram", handle_test_telegram)
    app.router.add_post("/api/bot/test", handle_test_telegram)

    # 7. Worker Controls
    app.router.add_get("/api/worker", handle_worker_status)
    app.router.add_get("/api/worker/status", handle_worker_status)
    app.router.add_post("/api/worker/toggle", handle_worker_toggle)
    app.router.add_post("/api/worker/pause", handle_worker_toggle)
    app.router.add_post("/api/worker/resume", handle_worker_toggle)

    # 8. Logs
    app.router.add_get("/api/logs", handle_get_logs)

    # 9. Static Assets
    if os.path.exists(STATIC_DIR):
        app.router.add_static("/static/", path=STATIC_DIR, name="static")

    return app


async def start_admin_server(host: str, port: int) -> Optional[web.AppRunner]:
    """Starts the async HTTP Admin & Health server concurrently on the event loop."""
    try:
        app = create_admin_app()
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, host=host, port=port)
        await site.start()
        logger.info(f"Admin Dashboard & API server started on http://{host}:{port} (Dashboard: http://{host}:{port}/)")
        return runner
    except Exception as e:
        logger.error(f"Failed to start Admin server on {host}:{port}: {e}", exc_info=True)
        return None
