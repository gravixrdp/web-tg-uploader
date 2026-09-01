"""
Main Orchestration Service for Bulk Video Scraper & Telegram Uploader.
Coordinates Discovery (Crawler) -> Resilient Queue (SQLite) -> Sequential Downloader (yt-dlp) -> Telegram Uploader.
Includes lightweight async HTTP health check endpoint on PORT for Railway container probes.
"""

import os
import sys
import time
import signal
import asyncio
import logging
import argparse
from typing import Optional, Dict, Any
from aiohttp import web

from modules.config import config
from modules.database import db_manager
from modules.crawler import UniversalCrawler
from modules.downloader import downloader
from modules.uploader import uploader

# Setup structured logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("App")

# Global lifecycle flags
RUNNING = True
START_TIME = time.time()


# ==============================================================================
# Lightweight Async HTTP Health Check Server
# ==============================================================================

async def handle_root(request: web.Request) -> web.Response:
    """
    Root endpoint returning service status, uptime, and current queue metrics.
    Suitable for Railway default health checks (GET /).
    """
    uptime = round(time.time() - START_TIME, 2)
    stats = db_manager.get_stats()
    return web.json_response({
        "status": "healthy",
        "service": "Bulk Video Scraper & Telegram Uploader",
        "uptime_seconds": uptime,
        "queue": stats,
    })


async def handle_health(request: web.Request) -> web.Response:
    """
    Dedicated health check endpoint (GET /health or GET /healthz).
    Returns 200 OK with queue status snapshot.
    """
    uptime = round(time.time() - START_TIME, 2)
    stats = db_manager.get_stats()
    return web.json_response({
        "status": "healthy",
        "uptime_seconds": uptime,
        "queue": stats,
    })


async def handle_stats(request: web.Request) -> web.Response:
    """Detailed queue statistics JSON endpoint (GET /stats)."""
    stats = db_manager.get_stats()
    return web.json_response({
        "status": "ok",
        "stats": stats,
    })


def create_health_app() -> web.Application:
    """Factory creating lightweight aiohttp web application for health probes."""
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/healthz", handle_health)
    app.router.add_get("/stats", handle_stats)
    return app


async def start_health_server(host: str, port: int) -> Optional[web.AppRunner]:
    """
    Starts the async HTTP health check server concurrently on the event loop.
    Returns the AppRunner instance for clean shutdown lifecycle management.
    """
    try:
        app = create_health_app()
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, host=host, port=port)
        await site.start()
        logger.info(f"Health check HTTP server started on http://{host}:{port} (routes: /, /health, /healthz, /stats)")
        return runner
    except Exception as e:
        logger.error(f"Failed to start health check HTTP server on {host}:{port}: {e}", exc_info=True)
        return None


# ==============================================================================
# Graceful Signal Handling
# ==============================================================================

def setup_signal_handlers(shutdown_event: asyncio.Event, loop: asyncio.AbstractEventLoop) -> None:
    """
    Registers clean shutdown handlers for SIGINT and SIGTERM.
    Supports both POSIX loops (Linux/Docker/Railway) and Windows proactor loops.
    """
    def _trigger_shutdown(sig_name: str) -> None:
        global RUNNING
        if not RUNNING:
            return
        logger.warning(f"Received exit signal ({sig_name}). Initiating graceful shutdown...")
        RUNNING = False
        shutdown_event.set()

    # POSIX / Unix: loop.add_signal_handler
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _trigger_shutdown, sig.name)
        except (NotImplementedError, AttributeError):
            # Fallback for Windows where loop.add_signal_handler is not implemented
            try:
                signal.signal(
                    sig,
                    lambda s, f, name=sig.name: loop.call_soon_threadsafe(_trigger_shutdown, name)
                )
            except Exception as e:
                logger.debug(f"Could not register signal handler for {sig}: {e}")


# ==============================================================================
# Application Phases
# ==============================================================================

async def run_discovery_phase(shutdown_event: Optional[asyncio.Event] = None) -> None:
    """Phase 1: Discover video targets and enqueue to database."""
    if shutdown_event and shutdown_event.is_set():
        return

    target_url = config.CRAWL_TARGET_URL
    if not target_url:
        logger.info("No CRAWL_TARGET_URL specified in environment. Skipping crawl.")
        return

    crawl_mode = config.CRAWL_MODE
    max_pages = config.MAX_PAGES

    logger.info("--- STARTING PHASE 1: DISCOVERY ---")
    logger.info(f"Target URL: {target_url} | Mode: {crawl_mode} | Max Pages: {max_pages}")

    crawler = UniversalCrawler()
    try:
        discovered_items = await crawler.discover(target_url, mode=crawl_mode, max_pages=max_pages)
        if shutdown_event and shutdown_event.is_set():
            logger.info("Discovery interrupted by shutdown.")
            return

        if discovered_items:
            inserted, ignored = db_manager.enqueue_batch(discovered_items)
            logger.info(f"Discovery Summary: {len(discovered_items)} found, {inserted} enqueued, {ignored} duplicates skipped.")
        else:
            logger.warning("Discovery completed with 0 video links found.")
    except Exception as e:
        logger.error(f"Error during discovery phase: {e}", exc_info=True)


async def run_worker_phase(shutdown_event: asyncio.Event) -> None:
    """Phase 2: Sequential Worker (Download 1 -> Upload 1 -> Delete 1 -> Cooldown -> Loop)."""
    logger.info("--- STARTING PHASE 2: SEQUENTIAL WORKER PIPELINE ---")
    
    empty_queue_logged = False
    last_periodic_crawl = time.time()
    crawl_interval = config.PERIODIC_CRAWL_INTERVAL  # 0 to disable recurring crawl

    while RUNNING and not shutdown_event.is_set():
        # Check if periodic re-crawl is enabled
        if crawl_interval > 0 and (time.time() - last_periodic_crawl) > crawl_interval:
            logger.info("Triggering scheduled periodic crawl...")
            await run_discovery_phase(shutdown_event)
            last_periodic_crawl = time.time()
            if shutdown_event.is_set():
                break

        # Atomically fetch next pending item
        item = db_manager.get_next_pending()
        if not item:
            if not empty_queue_logged:
                stats = db_manager.get_stats()
                logger.info(f"Queue idle. Status snapshot: {stats}")
                logger.info("Waiting for new tasks...")
                empty_queue_logged = True
            
            # Responsive sleep: wake up immediately on shutdown instead of blocking
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=10.0)
                break
            except asyncio.TimeoutError:
                continue

        empty_queue_logged = False
        video_id = item["id"]
        video_url = item["video_url"]
        title = item.get("title") or f"Video {video_id}"

        logger.info("\n========================================================")
        logger.info(f"Processing Task #{video_id}: {title}")
        logger.info(f"URL: {video_url}")
        logger.info("========================================================")

        file_path = None
        try:
            # 1. Download video
            file_path, metadata, error = await downloader.download_video(video_id, video_url)
            if not file_path or error:
                logger.error(f"Download failed for #{video_id}: {error}")
                db_manager.set_status(video_id, "FAILED", error_message=f"Download Error: {error}")
                continue

            file_size = metadata.get("file_size", 0)
            logger.info(f"File downloaded successfully: {file_path} ({file_size / (1024*1024):.2f} MB)")

            if shutdown_event.is_set():
                logger.warning(f"Shutdown requested during processing of #{video_id}. Resetting task to PENDING.")
                db_manager.set_status(video_id, "PENDING", error_message="Reset on service shutdown")
                break

            # 2. Update status to UPLOADING
            db_manager.set_status(video_id, "UPLOADING", file_size=file_size)

            # 3. Upload to Telegram
            upload_success, upload_error = await uploader.upload_video(
                file_path=file_path,
                title=title,
                metadata=metadata
            )

            if upload_success:
                logger.info(f"Task #{video_id} uploaded to Telegram successfully!")
                db_manager.set_status(video_id, "COMPLETED")
            else:
                logger.error(f"Upload failed for #{video_id}: {upload_error}")
                db_manager.set_status(video_id, "FAILED", error_message=f"Upload Error: {upload_error}")

        except Exception as e:
            logger.error(f"Unexpected error processing #{video_id}: {e}", exc_info=True)
            db_manager.set_status(video_id, "FAILED", error_message=f"Pipeline Exception: {str(e)}")

        finally:
            # 4. Strictly guaranteed cleanup in all conditions
            if file_path:
                downloader.cleanup_file(file_path)
            downloader.cleanup_video_files(video_id)
            logger.info(f"Disk cleaned up for Task #{video_id}. Ephemeral storage clear.")

    logger.info("Worker loop stopped cleanly.")


def display_stats() -> None:
    """Print current queue statistics to console."""
    stats = db_manager.get_stats()
    print("\n--- Current Queue Statistics ---")
    for key, value in stats.items():
        print(f"  {key:15}: {value}")
    print("--------------------------------\n")


# ==============================================================================
# Main Entry Point
# ==============================================================================

async def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk Video Scraper & Telegram Uploader")
    parser.add_argument("--crawl-only", action="store_true", help="Run discovery crawl only and exit")
    parser.add_argument("--worker-only", action="store_true", help="Run worker processing queue only")
    parser.add_argument("--stats", action="store_true", help="Display queue stats and exit")
    parser.add_argument("--reset-queue", action="store_true", help="Reset stalled tasks and exit")
    parser.add_argument("--retry-failed", type=int, nargs="?", const=3, default=None, help="Reset failed tasks with retry_count < threshold back to PENDING (default threshold: 3)")
    parser.add_argument("--failed-summary", action="store_true", help="Display summary of failed tasks and error reasons")
    parser.add_argument("--purge-completed", type=int, nargs="?", const=7, default=None, help="Purge completed tasks older than N days (default: 7, 0 for all)")
    parser.add_argument("--clear-queue", type=str, nargs="?", const="ALL", default=None, help="Clear queue items (optional status filter e.g. FAILED, COMPLETED, or ALL)")
    parser.add_argument("--no-health-server", action="store_true", help="Disable background HTTP health check server")
    args = parser.parse_args()

    if args.stats:
        display_stats()
        return

    if args.failed_summary:
        summary = db_manager.get_failed_summary()
        print("\n--- Failed Tasks Summary ---")
        if not summary:
            print("  No failed tasks found in database.")
        else:
            for item in summary:
                print(f"  Reason      : {item['error_reason']}")
                print(f"  Count       : {item['count']}")
                print(f"  Min Retries : {item['min_retries']} | Max Retries: {item['max_retries']}")
                print(f"  First Failed: {item['first_failed_at']} | Last Failed: {item['last_failed_at']}")
                print("  " + "-" * 40)
        print("----------------------------\n")
        return

    if args.retry_failed is not None:
        count = db_manager.retry_failed_tasks(max_retries=args.retry_failed)
        print(f"Reset {count} failed task(s) (retry_count < {args.retry_failed}) back to PENDING.")
        display_stats()
        return

    if args.purge_completed is not None:
        count = db_manager.purge_completed(days=args.purge_completed)
        print(f"Purged {count} completed task(s) older than {args.purge_completed} day(s).")
        display_stats()
        return

    if args.clear_queue is not None:
        count = db_manager.clear_queue(status=args.clear_queue)
        print(f"Cleared {count} task(s) from queue (filter: {args.clear_queue}).")
        display_stats()
        return

    logger.info("=== Initializing Video Scraper & Telegram Uploader Service ===")
    config.ensure_directories()
    config.log_summary(logger)

    # Initialize DB & reset stalled tasks from past container crashes
    db_manager.init_db()
    db_manager.reset_stalled_tasks()

    # Clean any leftover temp files
    downloader.purge_all_temp()

    if args.reset_queue:
        logger.info("Queue reset completed.")
        display_stats()
        return

    # Setup async shutdown event & signal handlers
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()
    setup_signal_handlers(shutdown_event, loop)

    # Start health check HTTP server concurrently
    health_runner = None
    if not args.no_health_server:
        health_runner = await start_health_server(config.HOST, config.PORT)

    try:
        # Verify Telegram Bot connectivity
        bot_ok, bot_info = await uploader.verify_bot_token()
        if bot_ok:
            logger.info(f"Telegram Bot connected: @{bot_info}")
        else:
            logger.warning(f"Telegram Bot check note: {bot_info}")
            logger.warning("Please ensure TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are set in Railway variables.")

        if args.crawl_only:
            await run_discovery_phase(shutdown_event)
            display_stats()
            return

        if args.worker_only:
            await run_worker_phase(shutdown_event)
            return

        # Default mode: Discovery -> Continuous Worker
        if not shutdown_event.is_set():
            await run_discovery_phase(shutdown_event)
        if not shutdown_event.is_set():
            await run_worker_phase(shutdown_event)

    finally:
        if health_runner:
            logger.info("Shutting down health check HTTP server...")
            try:
                await health_runner.cleanup()
            except Exception as e:
                logger.warning(f"Error during health check server cleanup: {e}")
        logger.info("Service shutdown completed cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Service process terminated.")
