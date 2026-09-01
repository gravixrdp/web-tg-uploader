#!/usr/bin/env python3
"""
Command Line Interface (CLI) for Operational Management.
Provides operational commands for inspecting queue stats, enqueuing URLs,
crawling web targets, resetting stalled/failed tasks, and listing queue items.

Usage:
  python cli.py stats
  python cli.py enqueue <url> [--title <title>]
  python cli.py retry-failed [--id <id>]
  python cli.py crawl <url> [--mode <mode>] [--max-pages <N>]
  python cli.py reset-stalled
  python cli.py list-pending [--limit 10]
  python cli.py list-failed [--limit 10]
  python cli.py list-all [--status <status>] [--limit 20]
  python cli.py view <id>
  python cli.py delete <id>
"""

import os
import sys
import argparse
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

# Load local .env
load_dotenv()

# Configure CLI logger
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

from modules.database import db_manager
from modules.crawler import UniversalCrawler

# ANSI Color formatting codes
COLOR_RESET = "\033[0m"
COLOR_BOLD = "\033[1m"
COLOR_CYAN = "\033[36m"
COLOR_GREEN = "\033[32m"
COLOR_YELLOW = "\033[33m"
COLOR_RED = "\033[31m"
COLOR_MAGENTA = "\033[35m"
COLOR_BLUE = "\033[34m"
COLOR_GRAY = "\033[90m"


def supports_color() -> bool:
    """Check if the current terminal supports ANSI color output."""
    if os.getenv("NO_COLOR"):
        return False
    if not sys.stdout.isatty():
        return False
    return True


def colored(text: str, color_code: str) -> str:
    """Wrap text in ANSI color if supported."""
    if supports_color():
        return f"{color_code}{text}{COLOR_RESET}"
    return text


def format_bytes(bytes_num: int) -> str:
    """Format bytes count into human-readable unit (KB, MB, GB)."""
    if not bytes_num or bytes_num < 0:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_num < 1024.0:
            return f"{bytes_num:.2f} {unit}"
        bytes_num /= 1024.0
    return f"{bytes_num:.2f} PB"


def truncate_text(text: Optional[str], max_length: int = 40) -> str:
    """Truncate text cleanly with ellipsis if it exceeds max_length."""
    if not text:
        return "-"
    text = text.replace("\r", " ").replace("\n", " ").strip()
    if len(text) > max_length:
        return text[:max_length - 3] + "..."
    return text


def render_table(headers: List[str], rows: List[List[str]]) -> None:
    """Render a clean, aligned ASCII table."""
    if not rows:
        print(colored("  (No records found)", COLOR_GRAY))
        return

    # Calculate column widths
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(str(cell)))

    # Header separator and line format
    header_line = " | ".join(headers[i].ljust(col_widths[i]) for i in range(len(headers)))
    sep_line = "-+-".join("-" * col_widths[i] for i in range(len(headers)))

    print(colored(header_line, COLOR_BOLD))
    print(colored(sep_line, COLOR_GRAY))

    for row in rows:
        cells = []
        for i, val in enumerate(row):
            if i < len(col_widths):
                cells.append(str(val).ljust(col_widths[i]))
        print(" | ".join(cells))


def get_status_badge(status: str) -> str:
    """Get color-coded status badge."""
    status = status.upper()
    if status == "COMPLETED":
        return colored("COMPLETED", COLOR_GREEN)
    elif status == "PENDING":
        return colored("PENDING", COLOR_YELLOW)
    elif status == "DOWNLOADING":
        return colored("DOWNLOADING", COLOR_CYAN)
    elif status == "UPLOADING":
        return colored("UPLOADING", COLOR_MAGENTA)
    elif status == "FAILED":
        return colored("FAILED", COLOR_RED)
    return status


# ============================================================================
# COMMAND HANDLERS
# ============================================================================

def cmd_stats(args: argparse.Namespace) -> None:
    """Show comprehensive queue statistics and storage metrics."""
    stats = db_manager.get_detailed_stats()

    total = stats.get("TOTAL", 0)
    pending = stats.get("PENDING", 0)
    downloading = stats.get("DOWNLOADING", 0)
    uploading = stats.get("UPLOADING", 0)
    completed = stats.get("COMPLETED", 0)
    failed = stats.get("FAILED", 0)
    in_progress = downloading + uploading

    db_path = stats.get("db_path", "data/queue.db")
    db_size = format_bytes(stats.get("db_size_bytes", 0))
    total_completed_bytes = format_bytes(stats.get("total_completed_bytes", 0))
    avg_completed_bytes = format_bytes(int(stats.get("avg_completed_bytes", 0)))

    # Calculate completion rate
    completion_rate = (completed / total * 100.0) if total > 0 else 0.0

    print("\n" + colored("=" * 56, COLOR_BOLD))
    print(colored("           SCRAPER & QUEUE DASHBOARD STATS", COLOR_BOLD))
    print(colored("=" * 56, COLOR_BOLD))

    print(f"\n{colored('Queue Breakdown:', COLOR_BOLD)}")
    print(f"  * {colored('PENDING', COLOR_YELLOW):<15} : {pending:>6}")
    print(f"  * {colored('DOWNLOADING', COLOR_CYAN):<15} : {downloading:>6}")
    print(f"  * {colored('UPLOADING', COLOR_MAGENTA):<15} : {uploading:>6}")
    print(f"  * {colored('COMPLETED', COLOR_GREEN):<15} : {completed:>6}")
    print(f"  * {colored('FAILED', COLOR_RED):<15} : {failed:>6}")
    print(colored("  " + "-" * 32, COLOR_GRAY))
    print(f"  * {colored('TOTAL ITEMS', COLOR_BOLD):<15} : {total:>6}")

    print(f"\n{colored('Operational Metrics:', COLOR_BOLD)}")
    print(f"  * Active In-Progress : {in_progress}")
    print(f"  * Completion Rate    : {completion_rate:.1f}%")
    print(f"  * Total Transferred  : {total_completed_bytes}")
    print(f"  * Avg Completed Size : {avg_completed_bytes}")
    print(f"  * Database Location  : {db_path}")
    print(f"  * Database File Size : {db_size}")
    print(colored("=" * 56, COLOR_BOLD) + "\n")


def cmd_enqueue(args: argparse.Namespace) -> None:
    """Manually add a URL to the queue."""
    url = args.url.strip()
    if not url:
        print(colored("Error: URL cannot be empty.", COLOR_RED))
        sys.exit(1)

    title = args.title or "Untitled Video"
    is_new, item_id = db_manager.enqueue_one(url, title=title)

    if is_new:
        print(colored(f"Successfully enqueued new task #{item_id}:", COLOR_GREEN))
        print(f"  ID    : {item_id}")
        print(f"  Title : {title}")
        print(f"  URL   : {url}")
        print(f"  Status: {colored('PENDING', COLOR_YELLOW)}")
    else:
        existing = db_manager.get_task(item_id) if item_id else None
        status = existing.get("status", "UNKNOWN") if existing else "UNKNOWN"
        print(colored(f"URL already exists in queue (Task #{item_id}) with status '{status}'.", COLOR_YELLOW))
        print(f"  URL: {url}")


def cmd_retry_failed(args: argparse.Namespace) -> None:
    """Reset failed tasks back to PENDING."""
    task_id = getattr(args, "id", None)
    if task_id:
        task = db_manager.get_task(task_id)
        if not task:
            print(colored(f"Task #{task_id} not found.", COLOR_RED))
            sys.exit(1)
        if task["status"] != "FAILED":
            print(colored(f"Task #{task_id} is currently '{task['status']}', not 'FAILED'.", COLOR_YELLOW))
            return
        db_manager.retry_failed(task_id=task_id)
        print(colored(f"Reset failed Task #{task_id} to PENDING.", COLOR_GREEN))
    else:
        count = db_manager.retry_failed()
        if count > 0:
            print(colored(f"Successfully reset {count} failed task(s) to PENDING.", COLOR_GREEN))
        else:
            print(colored("No failed tasks found to retry.", COLOR_YELLOW))


async def _run_crawl_async(url: str, mode: str, max_pages: int) -> None:
    """Run crawler asynchronously and enqueue discovered links."""
    print(f"\n{colored('Initiating Crawler Discovery...', COLOR_BOLD)}")
    print(f"  Target URL : {url}")
    print(f"  Mode       : {mode}")
    print(f"  Max Pages  : {max_pages}")
    print(colored("-" * 50, COLOR_GRAY))

    crawler = UniversalCrawler()
    try:
        discovered = await crawler.discover(url, mode=mode, max_pages=max_pages)
        print(f"\n{colored('Discovery Complete!', COLOR_BOLD)}")
        print(f"  Discovered items: {len(discovered)}")

        if not discovered:
            print(colored("No video links discovered for this target.", COLOR_YELLOW))
            return

        inserted, ignored = db_manager.enqueue_batch(discovered)
        print(colored("-" * 50, COLOR_GRAY))
        print(f"  * {colored('Newly Enqueued', COLOR_GREEN)}: {inserted}")
        print(f"  * {colored('Duplicates Skipped', COLOR_YELLOW)}: {ignored}")
        print(colored("=" * 50, COLOR_BOLD))

    except Exception as e:
        print(colored(f"Crawl failed with error: {e}", COLOR_RED))
        sys.exit(1)


def cmd_crawl(args: argparse.Namespace) -> None:
    """Run crawler directly and enqueue discovered links."""
    url = args.url.strip()
    if not url:
        print(colored("Error: Target URL cannot be empty.", COLOR_RED))
        sys.exit(1)

    mode = (args.mode or "auto").strip().lower()
    max_pages = args.max_pages if args.max_pages and args.max_pages > 0 else 10

    asyncio.run(_run_crawl_async(url, mode, max_pages))


def cmd_reset_stalled(args: argparse.Namespace) -> None:
    """Reset stuck DOWNLOADING or UPLOADING tasks back to PENDING."""
    count = db_manager.reset_stalled_tasks()
    if count > 0:
        print(colored(f"Successfully reset {count} stalled task(s) back to PENDING.", COLOR_GREEN))
    else:
        print(colored("No stalled tasks found (no tasks stranded in DOWNLOADING or UPLOADING).", COLOR_YELLOW))


def cmd_list_pending(args: argparse.Namespace) -> None:
    """List pending tasks."""
    limit = args.limit if args.limit and args.limit > 0 else 10
    tasks = db_manager.list_tasks(status="PENDING", limit=limit)

    print(f"\n{colored(f'Pending Tasks (Limit: {limit})', COLOR_BOLD)}")
    headers = ["ID", "Title", "URL", "Retries", "Created At"]
    rows = []
    for t in tasks:
        rows.append([
            str(t["id"]),
            truncate_text(t["title"], 30),
            truncate_text(t["video_url"], 45),
            str(t["retry_count"]),
            str(t["created_at"])
        ])

    render_table(headers, rows)
    print()


def cmd_list_failed(args: argparse.Namespace) -> None:
    """List failed tasks."""
    limit = args.limit if args.limit and args.limit > 0 else 10
    tasks = db_manager.list_tasks(status="FAILED", limit=limit)

    print(f"\n{colored(f'Failed Tasks (Limit: {limit})', COLOR_BOLD)}")
    headers = ["ID", "Title", "Error Message", "Retries", "Updated At"]
    rows = []
    for t in tasks:
        rows.append([
            str(t["id"]),
            truncate_text(t["title"], 25),
            truncate_text(t["error_message"], 40),
            str(t["retry_count"]),
            str(t["updated_at"])
        ])

    render_table(headers, rows)
    print()


def cmd_list_all(args: argparse.Namespace) -> None:
    """List tasks with optional status filter."""
    limit = args.limit if args.limit and args.limit > 0 else 20
    status = args.status.upper() if args.status else None
    tasks = db_manager.list_tasks(status=status, limit=limit, order_desc=True)

    title_str = f"Queue Tasks (Status: {status or 'ALL'}, Limit: {limit})"
    print(f"\n{colored(title_str, COLOR_BOLD)}")
    headers = ["ID", "Status", "Title", "URL", "Size", "Retries", "Updated At"]
    rows = []
    for t in tasks:
        rows.append([
            str(t["id"]),
            get_status_badge(t["status"]),
            truncate_text(t["title"], 25),
            truncate_text(t["video_url"], 35),
            format_bytes(t["file_size"]),
            str(t["retry_count"]),
            str(t["updated_at"])
        ])

    render_table(headers, rows)
    print()


def cmd_view(args: argparse.Namespace) -> None:
    """View full details of a specific task."""
    task = db_manager.get_task(args.id)
    if not task:
        print(colored(f"Task #{args.id} not found.", COLOR_RED))
        sys.exit(1)

    print("\n" + colored("=" * 60, COLOR_BOLD))
    print(colored(f"              TASK #{task['id']} DETAILS", COLOR_BOLD))
    print(colored("=" * 60, COLOR_BOLD))
    print(f"  ID           : {task['id']}")
    print(f"  Status       : {get_status_badge(task['status'])}")
    print(f"  Title        : {task['title']}")
    print(f"  URL          : {task['video_url']}")
    print(f"  File Size    : {format_bytes(task['file_size'])}")
    print(f"  Retry Count  : {task['retry_count']}")
    print(f"  Error Message: {task['error_message'] or '-'}")
    print(f"  Created At   : {task['created_at']}")
    print(f"  Updated At   : {task['updated_at']}")
    print(colored("=" * 60, COLOR_BOLD) + "\n")


def cmd_delete(args: argparse.Namespace) -> None:
    """Delete a task from the queue."""
    deleted = db_manager.delete_task(args.id)
    if deleted:
        print(colored(f"Deleted task #{args.id} from queue.", COLOR_GREEN))
    else:
        print(colored(f"Task #{args.id} not found.", COLOR_RED))


# ============================================================================
# MAIN ARGUMENT PARSER SETUP
# ============================================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cli.py",
        description="Scraper & Queue Operational Management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py stats
  python cli.py enqueue "https://example.com/video.mp4" --title "Sample Video"
  python cli.py retry-failed
  python cli.py retry-failed --id 42
  python cli.py crawl "https://example.com/sitemap.xml" --mode sitemap
  python cli.py crawl "https://example.com/videos" --mode pagination --max-pages 5
  python cli.py reset-stalled
  python cli.py list-pending --limit 20
  python cli.py list-failed --limit 10
  python cli.py list-all --status COMPLETED
  python cli.py view 1
  python cli.py delete 1
"""
    )
    subparsers = parser.add_subparsers(dest="command", help="Operational command to execute")

    # 1. stats
    subparsers.add_parser("stats", help="Display queue status dashboard and statistics")

    # 2. enqueue
    p_enqueue = subparsers.add_parser("enqueue", help="Manually add a video URL to the queue")
    p_enqueue.add_argument("url", help="URL of the video or page to download")
    p_enqueue.add_argument("--title", help="Optional title / caption for the video")

    # 3. retry-failed
    p_retry = subparsers.add_parser("retry-failed", help="Reset failed tasks back to PENDING")
    p_retry.add_argument("--id", type=int, help="Optional specific task ID to retry")

    # 4. crawl
    p_crawl = subparsers.add_parser("crawl", help="Run discovery crawler directly and enqueue results")
    p_crawl.add_argument("url", help="Target URL (sitemap XML or web page)")
    p_crawl.add_argument("--mode", choices=["auto", "sitemap", "pagination", "html5"], default="auto",
                         help="Crawling mode (default: auto)")
    p_crawl.add_argument("--max-pages", type=int, default=10,
                         help="Max pages for pagination crawler (default: 10)")

    # 5. reset-stalled
    subparsers.add_parser("reset-stalled", help="Reset stuck DOWNLOADING/UPLOADING tasks back to PENDING")

    # 6. list-pending
    p_pending = subparsers.add_parser("list-pending", help="List pending tasks in the queue")
    p_pending.add_argument("--limit", type=int, default=10, help="Max number of items to show (default: 10)")

    # 7. list-failed
    p_failed = subparsers.add_parser("list-failed", help="List failed tasks with error messages")
    p_failed.add_argument("--limit", type=int, default=10, help="Max number of items to show (default: 10)")

    # Extended operational commands
    # 8. list-all
    p_list_all = subparsers.add_parser("list-all", help="List queue items with optional status filter")
    p_list_all.add_argument("--status", choices=["PENDING", "DOWNLOADING", "UPLOADING", "COMPLETED", "FAILED"],
                            help="Filter by status")
    p_list_all.add_argument("--limit", type=int, default=20, help="Max number of items to show (default: 20)")

    # 9. view
    p_view = subparsers.add_parser("view", help="View full metadata for a specific task ID")
    p_view.add_argument("id", type=int, help="Task ID to inspect")

    # 10. delete
    p_del = subparsers.add_parser("delete", help="Delete a task from the queue by ID")
    p_del.add_argument("id", type=int, help="Task ID to delete")

    return parser


def main() -> None:
    # Ensure database tables exist
    db_manager.init_db()

    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    commands = {
        "stats": cmd_stats,
        "enqueue": cmd_enqueue,
        "retry-failed": cmd_retry_failed,
        "crawl": cmd_crawl,
        "reset-stalled": cmd_reset_stalled,
        "list-pending": cmd_list_pending,
        "list-failed": cmd_list_failed,
        "list-all": cmd_list_all,
        "view": cmd_view,
        "delete": cmd_delete,
    }

    handler = commands.get(args.command)
    if handler:
        handler(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
