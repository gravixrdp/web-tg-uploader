"""
Database Module for Bulk Video Scraper & Telegram Uploader.
Implements a resilient SQLite queue with WAL mode, thread-safe access,
atomic state transitions, automatic deduplication, and crash recovery.
"""

import os
import sqlite3
import logging
import threading
from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager
from datetime import datetime

from modules.config import config

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = config.DB_PATH


# Recognized extensions for strict media segregation
IMAGE_EXTENSIONS = (
    '.jpg', '.jpeg', '.png', '.webp', '.gif', '.svg', '.ico',
    '.bmp', '.tiff', '.avif', '.heic'
)

VIDEO_EXTENSIONS = (
    '.mp4', '.mkv', '.webm', '.mov', '.m3u8', '.ts', '.avi',
    '.flv', '.wmv', '.m4v', '.3gp', '.mpd', '.f4v', '.vob', '.ogv'
)

TRACKING_PARAMS = {
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'fbclid', 'gclid', 'msclkid', 'mc_cid', 'mc_eid', 'ref', 'source',
    '_ga', '_gl', 'yclid', 'zanpid'
}


def normalize_media_url(url: str) -> str:
    """Canonicalizes a URL for 100% airtight deduplication."""
    if not url:
        return ""
    from urllib.parse import urlparse, parse_qsl, urlencode
    url = url.strip().strip("'\"").replace("&amp;", "&")
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return url

        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()

        query_pairs = parse_qsl(parsed.query, keep_blank_values=False)
        cleaned_pairs = [(k, v) for k, v in query_pairs if k.lower() not in TRACKING_PARAMS]
        clean_query = urlencode(cleaned_pairs)

        clean_path = parsed.path.rstrip('/') if parsed.path != '/' else '/'

        rebuilt = f"{scheme}://{netloc}{clean_path}"
        if clean_query:
            rebuilt = f"{rebuilt}?{clean_query}"
        return rebuilt
    except Exception:
        return url.strip()


def normalize_title(title: str) -> str:
    """Normalize video title for deduplication (strip duration, stats, punctuation)."""
    if not title:
        return ""
    import re
    # Remove duration hints like [15 min], (12 min), etc.
    t = re.sub(r'\[\s*\d+\s*(?:min|sec|m|s)\s*\]', '', title, flags=re.IGNORECASE)
    t = re.sub(r'\(\s*\d+\s*(?:min|sec|m|s)\s*\)', '', t, flags=re.IGNORECASE)
    # Remove stats prefixes like 12 min8.8K67%
    t = re.sub(r'^\s*\d+\s*(?:min|sec)\s*[\d.]+[KkMm]?\s*\d+%', '', t)
    # Keep only alphanumeric characters and single spaces
    t = re.sub(r'[^a-zA-Z0-9\s]', ' ', t)
    return re.sub(r'\s+', ' ', t).strip().lower()


def is_image_url(url: str) -> bool:
    """Check if URL points to a static image file."""
    from urllib.parse import urlparse
    path = urlparse(url.lower()).path.rstrip('/')
    return path.endswith(IMAGE_EXTENSIONS)


class DatabaseManager:
    """Manages SQLite queue with WAL mode, thread-safety, and resilient concurrency."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        self._lock = threading.RLock()
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self.init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """Create a new SQLite connection with optimized PRAGMAs and busy timeout."""
        conn = sqlite3.connect(self.db_path, timeout=10.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        return conn

    @contextmanager
    def get_cursor(self):
        """Thread-safe context manager for safe database transactions."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Database transaction error: {e}", exc_info=True)
                raise
            finally:
                cursor.close()
                conn.close()

    def init_db(self) -> None:
        """Initialize the queue table, settings table, and indices."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_url TEXT UNIQUE NOT NULL,
                    title TEXT,
                    status TEXT CHECK(status IN ('PENDING', 'DOWNLOADING', 'UPLOADING', 'COMPLETED', 'FAILED')) DEFAULT 'PENDING',
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    file_size INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_queue_status ON queue (status);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_queue_url ON queue (video_url);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_queue_updated ON queue (updated_at);")
        logger.info(f"Database initialized at: {self.db_path}")

    def reset_stalled_tasks(self) -> int:
        """
        On service startup/restart, resets any tasks stranded in DOWNLOADING or UPLOADING
        back to PENDING so work resumes cleanly without orphaned tasks.
        """
        with self.get_cursor() as cursor:
            cursor.execute("""
                UPDATE queue
                SET status = 'PENDING',
                    error_message = 'Reset after process restart',
                    updated_at = CURRENT_TIMESTAMP
                WHERE status IN ('DOWNLOADING', 'UPLOADING');
            """)
            reset_count = cursor.rowcount
            if reset_count > 0:
                logger.warning(f"Reset {reset_count} stalled tasks back to 'PENDING'.")
            return reset_count

    def enqueue_one(self, url: str, title: Optional[str] = None, media_type: str = "video") -> Tuple[bool, Optional[int]]:
        """
        Enqueues a single item with normalization, strict video validation, and deduplication.
        Returns: (is_new: bool, task_id: Optional[int])
        """
        if not url or not isinstance(url, str) or not url.strip():
            return False, None

        canonical_url = normalize_media_url(url)
        media_type = (media_type or "video").lower()

        # Reject static images by default unless media_type == 'image' or 'all'
        if media_type == "video" and is_image_url(canonical_url):
            logger.warning(f"Rejected image URL in video scraping mode: {canonical_url}")
            return False, None

        item_title = (title or "").strip() or canonical_url.split("/")[-1] or "Untitled Media"

        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT OR IGNORE INTO queue (video_url, title, status, created_at, updated_at)
                VALUES (?, ?, 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
            """, (canonical_url, item_title))

            if cursor.rowcount > 0:
                return True, cursor.lastrowid
            else:
                cursor.execute("SELECT id FROM queue WHERE video_url = ?;", (canonical_url,))
                row = cursor.fetchone()
                task_id = row["id"] if row else None
                return False, task_id

    def get_task(self, video_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves a single task by its integer primary key ID."""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT * FROM queue WHERE id = ?;", (int(video_id),))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_task_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Retrieves a task by its canonical URL."""
        canonical_url = normalize_media_url(url)
        with self.get_cursor() as cursor:
            cursor.execute("SELECT * FROM queue WHERE video_url = ?;", (canonical_url,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def enqueue_batch(self, items: List[Dict[str, str]], media_type: str = "video") -> Tuple[int, int]:
        """
        Atomically enqueues a batch of items with normalization, strict video validation,
        and database deduplication.
        Returns: (inserted_count, ignored_count)
        """
        if not items:
            return 0, 0

        inserted = 0
        ignored = 0
        media_type = (media_type or "video").lower()

        with self.get_cursor() as cursor:
            for item in items:
                raw_url = item.get("url") or item.get("video_url")
                if not raw_url or not isinstance(raw_url, str):
                    continue

                canonical_url = normalize_media_url(raw_url)
                if not canonical_url:
                    continue

                # Strict media type filtering: discard static images in video mode
                if media_type == "video" and is_image_url(canonical_url):
                    ignored += 1
                    continue

                title = (item.get("title") or "Untitled Media").strip()

                # Deduplication: check if already in queue by URL or Title
                cursor.execute("""
                    SELECT id FROM queue
                    WHERE video_url = ?
                       OR (title = ? AND title != 'Untitled Media');
                """, (canonical_url, title))
                if cursor.fetchone():
                    ignored += 1
                    continue

                cursor.execute("""
                    INSERT OR IGNORE INTO queue (video_url, title, status, created_at, updated_at)
                    VALUES (?, ?, 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                """, (canonical_url, title))

                if cursor.rowcount > 0:
                    inserted += 1
                else:
                    ignored += 1

        logger.info(f"Enqueued batch ({media_type} mode): {inserted} added, {ignored} duplicate/image/ignored.")
        return inserted, ignored

    def is_duplicate_completed(self, video_id: int, title: str, video_url: str) -> bool:
        """Checks if another task with identical title or canonical URL was already COMPLETED."""
        clean_url = normalize_media_url(video_url)
        clean_t = normalize_title(title)
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT id FROM queue
                WHERE id != ? AND status = 'COMPLETED' AND video_url = ?;
            """, (int(video_id), clean_url))
            if cursor.fetchone():
                return True

            if clean_t and clean_t not in ("untitled media", "video", ""):
                cursor.execute("""
                    SELECT id, title FROM queue
                    WHERE id != ? AND status = 'COMPLETED';
                """, (int(video_id),))
                for row in cursor.fetchall():
                    if normalize_title(row["title"]) == clean_t:
                        return True
        return False

    def get_next_pending(self) -> Optional[Dict[str, Any]]:
        """
        Atomically fetches the next PENDING item and marks it as DOWNLOADING.
        Guarantees that parallel processes or threads won't double-process the same item.
        """
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT id, video_url, title, status, retry_count
                FROM queue
                WHERE status = 'PENDING'
                ORDER BY id ASC
                LIMIT 1;
            """)
            row = cursor.fetchone()
            if not row:
                return None

            video_id = row["id"]
            cursor.execute("""
                UPDATE queue
                SET status = 'DOWNLOADING',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?;
            """, (video_id,))

            return dict(row)

    def set_status(
        self,
        video_id: int,
        status: str,
        error_message: Optional[str] = None,
        file_size: Optional[int] = None
    ) -> bool:
        """Update status and optional error/metadata for a queue item."""
        valid_statuses = ('PENDING', 'DOWNLOADING', 'UPLOADING', 'COMPLETED', 'FAILED')
        if status not in valid_statuses:
            raise ValueError(f"Invalid status '{status}'. Must be one of {valid_statuses}")

        with self.get_cursor() as cursor:
            if status == 'FAILED':
                cursor.execute("""
                    UPDATE queue
                    SET status = ?,
                        error_message = ?,
                        retry_count = retry_count + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?;
                """, (status, error_message or "Unknown failure", video_id))
            elif file_size is not None:
                cursor.execute("""
                    UPDATE queue
                    SET status = ?,
                        error_message = ?,
                        file_size = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?;
                """, (status, error_message, file_size, video_id))
            else:
                cursor.execute("""
                    UPDATE queue
                    SET status = ?,
                        error_message = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?;
                """, (status, error_message, video_id))

            return cursor.rowcount > 0

    def retry_failed_tasks(self, max_retries: int = 3) -> int:
        """
        Resets failed items whose retry count is strictly under max_retries back to PENDING.
        Returns the number of tasks reset.
        """
        with self.get_cursor() as cursor:
            cursor.execute("""
                UPDATE queue
                SET status = 'PENDING',
                    error_message = 'Reset for retry (retry_count: ' || retry_count || ')',
                    updated_at = CURRENT_TIMESTAMP
                WHERE status = 'FAILED' AND retry_count < ?;
            """, (int(max_retries),))
            count = cursor.rowcount
            if count > 0:
                logger.info(f"Reset {count} failed tasks (retry_count < {max_retries}) back to 'PENDING'.")
            else:
                logger.info(f"No failed tasks eligible for retry (max_retries={max_retries}).")
            return count

    def purge_completed(self, days: int = 7) -> int:
        """
        Purges completed tasks older than the specified number of days.
        If days <= 0, purges all completed tasks.
        Returns the number of deleted records.
        """
        with self.get_cursor() as cursor:
            if days <= 0:
                cursor.execute("""
                    DELETE FROM queue
                    WHERE status = 'COMPLETED';
                """)
            else:
                cursor.execute("""
                    DELETE FROM queue
                    WHERE status = 'COMPLETED'
                      AND updated_at <= datetime('now', '-' || ? || ' days');
                """, (int(days),))
            deleted_count = cursor.rowcount
            logger.info(f"Purged {deleted_count} completed tasks older than {days} day(s).")
            return deleted_count

    def clear_queue(self, status: Optional[str] = None) -> int:
        """
        Utility to clear queue items.
        If status is specified (e.g. 'FAILED', 'PENDING', 'COMPLETED'), only items with that status are cleared.
        If status is None or 'ALL', all items in the queue are deleted.
        Returns the number of deleted records.
        """
        with self.get_cursor() as cursor:
            if status and status.upper() != 'ALL':
                st = status.upper()
                cursor.execute("DELETE FROM queue WHERE status = ?;", (st,))
            else:
                cursor.execute("DELETE FROM queue;")
            deleted_count = cursor.rowcount
            logger.info(f"Cleared {deleted_count} tasks from queue (filter status: {status or 'ALL'}).")
            return deleted_count

    def get_failed_summary(self) -> List[Dict[str, Any]]:
        """
        Returns an aggregated summary of failed tasks grouped by failure reason,
        including occurrence count, retry stats, and timestamps of first and last failure.
        """
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COALESCE(error_message, 'Unknown failure') AS error_reason,
                    COUNT(*) AS count,
                    MIN(retry_count) AS min_retries,
                    MAX(retry_count) AS max_retries,
                    MIN(updated_at) AS first_failed_at,
                    MAX(updated_at) AS last_failed_at
                FROM queue
                WHERE status = 'FAILED'
                GROUP BY COALESCE(error_message, 'Unknown failure')
                ORDER BY count DESC, last_failed_at DESC;
            """)
            return [dict(row) for row in cursor.fetchall()]

    def get_failed_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Returns detailed records of failed tasks."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT id, video_url, title, status, error_message, retry_count, file_size, created_at, updated_at
                FROM queue
                WHERE status = 'FAILED'
                ORDER BY updated_at DESC
                LIMIT ?;
            """, (int(limit),))
            return [dict(row) for row in cursor.fetchall()]

    def get_task_by_id(self, video_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a specific task by its ID."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT id, video_url, title, status, error_message, retry_count, file_size, created_at, updated_at
                FROM queue
                WHERE id = ?;
            """, (int(video_id),))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_task_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch a specific task by its URL."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT id, video_url, title, status, error_message, retry_count, file_size, created_at, updated_at
                FROM queue
                WHERE video_url = ?;
            """, (url.strip(),))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_stats(self) -> Dict[str, int]:
        """Returns statistics of all queue items grouped by status."""
        stats = {
            "PENDING": 0,
            "DOWNLOADING": 0,
            "UPLOADING": 0,
            "COMPLETED": 0,
            "FAILED": 0,
            "TOTAL": 0
        }
        with self.get_cursor() as cursor:
            cursor.execute("SELECT status, COUNT(*) as count FROM queue GROUP BY status;")
            for row in cursor.fetchall():
                st = row["status"]
                cnt = row["count"]
                if st in stats:
                    stats[st] = cnt
                stats["TOTAL"] += cnt
        return stats



    def retry_failed(self, task_id: Optional[int] = None) -> int:
        """
        Resets tasks with status 'FAILED' back to 'PENDING'.
        If task_id is specified, resets only that task.
        """
        with self.get_cursor() as cursor:
            if task_id is not None:
                cursor.execute("""
                    UPDATE queue
                    SET status = 'PENDING',
                        error_message = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE status = 'FAILED' AND id = ?;
                """, (task_id,))
            else:
                cursor.execute("""
                    UPDATE queue
                    SET status = 'PENDING',
                        error_message = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE status = 'FAILED';
                """)
            count = cursor.rowcount
            if count > 0:
                logger.info(f"Reset {count} failed task(s) to 'PENDING'.")
            return count

    def list_tasks(
        self,
        status: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        order_desc: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Lists tasks filtered optionally by status, ordered by id.
        """
        direction = "DESC" if order_desc else "ASC"
        with self.get_cursor() as cursor:
            if status:
                query = f"""
                    SELECT id, video_url, title, status, retry_count, error_message, file_size, created_at, updated_at
                    FROM queue
                    WHERE status = ?
                    ORDER BY id {direction}
                    LIMIT ? OFFSET ?;
                """
                cursor.execute(query, (status.upper(), int(limit), int(offset)))
            else:
                query = f"""
                    SELECT id, video_url, title, status, retry_count, error_message, file_size, created_at, updated_at
                    FROM queue
                    ORDER BY id {direction}
                    LIMIT ? OFFSET ?;
                """
                cursor.execute(query, (int(limit), int(offset)))
            return [dict(row) for row in cursor.fetchall()]

    def get_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single task by its ID."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT id, video_url, title, status, retry_count, error_message, file_size, created_at, updated_at
                FROM queue
                WHERE id = ?;
            """, (int(task_id),))
            row = cursor.fetchone()
            return dict(row) if row else None

    def delete_task(self, video_id: int) -> bool:
        """Deletes a task by ID."""
        with self.get_cursor() as cursor:
            cursor.execute("DELETE FROM queue WHERE id = ?;", (int(video_id),))
            return cursor.rowcount > 0

    def retry_task(self, video_id: int) -> bool:
        """
        Resets a single task by ID back to PENDING.
        Returns True if the task was found and reset, False otherwise.
        """
        with self.get_cursor() as cursor:
            cursor.execute("""
                UPDATE queue
                SET status = 'PENDING',
                    error_message = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?;
            """, (int(video_id),))
            count = cursor.rowcount
            if count > 0:
                logger.info(f"Task #{video_id} reset to 'PENDING' for retry.")
            return count > 0

    def retry_all_failed(self) -> int:
        """
        Resets all FAILED tasks back to PENDING.
        Returns the count of tasks reset.
        """
        with self.get_cursor() as cursor:
            cursor.execute("""
                UPDATE queue
                SET status = 'PENDING',
                    error_message = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE status = 'FAILED';
            """)
            count = cursor.rowcount
            if count > 0:
                logger.info(f"Reset {count} failed task(s) to 'PENDING'.")
            return count

    def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Retrieves a setting value by key. Returns default if key is not found.
        """
        with self.get_cursor() as cursor:
            cursor.execute("SELECT value FROM settings WHERE key = ?;", (key,))
            row = cursor.fetchone()
            if row is not None:
                return row["value"]
            return default

    def set_setting(self, key: str, value: str) -> None:
        """
        Inserts or updates a setting key-value pair.
        """
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO settings (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = CURRENT_TIMESTAMP;
            """, (key, str(value)))

    def delete_setting(self, key: str) -> bool:
        """
        Deletes a setting key-value pair from the database.
        Returns True if deleted, False if key was not found.
        """
        with self.get_cursor() as cursor:
            cursor.execute("DELETE FROM settings WHERE key = ?;", (key,))
            return cursor.rowcount > 0

    def get_all_settings(self) -> Dict[str, str]:
        """
        Returns all stored settings as a dictionary of key-value pairs.
        """
        with self.get_cursor() as cursor:
            cursor.execute("SELECT key, value FROM settings ORDER BY key ASC;")
            return {row["key"]: row["value"] for row in cursor.fetchall()}

    def is_worker_paused(self) -> bool:
        """
        Returns True if the worker is configured as paused in settings, False otherwise.
        """
        val = self.get_setting("worker_paused", default="false")
        return str(val).strip().lower() in ("true", "1", "yes", "paused", "on")

    def set_worker_paused(self, paused: bool) -> None:
        """
        Persists the worker pause state in the settings table.
        """
        self.set_setting("worker_paused", "true" if paused else "false")
        logger.info(f"Worker paused state set to: {paused}")

    def is_paused(self) -> bool:
        """Convenience alias for is_worker_paused."""
        return self.is_worker_paused()

    def set_paused(self, paused: bool) -> None:
        """Convenience alias for set_worker_paused."""
        self.set_worker_paused(paused)

    def get_active_chat_id(self) -> str:
        """Returns the database-configured chat ID if present, else fallback to config, then to primary admin ID."""
        val = self.get_setting("chat_id") or self.get_setting("telegram_chat_id")
        if val and val.strip():
            return val.strip()
        if config.TELEGRAM_CHAT_ID and config.TELEGRAM_CHAT_ID.strip():
            return config.TELEGRAM_CHAT_ID.strip()
        if config.ADMIN_USER_IDS:
            return str(config.ADMIN_USER_IDS[0])
        return ""

    def set_active_chat_id(self, chat_id: str) -> None:
        """Persists updated target Telegram chat/channel ID in settings."""
        self.set_setting("chat_id", chat_id.strip())

    def get_active_crawl_target(self) -> Tuple[str, str]:
        """Returns the active (target_url, crawl_mode) checking database first then config."""
        url = self.get_setting("target_url") or self.get_setting("crawl_target_url") or config.CRAWL_TARGET_URL
        mode = self.get_setting("crawl_mode") or config.CRAWL_MODE
        return url, mode

    def set_active_crawl_target(self, url: str, mode: Optional[str] = None) -> None:
        """Persists updated target crawl URL and optional crawl mode in settings."""
        self.set_setting("crawl_target_url", url.strip())
        if mode:
            self.set_setting("crawl_mode", mode.strip().lower())

    def get_active_cooldown(self) -> int:
        """Returns active upload cooldown in seconds, checking DB first then config."""
        val = self.get_setting("upload_cooldown") or self.get_setting("cooldown")
        if val is not None:
            try:
                return max(0, int(val))
            except (ValueError, TypeError):
                pass
        return config.UPLOAD_COOLDOWN

    def set_active_cooldown(self, cooldown: int) -> None:
        """Persists upload cooldown setting in database."""
        self.set_setting("upload_cooldown", str(max(0, int(cooldown))))

    def get_active_max_pages(self) -> int:
        """Returns active max pages for crawler, checking DB first then config."""
        val = self.get_setting("max_pages")
        if val is not None:
            try:
                return max(1, int(val))
            except (ValueError, TypeError):
                pass
        return config.MAX_PAGES

    def set_active_max_pages(self, max_pages: int) -> None:
        """Persists max pages setting in database."""
        self.set_setting("max_pages", str(max(1, int(max_pages))))

    def get_active_periodic_crawl_interval(self) -> int:
        """Returns active periodic crawl interval in seconds, checking DB first then config."""
        val = self.get_setting("periodic_crawl_interval") or self.get_setting("crawl_interval")
        if val is not None:
            try:
                return max(0, int(val))
            except (ValueError, TypeError):
                pass
        return config.PERIODIC_CRAWL_INTERVAL

    def set_active_periodic_crawl_interval(self, interval: int) -> None:
        """Persists periodic crawl interval setting in database."""
        self.set_setting("periodic_crawl_interval", str(max(0, int(interval))))

    def get_active_growth_settings(self) -> Dict[str, str]:
        """Returns active viral channel growth settings (buttons and promotional links)."""
        button_url = self.get_setting("button_url") or config.CHANNEL_BUTTON_URL
        button_text = self.get_setting("button_text") or config.CHANNEL_BUTTON_TEXT
        share_text = self.get_setting("share_text") or config.CHANNEL_SHARE_TEXT
        footer_link = self.get_setting("footer_link") or config.CHANNEL_FOOTER_LINK
        return {
            "button_url": button_url.strip() if button_url else "",
            "button_text": button_text.strip() if button_text else "📢 Join Main Channel",
            "share_text": share_text.strip() if share_text else "↗️ Share With Friends",
            "footer_link": footer_link.strip() if footer_link else ""
        }

    def get_effective_settings(self) -> Dict[str, Any]:
        """
        Returns a dictionary of effective runtime settings with active values and source (db vs env).
        """
        db_settings = self.get_all_settings()
        target_url, crawl_mode = self.get_active_crawl_target()
        chat_id = self.get_active_chat_id()
        cooldown = self.get_active_cooldown()
        max_pages = self.get_active_max_pages()
        crawl_interval = self.get_active_periodic_crawl_interval()
        paused = self.is_worker_paused()

        return {
            "worker_paused": {
                "value": paused,
                "source": "db" if "worker_paused" in db_settings else "default"
            },
            "chat_id": {
                "value": chat_id,
                "source": "db" if ("chat_id" in db_settings or "telegram_chat_id" in db_settings) else "env"
            },
            "target_url": {
                "value": target_url,
                "source": "db" if ("target_url" in db_settings or "crawl_target_url" in db_settings) else "env"
            },
            "crawl_mode": {
                "value": crawl_mode,
                "source": "db" if "crawl_mode" in db_settings else "env"
            },
            "cooldown": {
                "value": cooldown,
                "source": "db" if ("upload_cooldown" in db_settings or "cooldown" in db_settings) else "env"
            },
            "max_pages": {
                "value": max_pages,
                "source": "db" if "max_pages" in db_settings else "env"
            },
            "periodic_crawl_interval": {
                "value": crawl_interval,
                "source": "db" if ("periodic_crawl_interval" in db_settings or "crawl_interval" in db_settings) else "env"
            },
            "raw_db_settings": db_settings
        }


    def get_tasks(
        self,
        status: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Retrieves a paginated list of tasks matching optional status and search query,
        along with the total count of matching tasks.

        Returns: (tasks_list, total_count)
        """
        conditions = []
        params: List[Any] = []

        if status and status.upper() != 'ALL':
            st_upper = status.upper()
            if st_upper in ('IN_PROGRESS', 'ACTIVE'):
                conditions.append("status IN ('DOWNLOADING', 'UPLOADING')")
            elif ',' in st_upper:
                statuses = [s.strip() for s in st_upper.split(',') if s.strip()]
                placeholders = ','.join('?' for _ in statuses)
                conditions.append(f"status IN ({placeholders})")
                params.extend(statuses)
            else:
                conditions.append("status = ?")
                params.append(st_upper)

        if search and search.strip():
            term = f"%{search.strip()}%"
            conditions.append("(title LIKE ? OR video_url LIKE ? OR error_message LIKE ? OR CAST(id AS TEXT) LIKE ?)")
            params.extend([term, term, term, term])

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        with self.get_cursor() as cursor:
            # Get total matching count
            count_query = f"SELECT COUNT(*) as total FROM queue {where_clause};"
            cursor.execute(count_query, params)
            count_row = cursor.fetchone()
            total_count = count_row["total"] if count_row else 0

            # Get paginated tasks
            data_query = f"""
                SELECT id, video_url, title, status, retry_count, error_message, file_size, created_at, updated_at
                FROM queue
                {where_clause}
                ORDER BY id DESC
                LIMIT ? OFFSET ?;
            """
            cursor.execute(data_query, params + [int(limit), int(offset)])
            tasks = [dict(row) for row in cursor.fetchall()]

            return tasks, total_count

    def list_tasks_paginated(
        self,
        status: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        order_desc: bool = True
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Retrieves paginated tasks with optional multi-status filter and text search,
        along with the total matching count.
        """
        conditions = []
        params: List[Any] = []

        if status and status.upper() != 'ALL':
            st_upper = status.upper()
            if st_upper == 'IN_PROGRESS' or st_upper == 'ACTIVE':
                conditions.append("status IN ('DOWNLOADING', 'UPLOADING')")
            elif ',' in st_upper:
                statuses = [s.strip() for s in st_upper.split(',') if s.strip()]
                placeholders = ','.join('?' for _ in statuses)
                conditions.append(f"status IN ({placeholders})")
                params.extend(statuses)
            else:
                conditions.append("status = ?")
                params.append(st_upper)

        if search and search.strip():
            term = f"%{search.strip()}%"
            conditions.append("(title LIKE ? OR video_url LIKE ? OR error_message LIKE ? OR CAST(id AS TEXT) LIKE ?)")
            params.extend([term, term, term, term])

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        direction = "DESC" if order_desc else "ASC"

        with self.get_cursor() as cursor:
            # Get total matching count
            count_query = f"SELECT COUNT(*) as total FROM queue {where_clause};"
            cursor.execute(count_query, params)
            count_row = cursor.fetchone()
            total_count = count_row["total"] if count_row else 0

            # Get paginated tasks
            data_query = f"""
                SELECT id, video_url, title, status, retry_count, error_message, file_size, created_at, updated_at
                FROM queue
                {where_clause}
                ORDER BY id {direction}
                LIMIT ? OFFSET ?;
            """
            cursor.execute(data_query, params + [int(limit), int(offset)])
            tasks = [dict(row) for row in cursor.fetchall()]

            return tasks, total_count


    def get_detailed_stats(self) -> Dict[str, Any]:
        """Provides comprehensive statistics including storage size and processed volume."""
        stats = self.get_stats()
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COALESCE(SUM(file_size), 0) as total_size, 
                    COALESCE(AVG(file_size), 0) as avg_size 
                FROM queue 
                WHERE status = 'COMPLETED';
            """)
            row = cursor.fetchone()
            total_completed_bytes = row["total_size"] if row else 0
            avg_completed_bytes = row["avg_size"] if row else 0

        db_size_bytes = 0
        if os.path.exists(self.db_path):
            db_size_bytes = os.path.getsize(self.db_path)

        return {
            **stats,
            "total_completed_bytes": total_completed_bytes,
            "avg_completed_bytes": avg_completed_bytes,
            "db_size_bytes": db_size_bytes,
            "db_path": self.db_path
        }


# Global convenience instance
db_manager = DatabaseManager()

