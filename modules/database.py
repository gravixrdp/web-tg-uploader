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
        # Enable Write-Ahead Logging for high concurrency and resilience
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
        """Initialize the queue table and indices."""
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

    def enqueue_batch(self, items: List[Dict[str, str]]) -> Tuple[int, int]:
        """
        Atomically enqueues a batch of video items with deduplication.
        Returns: (inserted_count, ignored_count)
        """
        if not items:
            return 0, 0

        inserted = 0
        ignored = 0

        with self.get_cursor() as cursor:
            for item in items:
                url = item.get("url") or item.get("video_url")
                if not url or not isinstance(url, str):
                    continue
                url = url.strip()
                if not url:
                    continue
                title = (item.get("title") or "Untitled Video").strip()

                cursor.execute("""
                    INSERT OR IGNORE INTO queue (video_url, title, status, created_at, updated_at)
                    VALUES (?, ?, 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
                """, (url, title))

                if cursor.rowcount > 0:
                    inserted += 1
                else:
                    ignored += 1

        logger.info(f"Enqueued batch: {inserted} added, {ignored} duplicate/ignored.")
        return inserted, ignored

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

    def enqueue_one(self, url: str, title: Optional[str] = None) -> Tuple[bool, Optional[int]]:
        """
        Enqueues a single URL.
        Returns: (True, new_id) if inserted, or (False, existing_id) if already exists.
        """
        if not url or not isinstance(url, str):
            return False, None
        url = url.strip()
        if not url:
            return False, None
        title = (title or "Untitled Video").strip()
        with self.get_cursor() as cursor:
            cursor.execute("SELECT id FROM queue WHERE video_url = ?;", (url,))
            existing = cursor.fetchone()
            if existing:
                return False, existing["id"]

            cursor.execute("""
                INSERT INTO queue (video_url, title, status, created_at, updated_at)
                VALUES (?, ?, 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
            """, (url, title))
            new_id = cursor.lastrowid
            return True, new_id

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

    def delete_task(self, task_id: int) -> bool:
        """Deletes a task by ID."""
        with self.get_cursor() as cursor:
            cursor.execute("DELETE FROM queue WHERE id = ?;", (int(task_id),))
            return cursor.rowcount > 0

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

