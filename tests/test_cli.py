import unittest
import os
import sys
import shutil
import tempfile
from unittest.mock import patch, AsyncMock
from io import StringIO

from modules.database import DatabaseManager
import cli


class TestCLI(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "cli_test_queue.db")
        self.db = DatabaseManager(self.db_path)
        # Patch the global db_manager in cli module to use our test db
        self.patcher = patch("cli.db_manager", self.db)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir, ignore_errors=True)

    def run_cli_args(self, arg_list):
        """Helper to run CLI commands and capture stdout."""
        parser = cli.build_parser()
        args = parser.parse_args(arg_list)

        commands = {
            "stats": cli.cmd_stats,
            "enqueue": cli.cmd_enqueue,
            "retry-failed": cli.cmd_retry_failed,
            "crawl": cli.cmd_crawl,
            "reset-stalled": cli.cmd_reset_stalled,
            "list-pending": cli.cmd_list_pending,
            "list-failed": cli.cmd_list_failed,
            "list-all": cli.cmd_list_all,
            "view": cli.cmd_view,
            "delete": cli.cmd_delete,
        }

        captured_stdout = StringIO()
        with patch("sys.stdout", captured_stdout):
            handler = commands.get(args.command)
            self.assertIsNotNone(handler, f"Unknown command {args.command}")
            handler(args)

        return captured_stdout.getvalue()

    def test_stats_empty_and_populated(self):
        # Empty stats
        output = self.run_cli_args(["stats"])
        self.assertIn("SCRAPER & QUEUE DASHBOARD STATS", output)
        self.assertIn("TOTAL ITEMS", output)
        self.assertIn("0", output)

        # Populate db
        self.db.enqueue_one("https://example.com/v1.mp4", "Video 1")
        self.db.enqueue_one("https://example.com/v2.mp4", "Video 2")
        t1 = self.db.get_next_pending()
        self.db.set_status(t1["id"], "COMPLETED", file_size=1048576)

        output2 = self.run_cli_args(["stats"])
        self.assertIn("COMPLETED", output2)
        self.assertIn("1.00 MB", output2)

    def test_enqueue_command(self):
        # Enqueue new video
        output = self.run_cli_args(["enqueue", "https://example.com/movie.mp4", "--title", "Sample Movie"])
        self.assertIn("Successfully enqueued new task #1", output)
        self.assertIn("Sample Movie", output)

        # Enqueue duplicate
        output_dup = self.run_cli_args(["enqueue", "https://example.com/movie.mp4", "--title", "Duplicate"])
        self.assertIn("already exists in queue", output_dup)

    def test_list_pending_and_list_all(self):
        self.db.enqueue_one("https://example.com/p1.mp4", "Pending 1")
        self.db.enqueue_one("https://example.com/p2.mp4", "Pending 2")

        output = self.run_cli_args(["list-pending", "--limit", "5"])
        self.assertIn("Pending 1", output)
        self.assertIn("Pending 2", output)

        output_all = self.run_cli_args(["list-all"])
        self.assertIn("Pending 1", output_all)
        self.assertIn("Pending 2", output_all)

    def test_list_failed_and_retry_failed(self):
        self.db.enqueue_one("https://example.com/f1.mp4", "Fail 1")
        self.db.enqueue_one("https://example.com/f2.mp4", "Fail 2")

        t1 = self.db.get_next_pending()
        t2 = self.db.get_next_pending()

        self.db.set_status(t1["id"], "FAILED", error_message="HTTP 404")
        self.db.set_status(t2["id"], "FAILED", error_message="Network Timeout")

        # List failed
        output_failed = self.run_cli_args(["list-failed", "--limit", "10"])
        self.assertIn("HTTP 404", output_failed)
        self.assertIn("Network Timeout", output_failed)

        # Retry single failed task by id
        output_retry_id = self.run_cli_args(["retry-failed", "--id", str(t1["id"])])
        self.assertIn(f"Reset failed Task #{t1['id']} to PENDING", output_retry_id)

        # Retry remaining failed tasks
        output_retry_all = self.run_cli_args(["retry-failed"])
        self.assertIn("Successfully reset 1 failed task(s) to PENDING", output_retry_all)

        # Verify no failed tasks remaining
        output_empty_failed = self.run_cli_args(["list-failed"])
        self.assertIn("No records found", output_empty_failed)

    def test_reset_stalled(self):
        self.db.enqueue_one("https://example.com/s1.mp4", "Stalled 1")
        self.db.enqueue_one("https://example.com/s2.mp4", "Stalled 2")

        t1 = self.db.get_next_pending()  # transitions to DOWNLOADING
        t2 = self.db.get_next_pending()
        self.db.set_status(t2["id"], "UPLOADING")

        output = self.run_cli_args(["reset-stalled"])
        self.assertIn("Successfully reset 2 stalled task(s) back to PENDING", output)

        stats = self.db.get_stats()
        self.assertEqual(stats["PENDING"], 2)
        self.assertEqual(stats["DOWNLOADING"], 0)
        self.assertEqual(stats["UPLOADING"], 0)

    def test_view_and_delete(self):
        self.db.enqueue_one("https://example.com/v.mp4", "View Test")
        task = self.db.get_next_pending()

        # View task
        output_view = self.run_cli_args(["view", str(task["id"])])
        self.assertIn(f"TASK #{task['id']} DETAILS", output_view)
        self.assertIn("View Test", output_view)

        # Delete task
        output_del = self.run_cli_args(["delete", str(task["id"])])
        self.assertIn(f"Deleted task #{task['id']} from queue", output_del)
        self.assertIsNone(self.db.get_task(task["id"]))

    @patch("cli.UniversalCrawler.discover", new_callable=AsyncMock)
    def test_crawl_command(self, mock_discover):
        mock_discover.return_value = [
            {"url": "https://example.com/crawled1.mp4", "title": "Crawled 1"},
            {"url": "https://example.com/crawled2.mp4", "title": "Crawled 2"},
        ]

        output = self.run_cli_args(["crawl", "https://example.com/sitemap.xml", "--mode", "sitemap", "--max-pages", "2"])
        self.assertIn("Discovery Complete!", output)
        self.assertIn("Newly Enqueued", output)

        stats = self.db.get_stats()
        self.assertEqual(stats["PENDING"], 2)


if __name__ == "__main__":
    unittest.main()
