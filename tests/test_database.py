import unittest
import os
import shutil
import tempfile
import threading
from modules.database import DatabaseManager

class TestDatabaseManager(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, 'test_queue.db')
        self.db = DatabaseManager(self.db_path)

    def tearDown(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_pragmas_and_init(self):
        with self.db.get_cursor() as cursor:
            cursor.execute('PRAGMA busy_timeout;')
            timeout = cursor.fetchone()[0]
            self.assertEqual(timeout, 5000)

            cursor.execute('PRAGMA journal_mode;')
            journal_mode = cursor.fetchone()[0]
            self.assertEqual(journal_mode.lower(), 'wal')

    def test_enqueue_and_get_next_pending(self):
        items = [
            {'url': 'https://example.com/v1.mp4', 'title': 'Video 1'},
            {'url': 'https://example.com/v2.mp4', 'title': 'Video 2'},
            {'url': 'https://example.com/v1.mp4', 'title': 'Duplicate'}
        ]
        inserted, ignored = self.db.enqueue_batch(items)
        self.assertEqual(inserted, 2)
        self.assertEqual(ignored, 1)

        item1 = self.db.get_next_pending()
        self.assertIsNotNone(item1)
        self.assertEqual(item1['video_url'], 'https://example.com/v1.mp4')
        self.assertEqual(item1['title'], 'Video 1')

        task1 = self.db.get_task(item1['id'])
        self.assertEqual(task1['status'], 'DOWNLOADING')

    def test_retry_failed_tasks(self):
        self.db.enqueue_batch([
            {'url': 'https://example.com/fail1.mp4', 'title': 'Fail 1'},
            {'url': 'https://example.com/fail2.mp4', 'title': 'Fail 2'},
            {'url': 'https://example.com/fail3.mp4', 'title': 'Fail 3'},
        ])
        t1 = self.db.get_next_pending()
        t2 = self.db.get_next_pending()
        t3 = self.db.get_next_pending()

        self.db.set_status(t1['id'], 'FAILED', error_message='HTTP 404')
        self.db.set_status(t2['id'], 'FAILED', error_message='HTTP 500')
        self.db.set_status(t2['id'], 'FAILED', error_message='HTTP 500')
        self.db.set_status(t3['id'], 'FAILED', error_message='Network Error')
        self.db.set_status(t3['id'], 'FAILED', error_message='Network Error')
        self.db.set_status(t3['id'], 'FAILED', error_message='Network Error')

        reset_count = self.db.retry_failed_tasks(max_retries=3)
        self.assertEqual(reset_count, 2)

        t1_updated = self.db.get_task(t1['id'])
        self.assertEqual(t1_updated['status'], 'PENDING')
        self.assertEqual(t1_updated['retry_count'], 1)

        t2_updated = self.db.get_task(t2['id'])
        self.assertEqual(t2_updated['status'], 'PENDING')
        self.assertEqual(t2_updated['retry_count'], 2)

        t3_updated = self.db.get_task(t3['id'])
        self.assertEqual(t3_updated['status'], 'FAILED')
        self.assertEqual(t3_updated['retry_count'], 3)

    def test_purge_completed_and_clear_queue(self):
        self.db.enqueue_batch([
            {'url': 'https://example.com/c1.mp4', 'title': 'Completed 1'},
            {'url': 'https://example.com/c2.mp4', 'title': 'Completed 2'},
            {'url': 'https://example.com/p1.mp4', 'title': 'Pending 1'},
        ])
        t1 = self.db.get_next_pending()
        self.db.set_status(t1['id'], 'COMPLETED', file_size=1024)

        t2 = self.db.get_next_pending()
        self.db.set_status(t2['id'], 'COMPLETED', file_size=2048)

        with self.db.get_cursor() as cursor:
            cursor.execute("UPDATE queue SET updated_at = datetime('now', '-10 days') WHERE id = ?;", (t1['id'],))

        purged = self.db.purge_completed(days=7)
        self.assertEqual(purged, 1)
        self.assertIsNone(self.db.get_task(t1['id']))
        self.assertIsNotNone(self.db.get_task(t2['id']))

        purged_all_completed = self.db.purge_completed(days=0)
        self.assertEqual(purged_all_completed, 1)
        self.assertIsNone(self.db.get_task(t2['id']))

        cleared = self.db.clear_queue()
        self.assertEqual(cleared, 1)
        stats = self.db.get_stats()
        self.assertEqual(stats['TOTAL'], 0)

    def test_get_failed_summary(self):
        self.db.enqueue_batch([
            {'url': 'https://example.com/f1.mp4', 'title': 'Fail 1'},
            {'url': 'https://example.com/f2.mp4', 'title': 'Fail 2'},
            {'url': 'https://example.com/f3.mp4', 'title': 'Fail 3'},
        ])
        t1 = self.db.get_next_pending()
        t2 = self.db.get_next_pending()
        t3 = self.db.get_next_pending()

        self.db.set_status(t1['id'], 'FAILED', error_message='HTTP 404 Not Found')
        self.db.set_status(t2['id'], 'FAILED', error_message='HTTP 404 Not Found')
        self.db.set_status(t3['id'], 'FAILED', error_message='Timeout')

        summary = self.db.get_failed_summary()
        self.assertEqual(len(summary), 2)
        self.assertEqual(summary[0]['error_reason'], 'HTTP 404 Not Found')
        self.assertEqual(summary[0]['count'], 2)

        failed_tasks = self.db.get_failed_tasks(limit=10)
        self.assertEqual(len(failed_tasks), 3)

    def test_multithreading_safety(self):
        items = [{'url': 'https://example.com/v_' + str(i) + '.mp4', 'title': 'Video ' + str(i)} for i in range(100)]
        self.db.enqueue_batch(items)

        def worker():
            for _ in range(25):
                item = self.db.get_next_pending()
                if item:
                    self.db.set_status(item['id'], 'COMPLETED', file_size=500)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        stats = self.db.get_stats()
        self.assertEqual(stats['COMPLETED'], 100)
        self.assertEqual(stats['PENDING'], 0)

    def test_settings_table_and_helpers(self):
        # Test default return when key does not exist
        self.assertIsNone(self.db.get_setting("non_existent_key"))
        self.assertEqual(self.db.get_setting("non_existent_key", default="fallback"), "fallback")

        # Test set_setting and get_setting
        self.db.set_setting("site_title", "My Video Service")
        self.assertEqual(self.db.get_setting("site_title"), "My Video Service")

        # Test updating existing setting (upsert)
        self.db.set_setting("site_title", "Updated Title")
        self.assertEqual(self.db.get_setting("site_title"), "Updated Title")

        # Test get_all_settings
        self.db.set_setting("theme", "dark")
        self.db.set_setting("batch_size", "20")
        all_settings = self.db.get_all_settings()
        self.assertEqual(all_settings["site_title"], "Updated Title")
        self.assertEqual(all_settings["theme"], "dark")
        self.assertEqual(all_settings["batch_size"], "20")

    def test_worker_paused_methods(self):
        # Default should be False
        self.assertFalse(self.db.is_worker_paused())

        # Set paused to True
        self.db.set_worker_paused(True)
        self.assertTrue(self.db.is_worker_paused())
        self.assertEqual(self.db.get_setting("worker_paused"), "true")

        # Set paused to False
        self.db.set_worker_paused(False)
        self.assertFalse(self.db.is_worker_paused())
        self.assertEqual(self.db.get_setting("worker_paused"), "false")

    def test_get_tasks_filtering_and_pagination(self):
        # Seed tasks
        items = [
            {'url': 'https://youtube.com/watch?v=alpha', 'title': 'Alpha Tutorial'},
            {'url': 'https://vimeo.com/beta', 'title': 'Beta Guide'},
            {'url': 'https://youtube.com/watch?v=gamma', 'title': 'Gamma Demo'},
            {'url': 'https://dailymotion.com/delta', 'title': 'Delta Walkthrough'},
        ]
        self.db.enqueue_batch(items)

        # Initially all 4 are PENDING
        tasks, total = self.db.get_tasks()
        self.assertEqual(total, 4)
        self.assertEqual(len(tasks), 4)

        # Mark 1 as COMPLETED, 1 as FAILED
        t1 = self.db.get_next_pending()
        self.db.set_status(t1['id'], 'COMPLETED', file_size=1024)
        t2 = self.db.get_next_pending()
        self.db.set_status(t2['id'], 'FAILED', error_message='Download failed: 404')

        # Filter by status
        pending_tasks, pending_total = self.db.get_tasks(status='PENDING')
        self.assertEqual(pending_total, 2)
        self.assertEqual(len(pending_tasks), 2)

        failed_tasks, failed_total = self.db.get_tasks(status='FAILED')
        self.assertEqual(failed_total, 1)
        self.assertEqual(len(failed_tasks), 1)
        self.assertEqual(failed_tasks[0]['id'], t2['id'])

        # Search filter
        search_tasks, search_total = self.db.get_tasks(search='youtube')
        self.assertEqual(search_total, 2)
        self.assertEqual(len(search_tasks), 2)

        # Combined status + search filter
        combined_tasks, combined_total = self.db.get_tasks(status='COMPLETED', search='alpha')
        self.assertEqual(combined_total, 1)
        self.assertEqual(len(combined_tasks), 1)

        # Pagination limit and offset
        paged_tasks, paged_total = self.db.get_tasks(limit=2, offset=0)
        self.assertEqual(paged_total, 4)
        self.assertEqual(len(paged_tasks), 2)

        paged_tasks_2, paged_total_2 = self.db.get_tasks(limit=2, offset=2)
        self.assertEqual(paged_total_2, 4)
        self.assertEqual(len(paged_tasks_2), 2)
        # Ensure disjoint pages
        page1_ids = {t['id'] for t in paged_tasks}
        page2_ids = {t['id'] for t in paged_tasks_2}
        self.assertTrue(page1_ids.isdisjoint(page2_ids))

    def test_retry_task_and_delete_task(self):
        self.db.enqueue_batch([
            {'url': 'https://example.com/test_retry.mp4', 'title': 'Retry Test Video'}
        ])
        task = self.db.get_next_pending()
        self.db.set_status(task['id'], 'FAILED', error_message='Temporary error')

        # Verify task is FAILED
        t_failed = self.db.get_task(task['id'])
        self.assertEqual(t_failed['status'], 'FAILED')
        self.assertEqual(t_failed['error_message'], 'Temporary error')

        # Retry single task
        success = self.db.retry_task(task['id'])
        self.assertTrue(success)

        t_retried = self.db.get_task(task['id'])
        self.assertEqual(t_retried['status'], 'PENDING')
        self.assertIsNone(t_retried['error_message'])

        # Retry non-existent task
        self.assertFalse(self.db.retry_task(99999))

        # Delete task
        deleted = self.db.delete_task(task['id'])
        self.assertTrue(deleted)
        self.assertIsNone(self.db.get_task(task['id']))

        # Delete non-existent task
        self.assertFalse(self.db.delete_task(99999))

    def test_retry_all_failed(self):
        self.db.enqueue_batch([
            {'url': 'https://example.com/f1.mp4', 'title': 'Fail 1'},
            {'url': 'https://example.com/f2.mp4', 'title': 'Fail 2'},
            {'url': 'https://example.com/p1.mp4', 'title': 'Pending 1'},
        ])
        t1 = self.db.get_next_pending()
        t2 = self.db.get_next_pending()
        self.db.set_status(t1['id'], 'FAILED', error_message='Error 1')
        self.db.set_status(t2['id'], 'FAILED', error_message='Error 2')

        retried_count = self.db.retry_all_failed()
        self.assertEqual(retried_count, 2)

        # All should now be PENDING
        stats = self.db.get_stats()
        self.assertEqual(stats['FAILED'], 0)
        self.assertEqual(stats['PENDING'], 3)

    def test_dynamic_settings_helpers(self):
        # 1. delete_setting
        self.db.set_setting("temp_key", "temp_value")
        self.assertEqual(self.db.get_setting("temp_key"), "temp_value")
        self.assertTrue(self.db.delete_setting("temp_key"))
        self.assertIsNone(self.db.get_setting("temp_key"))
        self.assertFalse(self.db.delete_setting("non_existent"))

        # 2. Chat ID setting
        self.db.set_active_chat_id("-1009988776655")
        self.assertEqual(self.db.get_active_chat_id(), "-1009988776655")

        # 3. Crawl target setting
        self.db.set_active_crawl_target("https://example.com/sitemap_new.xml", mode="sitemap")
        url, mode = self.db.get_active_crawl_target()
        self.assertEqual(url, "https://example.com/sitemap_new.xml")
        self.assertEqual(mode, "sitemap")

        # 4. Cooldown setting
        self.db.set_active_cooldown(45)
        self.assertEqual(self.db.get_active_cooldown(), 45)

        # 5. Max pages setting
        self.db.set_active_max_pages(25)
        self.assertEqual(self.db.get_active_max_pages(), 25)

        # 6. Periodic crawl interval setting
        self.db.set_active_periodic_crawl_interval(3600)
        self.assertEqual(self.db.get_active_periodic_crawl_interval(), 3600)

        # 7. Effective settings dictionary
        effective = self.db.get_effective_settings()
        self.assertEqual(effective["chat_id"]["value"], "-1009988776655")
        self.assertEqual(effective["chat_id"]["source"], "db")
        self.assertEqual(effective["cooldown"]["value"], 45)
        self.assertEqual(effective["max_pages"]["value"], 25)
        self.assertEqual(effective["periodic_crawl_interval"]["value"], 3600)


if __name__ == '__main__':
    unittest.main()
