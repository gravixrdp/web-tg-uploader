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

if __name__ == '__main__':
    unittest.main()
