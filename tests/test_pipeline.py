"""
Comprehensive Unit and Integration Test Suite for Bulk Video Scraper & Telegram Uploader.

Covers:
1. DatabaseManager:
   - WAL mode and PRAGMA settings
   - Batch enqueuing with deduplication and edge cases
   - Atomic get_next_pending() status updates
   - Status transitions (PENDING -> DOWNLOADING -> UPLOADING -> COMPLETED / FAILED)
   - Incremental retry counts on failures
   - Resetting stalled tasks on process restart
   - Queue statistics reporting

2. Crawler & Extraction:
   - BaseCrawler video extension checks and fetch_text error handling
   - SitemapCrawler (standard sitemap XML and nested sitemap index XML)
   - PaginationCrawler (page=1..N crawling, deduplication, early termination)
   - HTML5Extractor (<video src>, <source src>, regex stream URLs, page titles)
   - UniversalCrawler routing (sitemap, pagination, html5, auto fallback)

3. VideoDownloader:
   - yt-dlp successful extraction and metadata generation
   - yt-dlp error handling and partial download cleanup
   - Single file cleanup (cleanup_file)
   - Multi-file prefix cleanup (cleanup_video_files)
   - Full temp directory purge (purge_all_temp)
   - Pre-flight disk space checking (check_disk_space)

4. TelegramBotUploader:
   - verify_bot_token (success, API rejection, network errors, empty token)
   - upload_video input validation (missing chat ID, non-existent file)
   - sendVideo endpoint upload with caption trimming and thumbnail
   - sendDocument fallback upon video format/stream failure
   - HTTP 429 FloodWait rate limit handling and retry
   - Max retries handling on recurring failures
   - Inter-upload cooldown timer
   - Advanced caption formatting and length limits

5. End-to-End Pipeline Integration:
   - Full happy path (Discovery -> Queue -> Download -> Upload -> Completed -> Cleanup)
   - Failure path (Download/Upload failure -> Status FAILED -> Retry count incremented -> Cleanup)
"""

import os
import sqlite3
import asyncio
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

import aiohttp

from modules.database import DatabaseManager
from modules.crawler import (
    BaseCrawler,
    SitemapCrawler,
    PaginationCrawler,
    HTML5Extractor,
    UniversalCrawler,
    VIDEO_EXTENSIONS,
)
from modules.downloader import VideoDownloader
from modules.uploader import TelegramBotUploader


# ==============================================================================
# Helper Mock Classes for Async aiohttp Responses
# ==============================================================================

class MockAsyncResponse:
    """Mock aiohttp ClientResponse supporting async context manager, json, and text."""

    def __init__(self, status: int = 200, json_data: dict = None, text_data: str = ""):
        self.status = status
        self._json_data = json_data if json_data is not None else {}
        self._text_data = text_data

    async def json(self):
        return self._json_data

    async def text(self, encoding: str = "utf-8"):
        return self._text_data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


# ==============================================================================
# 1. DATABASE MANAGER TESTS
# ==============================================================================

class TestDatabaseManager:
    """Unit tests for SQLite DatabaseManager."""

    @pytest.fixture
    def db(self, tmp_path):
        """Provides an isolated database instance in a temp directory."""
        db_file = tmp_path / "test_queue.db"
        return DatabaseManager(db_path=str(db_file))

    def test_wal_mode_and_pragmas(self, db):
        """Verify WAL journal mode and performance PRAGMAs are active."""
        conn = db._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode;")
            journal_mode = cursor.fetchone()[0]
            assert journal_mode.lower() == "wal"

            cursor.execute("PRAGMA foreign_keys;")
            foreign_keys = cursor.fetchone()[0]
            assert foreign_keys == 1
        finally:
            conn.close()

    def test_init_db_creates_table_and_indices(self, db):
        """Verify queue table and status/url indices exist."""
        conn = db._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='queue';")
            assert cursor.fetchone() is not None

            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_queue_status';")
            assert cursor.fetchone() is not None

            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_queue_url';")
            assert cursor.fetchone() is not None
        finally:
            conn.close()

    def test_enqueue_batch_empty_or_invalid(self, db):
        """Verify enqueuing empty list or items missing URLs returns (0, 0)."""
        assert db.enqueue_batch([]) == (0, 0)

        invalid_items = [{"title": "No URL item"}, {"url": ""}, {"video_url": "   "}]
        assert db.enqueue_batch(invalid_items) == (0, 0)

    def test_enqueue_batch_with_deduplication(self, db):
        """Verify batch insertion deduplicates items based on unique video_url."""
        items = [
            {"url": "https://example.com/v1.mp4", "title": "Video 1"},
            {"video_url": "https://example.com/v2.mp4", "title": "Video 2"},
            {"url": "https://example.com/v1.mp4", "title": "Duplicate Video 1"},
        ]
        inserted, ignored = db.enqueue_batch(items)
        assert inserted == 2
        assert ignored == 1

        # Enqueuing the same items again should ignore all duplicates
        inserted2, ignored2 = db.enqueue_batch([
            {"url": "https://example.com/v1.mp4", "title": "Video 1"},
            {"url": "https://example.com/v3.mp4", "title": "Video 3"},
        ])
        assert inserted2 == 1
        assert ignored2 == 1

    def test_get_next_pending_atomic_transition(self, db):
        """Verify get_next_pending returns the lowest ID and transitions status to DOWNLOADING."""
        # Empty queue
        assert db.get_next_pending() is None

        # Add two items
        db.enqueue_batch([
            {"url": "https://example.com/v1.mp4", "title": "First"},
            {"url": "https://example.com/v2.mp4", "title": "Second"},
        ])

        item1 = db.get_next_pending()
        assert item1 is not None
        assert item1["video_url"] == "https://example.com/v1.mp4"
        assert item1["title"] == "First"

        # Verify in DB that item1 is now DOWNLOADING
        conn = db._get_connection()
        try:
            row = conn.execute("SELECT status FROM queue WHERE id = ?", (item1["id"],)).fetchone()
            assert row["status"] == "DOWNLOADING"
        finally:
            conn.close()

        # Second call returns item2
        item2 = db.get_next_pending()
        assert item2 is not None
        assert item2["video_url"] == "https://example.com/v2.mp4"

        # Third call returns None as no more PENDING items exist
        assert db.get_next_pending() is None

    def test_status_transitions_and_retry_count(self, db):
        """Verify status changes, file_size update, and retry_count increment on FAILED."""
        db.enqueue_batch([{"url": "https://example.com/v1.mp4", "title": "Test"}])
        item = db.get_next_pending()
        video_id = item["id"]

        # Transition to UPLOADING with file_size
        success = db.set_status(video_id, "UPLOADING", file_size=1048576)
        assert success is True

        conn = db._get_connection()
        try:
            row = conn.execute("SELECT status, file_size FROM queue WHERE id = ?", (video_id,)).fetchone()
            assert row["status"] == "UPLOADING"
            assert row["file_size"] == 1048576
        finally:
            conn.close()

        # Transition to FAILED with error message
        db.set_status(video_id, "FAILED", error_message="Network timeout")
        conn = db._get_connection()
        try:
            row = conn.execute("SELECT status, error_message, retry_count FROM queue WHERE id = ?", (video_id,)).fetchone()
            assert row["status"] == "FAILED"
            assert row["error_message"] == "Network timeout"
            assert row["retry_count"] == 1
        finally:
            conn.close()

        # Second failure increments retry_count again
        db.set_status(video_id, "FAILED", error_message="Second error")
        conn = db._get_connection()
        try:
            row = conn.execute("SELECT retry_count FROM queue WHERE id = ?", (video_id,)).fetchone()
            assert row["retry_count"] == 2
        finally:
            conn.close()

        # Transition to COMPLETED
        db.set_status(video_id, "COMPLETED")
        conn = db._get_connection()
        try:
            row = conn.execute("SELECT status FROM queue WHERE id = ?", (video_id,)).fetchone()
            assert row["status"] == "COMPLETED"
        finally:
            conn.close()

    def test_set_status_invalid_raises_value_error(self, db):
        """Verify setting an unsupported status raises ValueError."""
        db.enqueue_batch([{"url": "https://example.com/v1.mp4", "title": "Test"}])
        with pytest.raises(ValueError, match="Invalid status"):
            db.set_status(1, "UNKNOWN_STATUS")

    def test_reset_stalled_tasks(self, db):
        """Verify stalled DOWNLOADING and UPLOADING tasks are reset to PENDING."""
        db.enqueue_batch([
            {"url": "https://example.com/v1.mp4", "title": "V1"},
            {"url": "https://example.com/v2.mp4", "title": "V2"},
            {"url": "https://example.com/v3.mp4", "title": "V3"},
            {"url": "https://example.com/v4.mp4", "title": "V4"},
            {"url": "https://example.com/v5.mp4", "title": "V5"},
        ])

        # Manually set various statuses
        with db.get_cursor() as cursor:
            cursor.execute("UPDATE queue SET status = 'DOWNLOADING' WHERE id = 1;")
            cursor.execute("UPDATE queue SET status = 'UPLOADING' WHERE id = 2;")
            cursor.execute("UPDATE queue SET status = 'COMPLETED' WHERE id = 3;")
            cursor.execute("UPDATE queue SET status = 'FAILED' WHERE id = 4;")
            # id = 5 remains PENDING

        reset_count = db.reset_stalled_tasks()
        assert reset_count == 2

        stats = db.get_stats()
        assert stats["PENDING"] == 3  # IDs 1, 2, 5
        assert stats["DOWNLOADING"] == 0
        assert stats["UPLOADING"] == 0
        assert stats["COMPLETED"] == 1
        assert stats["FAILED"] == 1
        assert stats["TOTAL"] == 5

    def test_get_stats_empty_and_populated(self, db):
        """Verify get_stats accurately tallies items by status."""
        stats = db.get_stats()
        assert stats == {
            "PENDING": 0,
            "DOWNLOADING": 0,
            "UPLOADING": 0,
            "COMPLETED": 0,
            "FAILED": 0,
            "TOTAL": 0,
        }

        db.enqueue_batch([
            {"url": "https://example.com/v1.mp4", "title": "V1"},
            {"url": "https://example.com/v2.mp4", "title": "V2"},
        ])
        stats = db.get_stats()
        assert stats["PENDING"] == 2
        assert stats["TOTAL"] == 2


# ==============================================================================
# 2. CRAWLER TESTS
# ==============================================================================

class TestCrawler:
    """Unit tests for BaseCrawler, SitemapCrawler, PaginationCrawler, HTML5Extractor, and UniversalCrawler."""

    def test_base_crawler_is_video_url(self):
        """Verify video URL detection across standard media extensions."""
        crawler = BaseCrawler(delay=0, jitter=0)
        for ext in ('.mp4', '.mkv', '.webm', '.mov', '.m3u8', '.ts', '.avi', '.flv'):
            assert crawler.is_video_url(f"https://example.com/path/video{ext}")
            assert crawler.is_video_url(f"https://example.com/path/video{ext.upper()}")
            assert crawler.is_video_url(f"https://example.com/video{ext}?token=abc&quality=1080")

        assert not crawler.is_video_url("https://example.com/page.html")
        assert not crawler.is_video_url("https://example.com/image.png")
        assert not crawler.is_video_url("https://example.com/audio.mp3")

    @pytest.mark.asyncio
    async def test_base_crawler_fetch_text(self):
        """Verify fetch_text handles 200, non-200 HTTP, and exceptions."""
        crawler = BaseCrawler(delay=0, jitter=0)

        # Success 200
        mock_session = MagicMock()
        mock_session.get.return_value = MockAsyncResponse(status=200, text_data="<html>Hello</html>")
        content = await crawler.fetch_text(mock_session, "https://example.com")
        assert content == "<html>Hello</html>"

        # HTTP 404
        mock_session.get.return_value = MockAsyncResponse(status=404, text_data="Not Found")
        content_404 = await crawler.fetch_text(mock_session, "https://example.com/404", max_retries=1)
        assert content_404 is None

        # Network Exception
        mock_session.get.side_effect = aiohttp.ClientError("Connection refused")
        content_err = await crawler.fetch_text(mock_session, "https://example.com/err", max_retries=1)
        assert content_err is None

    @pytest.mark.asyncio
    async def test_sitemap_crawler_standard_xml(self):
        """Verify parsing standard XML sitemaps with video:title and URL fallback titles."""
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
                xmlns:video="http://www.google.com/schemas/sitemap-video/1.1">
            <url>
                <loc>https://example.com/video-alpha.mp4</loc>
                <video:title>Alpha Video Title</video:title>
            </url>
            <url>
                <loc>https://example.com/folder/sample-beta_movie.mp4</loc>
            </url>
            <url>
                <!-- Missing loc tag -->
                <title>No Loc</title>
            </url>
        </urlset>
        """
        crawler = SitemapCrawler(delay=0, jitter=0)
        with patch.object(crawler, "fetch_text", AsyncMock(return_value=xml_content)):
            results = await crawler.crawl("https://example.com/sitemap.xml")

        assert len(results) == 2
        assert results[0]["url"] == "https://example.com/video-alpha.mp4"
        assert "Alpha Video Title" in results[0]["title"]
        assert results[1]["url"] == "https://example.com/folder/sample-beta_movie.mp4"
        assert "sample beta movie" in results[1]["title"]

    @pytest.mark.asyncio
    async def test_sitemap_crawler_nested_sitemap_index(self):
        """Verify recursive parsing of sitemap index files containing child sitemaps."""
        index_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <sitemap>
                <loc>https://example.com/child_sitemap_1.xml</loc>
            </sitemap>
            <sitemap>
                <loc>https://example.com/child_sitemap_2.xml</loc>
            </sitemap>
        </sitemapindex>
        """
        child_xml_1 = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset><url><loc>https://example.com/video1.mp4</loc><title>Vid 1</title></url></urlset>
        """
        child_xml_2 = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset><url><loc>https://example.com/video2.mp4</loc><title>Vid 2</title></url></urlset>
        """

        crawler = SitemapCrawler(delay=0, jitter=0)

        async def mock_fetch(session, url, **kwargs):
            if url == "https://example.com/sitemap_index.xml":
                return index_xml
            elif url == "https://example.com/child_sitemap_1.xml":
                return child_xml_1
            elif url == "https://example.com/child_sitemap_2.xml":
                return child_xml_2
            return None

        with patch.object(crawler, "fetch_text", side_effect=mock_fetch):
            results = await crawler.crawl("https://example.com/sitemap_index.xml")

        assert len(results) == 2
        assert results[0]["url"] == "https://example.com/video1.mp4"
        assert results[1]["url"] == "https://example.com/video2.mp4"

    @pytest.mark.asyncio
    async def test_pagination_crawler(self):
        """Verify page=1..N pagination crawling, link extraction, deduplication, and early break."""
        page1_html = """
        <html>
            <body>
                <a href="/media/video1.mp4">Video One</a>
                <a href="https://example.com/media/video2.mkv">Video Two</a>
                <a href="/page.html">Not a video</a>
            </body>
        </html>
        """
        page2_html = """
        <html>
            <body>
                <a href="/media/video1.mp4">Duplicate Video One</a>
                <a href="/media/video3.webm">Video Three</a>
            </body>
        </html>
        """
        page3_html = """
        <html>
            <body>
                <p>No video links on page 3</p>
            </body>
        </html>
        """

        crawler = PaginationCrawler(delay=0, jitter=0)

        async def mock_fetch(session, url, **kwargs):
            if "page=1" in url:
                return page1_html
            elif "page=2" in url:
                return page2_html
            elif "page=3" in url:
                return page3_html
            return None

        with patch.object(crawler, "fetch_text", side_effect=mock_fetch):
            results = await crawler.crawl("https://example.com/videos", max_pages=5)

        # Discovered 3 unique videos across pages 1 and 2, stopped early at page 3
        assert len(results) == 3
        urls = [r["url"] for r in results]
        assert "https://example.com/media/video1.mp4" in urls
        assert "https://example.com/media/video2.mkv" in urls
        assert "https://example.com/media/video3.webm" in urls

    @pytest.mark.asyncio
    async def test_html5_extractor(self):
        """Verify extraction from <video src>, <source src>, and embedded regex stream URLs."""
        html_content = """
        <html>
            <head><title>Awesome Media Page</title></head>
            <body>
                <video src="/videos/direct_video.mp4"></video>
                <video>
                    <source src="/videos/nested_source.webm" type="video/webm">
                    <source src="/audio/sound.mp3" type="audio/mp3">
                </video>
                <script>
                    var streamSource = "https://cdn.example.com/live/playlist.m3u8";
                    var fallbackSource = "https://cdn.example.com/stream.mp4";
                </script>
            </body>
        </html>
        """
        extractor = HTML5Extractor(delay=0, jitter=0)
        with patch.object(extractor, "fetch_text", AsyncMock(return_value=html_content)):
            results = await extractor.extract("https://example.com/player.html")

        assert len(results) == 4
        urls = [r["url"] for r in results]
        assert "https://example.com/videos/direct_video.mp4" in urls
        assert "https://example.com/videos/nested_source.webm" in urls
        assert "https://cdn.example.com/live/playlist.m3u8" in urls
        assert "https://cdn.example.com/stream.mp4" in urls

        for item in results:
            assert "Awesome Media Page" in item["title"]

    @pytest.mark.asyncio
    async def test_universal_crawler_routing(self):
        """Verify UniversalCrawler routes to appropriate crawler based on mode and auto-detection."""
        crawler = UniversalCrawler(delay=0, jitter=0)

        # 1. Sitemap mode explicit
        with patch.object(crawler.sitemap_crawler, "crawl", AsyncMock(return_value=[{"url": "sm"}])) as mock_sm:
            res = await crawler.discover("https://example.com/feed", mode="sitemap")
            assert res == [{"url": "sm"}]
            mock_sm.assert_called_once()

        # 2. Pagination mode explicit
        with patch.object(crawler.pagination_crawler, "crawl", AsyncMock(return_value=[{"url": "pg"}])) as mock_pg:
            res = await crawler.discover("https://example.com/list", mode="pagination", max_pages=7)
            assert res == [{"url": "pg"}]
            mock_pg.assert_called_once()

        # 3. HTML5 mode explicit
        with patch.object(crawler.html5_extractor, "extract", AsyncMock(return_value=[{"url": "h5"}])) as mock_h5:
            res = await crawler.discover("https://example.com/watch", mode="html5")
            assert res == [{"url": "h5"}]
            mock_h5.assert_called_once()

        # 4. Auto mode with .xml URL -> routes to sitemap
        with patch.object(crawler.sitemap_crawler, "crawl", AsyncMock(return_value=[{"url": "sm_auto"}])) as mock_sm:
            res = await crawler.discover("https://example.com/sitemap.xml", mode="auto")
            assert res == [{"url": "sm_auto"}]
            mock_sm.assert_called_once()

        # 5. Auto mode fallback (HTML5 first, then pagination if HTML5 empty)
        with patch.object(crawler.html5_extractor, "fetch_text", AsyncMock(return_value="<html><body></body></html>")):
            with patch.object(crawler.html5_extractor, "extract", AsyncMock(return_value=[])):
                with patch.object(crawler.pagination_crawler, "crawl", AsyncMock(return_value=[{"url": "fallback"}])) as mock_pg:
                    res = await crawler.discover("https://example.com/browse", mode="auto", max_pages=3)
                    assert res == [{"url": "fallback"}]
                    mock_pg.assert_called_once()


# ==============================================================================
# 3. DOWNLOADER TESTS
# ==============================================================================

class TestVideoDownloader:
    """Unit tests for VideoDownloader disk cleanup, disk space checks, and yt-dlp operations."""

    @pytest.fixture
    def dl_dir(self, tmp_path):
        """Temporary download directory."""
        dir_path = tmp_path / "temp_downloads"
        dir_path.mkdir(parents=True, exist_ok=True)
        return str(dir_path)

    @pytest.fixture
    def downloader_instance(self, dl_dir):
        """Provides a VideoDownloader instance configured with isolated directory."""
        return VideoDownloader(download_dir=dl_dir)

    def test_cleanup_file(self, tmp_path):
        """Verify cleanup_file deletes existing files and handles missing files/None safely."""
        test_file = tmp_path / "sample.mp4"
        test_file.write_text("dummy video data")
        assert test_file.exists()

        VideoDownloader.cleanup_file(str(test_file))
        assert not test_file.exists()

        # Safe with missing file and None
        VideoDownloader.cleanup_file(str(test_file))
        VideoDownloader.cleanup_file(None)

    def test_cleanup_video_files_scoped_by_id(self, downloader_instance, dl_dir):
        """Verify cleanup_video_files removes only files for the specified video ID."""
        # Create files for video 101
        v101_mp4 = os.path.join(dl_dir, "video_101.mp4")
        v101_jpg = os.path.join(dl_dir, "video_101.jpg")
        v101_part = os.path.join(dl_dir, "video_101.part")

        # Create files for video 102
        v102_mp4 = os.path.join(dl_dir, "video_102.mp4")

        for f in (v101_mp4, v101_jpg, v101_part, v102_mp4):
            with open(f, "wb") as fh:
                fh.write(b"data")

        downloader_instance.cleanup_video_files(101)

        # Video 101 files deleted
        assert not os.path.exists(v101_mp4)
        assert not os.path.exists(v101_jpg)
        assert not os.path.exists(v101_part)

        # Video 102 file preserved
        assert os.path.exists(v102_mp4)

    def test_purge_all_temp(self, downloader_instance, dl_dir):
        """Verify purge_all_temp resets the download directory completely."""
        f1 = os.path.join(dl_dir, "file1.txt")
        f2 = os.path.join(dl_dir, "file2.txt")
        for f in (f1, f2):
            with open(f, "w") as fh:
                fh.write("test")

        downloader_instance.purge_all_temp()
        assert os.path.exists(dl_dir)
        assert len(os.listdir(dl_dir)) == 0

    def test_check_disk_space(self, downloader_instance):
        """Verify check_disk_space reports disk space availability correctly."""
        has_space, free_bytes, err = downloader_instance.check_disk_space(required_bytes=1000)
        assert isinstance(has_space, bool)
        assert isinstance(free_bytes, int)

    @pytest.mark.asyncio
    async def test_download_video_success(self, downloader_instance, dl_dir):
        """Verify successful download creates expected metadata and returns file path."""
        video_id = 42
        url = "https://example.com/watch?v=42"

        dummy_mp4 = os.path.join(dl_dir, f"video_{video_id}.mp4")
        dummy_thumb = os.path.join(dl_dir, f"video_{video_id}.jpg")

        def fake_extract_info(url, download=True):
            with open(dummy_mp4, "wb") as fh:
                fh.write(b"fake video content of length 1000")
            with open(dummy_thumb, "wb") as fh:
                fh.write(b"fake thumb")
            return {
                "title": "Super Cool Video",
                "duration": 120,
                "width": 1920,
                "height": 1080,
            }

        mock_ydl = MagicMock()
        mock_ydl.__enter__.return_value = mock_ydl
        mock_ydl.extract_info.side_effect = fake_extract_info

        with patch("yt_dlp.YoutubeDL", return_value=mock_ydl):
            file_path, metadata, error = await downloader_instance.download_video(video_id, url)

        assert error is None
        assert file_path == dummy_mp4
        assert metadata["title"] == "Super Cool Video"
        assert metadata["duration"] == 120
        assert metadata["width"] == 1920
        assert metadata["height"] == 1080
        assert metadata["file_size"] > 0
        assert metadata["thumbnail"] == dummy_thumb

    @pytest.mark.asyncio
    async def test_download_video_failure_cleans_up(self, downloader_instance, dl_dir):
        """Verify download error triggers cleanup and returns error message."""
        video_id = 99
        url = "https://example.com/broken"

        # Create a partial leftover file
        partial_file = os.path.join(dl_dir, f"video_{video_id}.part")
        with open(partial_file, "w") as fh:
            fh.write("partial")

        mock_ydl = MagicMock()
        mock_ydl.__enter__.return_value = mock_ydl
        mock_ydl.extract_info.side_effect = Exception("HTTP 403 Forbidden")

        with patch("yt_dlp.YoutubeDL", return_value=mock_ydl):
            file_path, metadata, error = await downloader_instance.download_video(video_id, url)

        assert file_path is None
        assert metadata is None
        assert "HTTP 403 Forbidden" in error
        # Verify partial file was cleaned up
        assert not os.path.exists(partial_file)


# ==============================================================================
# 4. TELEGRAM BOT UPLOADER TESTS
# ==============================================================================

class TestTelegramBotUploader:
    """Unit tests for TelegramBotUploader direct Bot Token HTTP API client."""

    @pytest.fixture
    def uploader(self):
        return TelegramBotUploader(
            bot_token="123456789:ABCdefGHIjklMNOpqrSTUvwxYZ_123456",
            chat_id="-1001234567890",
            api_base="https://api.telegram.org",
            cooldown=0  # Cooldown 0 for fast tests
        )

    @pytest.mark.asyncio
    async def test_verify_bot_token_success(self, uploader):
        """Verify getMe success returns (True, username)."""
        response_data = {"ok": True, "result": {"id": 123456789, "username": "SuperMediaBot"}}

        mock_resp = MockAsyncResponse(status=200, json_data=response_data)
        with patch("aiohttp.ClientSession.get", return_value=mock_resp):
            ok, username = await uploader.verify_bot_token()

        assert ok is True
        assert username == "SuperMediaBot"

    @pytest.mark.asyncio
    async def test_verify_bot_token_failure_and_empty(self, uploader):
        """Verify getMe failure response and empty token handling."""
        # Empty token
        empty_uploader = TelegramBotUploader(bot_token="", chat_id="123")
        ok, msg = await empty_uploader.verify_bot_token()
        assert ok is False
        assert "TELEGRAM_BOT_TOKEN" in msg

        # API returns error
        response_data = {"ok": False, "error_code": 401, "description": "Unauthorized"}
        mock_resp = MockAsyncResponse(status=401, json_data=response_data)
        with patch("aiohttp.ClientSession.get", return_value=mock_resp):
            ok, err = await uploader.verify_bot_token()

        assert ok is False
        assert "Unauthorized" in err

        # Network Exception
        with patch("aiohttp.ClientSession.get", side_effect=aiohttp.ClientError("DNS lookup failed")):
            ok, err = await uploader.verify_bot_token()

        assert ok is False
        assert "Network error" in err

    @pytest.mark.asyncio
    async def test_upload_video_validations(self, uploader, tmp_path):
        """Verify validation errors for missing chat ID and missing file."""
        # Missing chat ID
        no_chat_uploader = TelegramBotUploader(bot_token="123:ABC", chat_id="")
        success, err = await no_chat_uploader.upload_video(file_path="foo.mp4")
        assert success is False
        assert "Target chat ID is missing" in err

        # Non-existent file
        success, err = await uploader.upload_video(file_path=str(tmp_path / "non_existent.mp4"))
        assert success is False
        assert "File does not exist" in err

    @pytest.mark.asyncio
    async def test_upload_video_sendvideo_success(self, uploader, tmp_path):
        """Verify successful upload via sendVideo with metadata and thumbnail."""
        test_video = tmp_path / "test.mp4"
        test_video.write_bytes(b"dummy video data")
        test_thumb = tmp_path / "test.jpg"
        test_thumb.write_bytes(b"dummy thumb data")

        metadata = {
            "title": "My Great Video",
            "duration": 90,
            "width": 1280,
            "height": 720,
            "thumbnail": str(test_thumb),
        }

        mock_resp = MockAsyncResponse(status=200, json_data={"ok": True, "result": {"message_id": 999}})
        with patch("aiohttp.ClientSession.post", return_value=mock_resp) as mock_post:
            success, err = await uploader.upload_video(
                file_path=str(test_video),
                title="A" * 1200,  # Long title to verify caption truncation
                metadata=metadata
            )

        assert success is True
        assert err is None
        assert mock_post.call_count == 1
        called_url = mock_post.call_args[0][0]
        assert called_url.endswith("/sendVideo")

    @pytest.mark.asyncio
    async def test_upload_video_senddocument_fallback(self, uploader, tmp_path):
        """Verify automatic fallback to sendDocument when sendVideo fails with format issue."""
        test_video = tmp_path / "test.mkv"
        test_video.write_bytes(b"dummy mkv data")

        resp_sendvideo_fail = MockAsyncResponse(
            status=400,
            json_data={"ok": False, "error_code": 400, "description": "Bad Request: can't use file of type video/x-matroska"}
        )
        resp_senddocument_ok = MockAsyncResponse(
            status=200,
            json_data={"ok": True, "result": {"message_id": 1001}}
        )

        with patch("aiohttp.ClientSession.post", side_effect=[resp_sendvideo_fail, resp_senddocument_ok]) as mock_post:
            success, err = await uploader.upload_video(file_path=str(test_video), title="MKV Video")

        assert success is True
        assert err is None
        assert mock_post.call_count == 2
        assert mock_post.call_args_list[0][0][0].endswith("/sendVideo")
        assert mock_post.call_args_list[1][0][0].endswith("/sendDocument")

    @pytest.mark.asyncio
    async def test_upload_video_429_floodwait_retry(self, uploader, tmp_path):
        """Verify HTTP 429 FloodWait handling sleeps and retries successfully."""
        test_video = tmp_path / "test.mp4"
        test_video.write_bytes(b"dummy video data")

        resp_429 = MockAsyncResponse(
            status=429,
            json_data={
                "ok": False,
                "error_code": 429,
                "description": "Too Many Requests: retry after 2",
                "parameters": {"retry_after": 2}
            }
        )
        resp_ok = MockAsyncResponse(
            status=200,
            json_data={"ok": True, "result": {"message_id": 1002}}
        )

        with patch("aiohttp.ClientSession.post", side_effect=[resp_429, resp_ok]) as mock_post:
            with patch("asyncio.sleep", AsyncMock()) as mock_sleep:
                success, err = await uploader.upload_video(file_path=str(test_video), title="Rate Limited Video")

        assert success is True
        assert err is None
        assert mock_post.call_count == 2
        assert mock_sleep.called
        slept_arg = mock_sleep.call_args[0][0]
        assert slept_arg >= 2

    @pytest.mark.asyncio
    async def test_upload_video_max_retries_exhausted(self, uploader, tmp_path):
        """Verify upload returns failure after exhausting max retries."""
        test_video = tmp_path / "test.mp4"
        test_video.write_bytes(b"dummy video data")

        with patch("aiohttp.ClientSession.post", side_effect=asyncio.TimeoutError("Socket timeout")):
            with patch("asyncio.sleep", AsyncMock()):
                success, err = await uploader.upload_video(file_path=str(test_video), max_retries=3)

        assert success is False
        assert err is not None

    @pytest.mark.asyncio
    async def test_wait_cooldown(self):
        """Verify _wait_cooldown enforces configured delay between consecutive uploads."""
        uploader = TelegramBotUploader(
            bot_token="123:ABC",
            chat_id="123",
            cooldown=10
        )
        uploader.last_upload_time = 100.0

        with patch("time.time", return_value=104.0):  # 4 seconds elapsed, 6 seconds remaining
            with patch("asyncio.sleep", AsyncMock()) as mock_sleep:
                await uploader._wait_cooldown()
                mock_sleep.assert_called_once()
                slept_time = mock_sleep.call_args[0][0]
                assert abs(slept_time - 6.0) < 0.1

    def test_format_caption_and_truncation(self, uploader):
        """Verify caption formatting supports variables and caps length at 1024."""
        metadata = {
            "title": "Action Movie",
            "duration": 3665,
            "width": 1920,
            "height": 1080,
            "file_size": 104857600,
        }
        caption = uploader.format_caption(
            file_path="movie.mp4",
            title="Action Movie",
            metadata=metadata,
            template="Title: {title} | Size: {size} | Duration: {duration} | Res: {resolution}"
        )
        assert "Action Movie" in caption
        assert "100.00 MB" in caption
        assert "01:01:05" in caption
        assert "1920x1080" in caption

        # Truncation
        long_caption = uploader.format_caption(
            file_path="movie.mp4",
            title="X" * 2000,
            max_length=1024
        )
        assert len(long_caption) <= 1024
        assert long_caption.endswith("...")


# ==============================================================================
# 5. END-TO-END PIPELINE INTEGRATION TESTS
# ==============================================================================

class TestPipelineIntegration:
    """Integration tests covering complete lifecycle from Discovery to Upload & Cleanup."""

    @pytest.mark.asyncio
    async def test_full_pipeline_happy_path(self, tmp_path):
        """
        End-to-End Simulation:
        1. Discover items and enqueue to DatabaseManager
        2. Worker fetches pending item (atomic transition to DOWNLOADING)
        3. VideoDownloader downloads media file (mocked yt-dlp)
        4. Status transitions to UPLOADING
        5. TelegramBotUploader streams video to Telegram (mocked aiohttp)
        6. Status transitions to COMPLETED
        7. File and temp artifacts are deleted from disk
        """
        db_file = str(tmp_path / "pipeline.db")
        dl_dir = str(tmp_path / "downloads")
        os.makedirs(dl_dir, exist_ok=True)

        db = DatabaseManager(db_path=db_file)
        downloader = VideoDownloader(download_dir=dl_dir)
        uploader = TelegramBotUploader(bot_token="123:ABC", chat_id="-100123", cooldown=0)

        # 1. Discovery & Enqueue
        discovered = [
            {"url": "https://example.com/movie1.mp4", "title": "Movie 1"},
            {"url": "https://example.com/movie2.mp4", "title": "Movie 2"},
        ]
        inserted, ignored = db.enqueue_batch(discovered)
        assert inserted == 2
        assert ignored == 0

        # Process first item
        # 2. Worker fetches item
        item = db.get_next_pending()
        assert item is not None
        video_id = item["id"]
        video_url = item["video_url"]

        # 3. Downloader (mock yt-dlp)
        dummy_file = os.path.join(dl_dir, f"video_{video_id}.mp4")
        with open(dummy_file, "wb") as f:
            f.write(b"video data content")

        mock_ydl = MagicMock()
        mock_ydl.__enter__.return_value = mock_ydl
        mock_ydl.extract_info.return_value = {
            "title": "Movie 1",
            "duration": 60,
            "width": 1920,
            "height": 1080,
        }

        with patch("yt_dlp.YoutubeDL", return_value=mock_ydl):
            file_path, metadata, dl_err = await downloader.download_video(video_id, video_url)

        assert file_path == dummy_file
        assert dl_err is None
        assert os.path.exists(file_path)

        # 4. Status update to UPLOADING
        db.set_status(video_id, "UPLOADING", file_size=metadata["file_size"])

        # 5. Uploader (mock aiohttp)
        mock_resp = MockAsyncResponse(status=200, json_data={"ok": True, "result": {"message_id": 42}})
        with patch("aiohttp.ClientSession.post", return_value=mock_resp):
            up_success, up_err = await uploader.upload_video(file_path=file_path, title=item["title"], metadata=metadata)

        assert up_success is True

        # 6. Status update to COMPLETED
        db.set_status(video_id, "COMPLETED")

        # 7. Guaranteed cleanup
        downloader.cleanup_file(file_path)
        downloader.cleanup_video_files(video_id)
        assert not os.path.exists(file_path)

        # Verify database stats
        stats = db.get_stats()
        assert stats["COMPLETED"] == 1
        assert stats["PENDING"] == 1
        assert stats["TOTAL"] == 2

    @pytest.mark.asyncio
    async def test_full_pipeline_failure_recovery(self, tmp_path):
        """
        End-to-End Simulation of Failures:
        - Item 1 fails during download -> marked FAILED with retry_count=1, partial files cleaned up
        - Item 2 fails during upload -> marked FAILED with retry_count=1, downloaded files cleaned up
        """
        db_file = str(tmp_path / "pipeline_fail.db")
        dl_dir = str(tmp_path / "downloads_fail")
        os.makedirs(dl_dir, exist_ok=True)

        db = DatabaseManager(db_path=db_file)
        downloader = VideoDownloader(download_dir=dl_dir)
        uploader = TelegramBotUploader(bot_token="123:ABC", chat_id="-100123", cooldown=0)

        db.enqueue_batch([
            {"url": "https://example.com/fail_dl.mp4", "title": "Download Fail"},
            {"url": "https://example.com/fail_up.mp4", "title": "Upload Fail"},
        ])

        # Task 1: Download failure
        item1 = db.get_next_pending()
        mock_ydl = MagicMock()
        mock_ydl.__enter__.return_value = mock_ydl
        mock_ydl.extract_info.side_effect = Exception("404 Video Not Found")

        with patch("yt_dlp.YoutubeDL", return_value=mock_ydl):
            file_path, metadata, err = await downloader.download_video(item1["id"], item1["video_url"])

        assert file_path is None
        db.set_status(item1["id"], "FAILED", error_message=f"Download Error: {err}")

        # Task 2: Download succeeds but upload fails
        item2 = db.get_next_pending()
        dummy_file2 = os.path.join(dl_dir, f"video_{item2['id']}.mp4")
        with open(dummy_file2, "wb") as f:
            f.write(b"data2")

        mock_ydl2 = MagicMock()
        mock_ydl2.__enter__.return_value = mock_ydl2
        mock_ydl2.extract_info.return_value = {"title": "Upload Fail", "duration": 10}

        with patch("yt_dlp.YoutubeDL", return_value=mock_ydl2):
            file_path2, meta2, dl_err2 = await downloader.download_video(item2["id"], item2["video_url"])

        db.set_status(item2["id"], "UPLOADING", file_size=meta2["file_size"])

        # Telegram upload rejection
        mock_resp_fail = MockAsyncResponse(status=400, json_data={"ok": False, "error_code": 400, "description": "Chat not found"})
        with patch("aiohttp.ClientSession.post", return_value=mock_resp_fail):
            up_success, up_err = await uploader.upload_video(file_path=file_path2, title="Upload Fail", metadata=meta2)

        assert up_success is False
        db.set_status(item2["id"], "FAILED", error_message=f"Upload Error: {up_err}")

        # Cleanup
        downloader.cleanup_file(file_path2)
        downloader.cleanup_video_files(item2["id"])
        assert not os.path.exists(dummy_file2)

        # Check final stats and retries
        stats = db.get_stats()
        assert stats["FAILED"] == 2
        assert stats["PENDING"] == 0
        assert stats["COMPLETED"] == 0
        assert stats["TOTAL"] == 2
