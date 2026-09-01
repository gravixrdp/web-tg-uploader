"""
Unit Tests for modules/downloader.py
Verifies:
1. Disk space pre-flight checks using shutil.disk_usage.
2. Automatic video thumbnail generation with ffmpeg fallback.
3. Custom referer, user-agent, and HTTP headers passing for protected streams.
4. Strict cleanup of video_{id}.* temporary files across all failure modes.
"""

import os
import stat
import shutil
import tempfile
import unittest
from unittest.mock import patch, MagicMock
import asyncio

from modules.downloader import VideoDownloader


class TestVideoDownloader(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="test_downloader_")
        self.downloader = VideoDownloader(
            download_dir=self.test_dir,
            min_free_disk_mb=500,
            default_user_agent="DefaultUA/1.0",
            default_referer="https://default.example.com"
        )

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    # -------------------------------------------------------------
    # 1. Disk Space Pre-flight Check Tests
    # -------------------------------------------------------------
    def test_disk_space_sufficient(self):
        """Test pre-flight disk check succeeds when free space is above threshold."""
        # 1 GB free, threshold is 500 MB
        with patch("shutil.disk_usage", return_value=(10 * 1024**3, 5 * 1024**3, 1024 * 1024 * 1024)):
            ok, free, err = self.downloader.check_disk_space()
            self.assertTrue(ok)
            self.assertIsNone(err)
            self.assertEqual(free, 1024 * 1024 * 1024)

    def test_disk_space_insufficient(self):
        """Test pre-flight disk check fails when free space is below threshold."""
        # 100 MB free, threshold is 500 MB
        with patch("shutil.disk_usage", return_value=(10 * 1024**3, 9.9 * 1024**3, 100 * 1024 * 1024)):
            ok, free, err = self.downloader.check_disk_space()
            self.assertFalse(ok)
            self.assertIsNotNone(err)
            self.assertIn("Pre-flight disk check failed", err)
            self.assertIn("100.0 MB free", err)

    def test_disk_space_insufficient_with_required_bytes(self):
        """Test pre-flight check accounting for additional required bytes."""
        # 600 MB free, threshold is 500 MB, but required is 200 MB (total needed = 700 MB)
        with patch("shutil.disk_usage", return_value=(10 * 1024**3, 9.4 * 1024**3, 600 * 1024 * 1024)):
            ok, free, err = self.downloader.check_disk_space(required_bytes=200 * 1024 * 1024)
            self.assertFalse(ok)
            self.assertIsNotNone(err)
            self.assertIn("700.0 MB", err)

    # -------------------------------------------------------------
    # 2. Custom Referer and User-Agent Support Tests
    # -------------------------------------------------------------
    def test_ydl_opts_defaults(self):
        """Test yt-dlp options populate class defaults when not explicitly provided."""
        opts = self.downloader._get_ydl_opts("video.mp4")
        self.assertEqual(opts.get("user_agent"), "DefaultUA/1.0")
        self.assertEqual(opts.get("referer"), "https://default.example.com")
        self.assertEqual(opts.get("http_headers", {}).get("User-Agent"), "DefaultUA/1.0")
        self.assertEqual(opts.get("http_headers", {}).get("Referer"), "https://default.example.com")

    def test_ydl_opts_custom_overrides(self):
        """Test custom referer, user_agent, and extra headers override defaults."""
        custom_headers = {
            "Authorization": "Bearer token123",
            "Cookie": "session=xyz",
            "Sec-Fetch-Mode": "navigate"
        }
        opts = self.downloader._get_ydl_opts(
            output_template="video.mp4",
            referer="https://custom.stream.org/watch",
            user_agent="CustomBot/2.0",
            custom_headers=custom_headers
        )
        self.assertEqual(opts.get("user_agent"), "CustomBot/2.0")
        self.assertEqual(opts.get("referer"), "https://custom.stream.org/watch")
        self.assertEqual(opts.get("http_headers", {}).get("User-Agent"), "CustomBot/2.0")
        self.assertEqual(opts.get("http_headers", {}).get("Referer"), "https://custom.stream.org/watch")
        self.assertEqual(opts.get("http_headers", {}).get("Authorization"), "Bearer token123")
        self.assertEqual(opts.get("http_headers", {}).get("Cookie"), "session=xyz")
        self.assertEqual(opts.get("http_headers", {}).get("Sec-Fetch-Mode"), "navigate")

    # -------------------------------------------------------------
    # 3. Automatic Thumbnail Generation (FFmpeg Fallback) Tests
    # -------------------------------------------------------------
    def test_find_thumbnail_existing(self):
        """Test discovering yt-dlp generated thumbnail."""
        thumb_path = os.path.join(self.test_dir, "video_42.webp")
        with open(thumb_path, "wb") as f:
            f.write(b"\x00" * 100)

        found = self.downloader._find_thumbnail(42)
        self.assertEqual(found, thumb_path)

    def test_find_thumbnail_ignores_empty(self):
        """Test ignoring 0-byte thumbnail files."""
        thumb_path = os.path.join(self.test_dir, "video_42.jpg")
        with open(thumb_path, "wb") as f:
            pass  # 0 bytes

        found = self.downloader._find_thumbnail(42)
        self.assertIsNone(found)

    @patch("shutil.which", return_value="/usr/bin/ffmpeg")
    @patch("subprocess.run")
    def test_generate_thumbnail_ffmpeg_success(self, mock_subprocess_run, mock_which):
        """Test successful ffmpeg thumbnail fallback."""
        video_path = os.path.join(self.test_dir, "video_42.mp4")
        with open(video_path, "wb") as f:
            f.write(b"\x00" * 500)

        thumb_output = os.path.join(self.test_dir, "video_42.jpg")

        def side_effect(cmd, **kwargs):
            with open(thumb_output, "wb") as f:
                f.write(b"\xFF\xD8\xFF" * 10)  # non-empty jpeg
            mock_res = MagicMock()
            mock_res.returncode = 0
            return mock_res

        mock_subprocess_run.side_effect = side_effect

        result = self.downloader._generate_thumbnail_ffmpeg(video_path, 42, duration=60)
        self.assertEqual(result, thumb_output)
        self.assertTrue(os.path.exists(thumb_output))

    @patch("shutil.which", return_value=None)
    def test_generate_thumbnail_ffmpeg_no_binary(self, mock_which):
        """Test ffmpeg fallback gracefully returns None when ffmpeg is not installed."""
        video_path = os.path.join(self.test_dir, "video_42.mp4")
        with open(video_path, "wb") as f:
            f.write(b"data")

        result = self.downloader._generate_thumbnail_ffmpeg(video_path, 42, duration=10)
        self.assertIsNone(result)

    # -------------------------------------------------------------
    # 4. Strict Temporary Files Cleanup Tests
    # -------------------------------------------------------------
    def test_cleanup_video_files_all_extensions(self):
        """Test cleanup_video_files removes all video_{id}.* files and leaves others intact."""
        v42_files = [
            "video_42.mp4",
            "video_42.mp4.part",
            "video_42.f137.mp4",
            "video_42.f140.m4a",
            "video_42.webp",
            "video_42.jpg",
            "video_42.ytdl",
            "video_42.info.json",
            "video_42.temp.mp4",
            "video_42_thumb.jpg",
            "video_42"
        ]
        other_files = [
            "video_43.mp4",
            "video_43.jpg",
            "video_420.mp4"  # should not be deleted
        ]

        for fname in v42_files + other_files:
            fpath = os.path.join(self.test_dir, fname)
            with open(fpath, "w") as f:
                f.write("content")

        self.downloader.cleanup_video_files(42)

        for fname in v42_files:
            fpath = os.path.join(self.test_dir, fname)
            self.assertFalse(os.path.exists(fpath), f"File {fname} was not deleted!")

        for fname in other_files:
            fpath = os.path.join(self.test_dir, fname)
            self.assertTrue(os.path.exists(fpath), f"Unrelated file {fname} was accidentally deleted!")

    def test_cleanup_read_only_file(self):
        """Test cleanup_file handles read-only files without raising exception."""
        ro_file = os.path.join(self.test_dir, "video_99.mp4")
        with open(ro_file, "w") as f:
            f.write("test")

        # Make file read-only
        os.chmod(ro_file, stat.S_IREAD)

        self.downloader.cleanup_video_files(99)
        self.assertFalse(os.path.exists(ro_file))

    # -------------------------------------------------------------
    # 5. Failure Modes Cleanup Guarantees
    # -------------------------------------------------------------
    def test_sync_download_disk_space_failure_cleans_up(self):
        """Test that disk space failure immediately cleans any files and aborts."""
        part_file = os.path.join(self.test_dir, "video_10.part")
        with open(part_file, "w") as f:
            f.write("partial")

        with patch("shutil.disk_usage", return_value=(10 * 1024**3, 9.99 * 1024**3, 10 * 1024 * 1024)):
            target, meta, err = self.downloader._sync_download(10, "https://example.com/video")
            self.assertIsNone(target)
            self.assertIsNone(meta)
            self.assertIn("Pre-flight disk check failed", err)
            self.assertFalse(os.path.exists(part_file))

    @patch("yt_dlp.YoutubeDL")
    def test_sync_download_ytdlp_exception_cleans_up(self, mock_ytdl):
        """Test that yt-dlp exception wipes all video_{id}.* files."""
        part_file = os.path.join(self.test_dir, "video_20.f137.mp4.part")
        with open(part_file, "w") as f:
            f.write("partial data")

        mock_instance = MagicMock()
        mock_instance.extract_info.side_effect = RuntimeError("HTTP 403 Forbidden")
        mock_ytdl.return_value.__enter__.return_value = mock_instance

        target, meta, err = self.downloader._sync_download(20, "https://example.com/stream.m3u8")
        self.assertIsNone(target)
        self.assertIsNone(meta)
        self.assertIn("HTTP 403 Forbidden", err)
        self.assertFalse(os.path.exists(part_file))

    @patch("yt_dlp.YoutubeDL")
    def test_sync_download_missing_target_file_cleans_up(self, mock_ytdl):
        """Test that missing downloaded file wipes leftovers and returns error."""
        leftover = os.path.join(self.test_dir, "video_30.temp")
        with open(leftover, "w") as f:
            f.write("leftover")

        mock_instance = MagicMock()
        mock_instance.extract_info.return_value = {"title": "Test", "duration": 10}
        mock_ytdl.return_value.__enter__.return_value = mock_instance

        target, meta, err = self.downloader._sync_download(30, "https://example.com/video")
        self.assertIsNone(target)
        self.assertIsNone(meta)
        self.assertEqual(err, "Downloaded file not found on disk")
        self.assertFalse(os.path.exists(leftover))

    @patch("yt_dlp.YoutubeDL")
    @patch.object(VideoDownloader, "_generate_thumbnail_ffmpeg")
    def test_sync_download_success_with_ffmpeg_thumbnail(self, mock_thumb_gen, mock_ytdl):
        """Test end-to-end success path triggering ffmpeg thumbnail fallback."""
        video_file = os.path.join(self.test_dir, "video_50.mp4")
        with open(video_file, "wb") as f:
            f.write(b"valid video stream data")

        thumb_file = os.path.join(self.test_dir, "video_50.jpg")
        mock_thumb_gen.return_value = thumb_file

        mock_instance = MagicMock()
        mock_instance.extract_info.return_value = {
            "title": "Sample Protected Stream",
            "duration": 120,
            "width": 1920,
            "height": 1080
        }
        mock_ytdl.return_value.__enter__.return_value = mock_instance

        target, meta, err = self.downloader._sync_download(
            50,
            "https://example.com/protected",
            referer="https://example.com",
            user_agent="TestBot"
        )
        self.assertEqual(target, video_file)
        self.assertIsNone(err)
        self.assertEqual(meta["title"], "Sample Protected Stream")
        self.assertEqual(meta["duration"], 120)
        self.assertEqual(meta["width"], 1920)
        self.assertEqual(meta["height"], 1080)
        self.assertEqual(meta["thumbnail"], thumb_file)
        self.assertTrue(os.path.exists(video_file))

    def test_async_download_cancellation_cleans_up(self):
        """Test async download task cancellation triggers cleanup."""
        part_file = os.path.join(self.test_dir, "video_60.mp4.part")
        with open(part_file, "w") as f:
            f.write("partial")

        async def run_cancellation():
            with patch.object(self.downloader, "_sync_download", side_effect=asyncio.CancelledError()):
                await self.downloader.download_video(60, "https://example.com/video")

        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(run_cancellation())

        self.assertFalse(os.path.exists(part_file))


if __name__ == "__main__":
    unittest.main()
