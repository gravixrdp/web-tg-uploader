"""
Unit tests for centralized configuration module (modules/config.py).
"""

import os
import shutil
import unittest
import logging
from unittest.mock import patch, MagicMock

from modules.config import (
    Config,
    AppConfig,
    config,
    get_env_str,
    get_env_int,
    get_env_optional_int,
    get_env_bool,
    mask_sensitive,
)


class TestConfig(unittest.TestCase):

    def test_default_values(self):
        with patch.dict(os.environ, {}, clear=True):
            cfg = Config()
            self.assertEqual(cfg.TELEGRAM_BOT_TOKEN, "")
            self.assertEqual(cfg.TELEGRAM_CHAT_ID, "")
            self.assertEqual(cfg.TELEGRAM_API_BASE, "https://api.telegram.org")
            self.assertEqual(cfg.CRAWL_TARGET_URL, "")
            self.assertEqual(cfg.CRAWL_MODE, "auto")
            self.assertEqual(cfg.MAX_PAGES, 10)
            self.assertEqual(cfg.PERIODIC_CRAWL_INTERVAL, 0)
            self.assertEqual(cfg.UPLOAD_COOLDOWN, 20)
            self.assertEqual(cfg.MAX_RETRIES, 5)
            self.assertEqual(cfg.DB_PATH, "data/queue.db")
            self.assertEqual(cfg.TEMP_DOWNLOAD_DIR, "temp_downloads")
            self.assertIsNone(cfg.HTTP_PORT)
            self.assertFalse(cfg.is_telegram_configured())
            self.assertFalse(cfg.is_crawler_configured())

    def test_custom_environment_loading(self):
        env_vars = {
            "TELEGRAM_BOT_TOKEN": "123456789:ABCdefGHIjklMNOpqrSTUvwxYZ_123456",
            "TELEGRAM_CHAT_ID": "-1001234567890",
            "TELEGRAM_API_BASE": "https://custom-tg-api.org",
            "CRAWL_TARGET_URL": "https://example.com/sitemap.xml",
            "CRAWL_MODE": "sitemap",
            "MAX_PAGES": "25",
            "PERIODIC_CRAWL_INTERVAL": "3600",
            "UPLOAD_COOLDOWN": "15",
            "MAX_RETRIES": "3",
            "DB_PATH": "custom_data/queue.db",
            "TEMP_DOWNLOAD_DIR": "custom_temp",
            "HTTP_PORT": "8080"
        }
        with patch.dict(os.environ, env_vars, clear=True):
            cfg = Config()
            self.assertEqual(cfg.TELEGRAM_BOT_TOKEN, "123456789:ABCdefGHIjklMNOpqrSTUvwxYZ_123456")
            self.assertEqual(cfg.TELEGRAM_CHAT_ID, "-1001234567890")
            self.assertEqual(cfg.TELEGRAM_API_BASE, "https://custom-tg-api.org")
            self.assertEqual(cfg.CRAWL_TARGET_URL, "https://example.com/sitemap.xml")
            self.assertEqual(cfg.CRAWL_MODE, "sitemap")
            self.assertEqual(cfg.MAX_PAGES, 25)
            self.assertEqual(cfg.PERIODIC_CRAWL_INTERVAL, 3600)
            self.assertEqual(cfg.UPLOAD_COOLDOWN, 15)
            self.assertEqual(cfg.MAX_RETRIES, 3)
            self.assertEqual(cfg.DB_PATH, "custom_data/queue.db")
            self.assertEqual(cfg.TEMP_DOWNLOAD_DIR, "custom_temp")
            self.assertEqual(cfg.HTTP_PORT, 8080)
            self.assertTrue(cfg.is_telegram_configured())
            self.assertTrue(cfg.is_crawler_configured())

    def test_port_fallback_for_railway(self):
        with patch.dict(os.environ, {"PORT": "5000"}, clear=True):
            cfg = Config()
            self.assertEqual(cfg.HTTP_PORT, 5000)

    def test_validation_success(self):
        env_vars = {
            "TELEGRAM_BOT_TOKEN": "123456789:ABCdefGHIjklMNOpqrSTUvwxYZ_123456",
            "TELEGRAM_CHAT_ID": "-1001234567890",
            "CRAWL_TARGET_URL": "https://example.com/feed",
            "CRAWL_MODE": "pagination",
            "MAX_PAGES": "5",
            "UPLOAD_COOLDOWN": "10",
            "MAX_RETRIES": "3",
            "HTTP_PORT": "3000"
        }
        with patch.dict(os.environ, env_vars, clear=True):
            cfg = Config()
            errors = cfg.validate(strict=False)
            self.assertEqual(errors, [])

    def test_validation_errors(self):
        env_vars = {
            "TELEGRAM_BOT_TOKEN": "invalid_token_format",
            "TELEGRAM_CHAT_ID": "",
            "CRAWL_TARGET_URL": "ftp://example.com",
            "CRAWL_MODE": "invalid_mode",
            "MAX_PAGES": "0",
            "UPLOAD_COOLDOWN": "-5",
            "PERIODIC_CRAWL_INTERVAL": "-1",
            "MAX_RETRIES": "0",
            "HTTP_PORT": "999999"
        }
        with patch.dict(os.environ, env_vars, clear=True):
            cfg = Config()
            errors = cfg.validate(strict=False)
            self.assertTrue(any("TELEGRAM_BOT_TOKEN" in e for e in errors))
            self.assertTrue(any("TELEGRAM_CHAT_ID" in e for e in errors))
            self.assertTrue(any("CRAWL_TARGET_URL" in e for e in errors))
            self.assertTrue(any("CRAWL_MODE" in e for e in errors))
            self.assertTrue(any("MAX_PAGES" in e for e in errors))
            self.assertTrue(any("UPLOAD_COOLDOWN" in e for e in errors))
            self.assertTrue(any("PERIODIC_CRAWL_INTERVAL" in e for e in errors))
            self.assertTrue(any("MAX_RETRIES" in e for e in errors))
            self.assertTrue(any("HTTP_PORT" in e for e in errors))

            with self.assertRaises(ValueError):
                cfg.validate(strict=True)

    def test_helpers(self):
        self.assertEqual(mask_sensitive("123456789:abcdefghijklmnop"), "123456...mnop")
        self.assertEqual(mask_sensitive(""), "<NOT SET>")
        self.assertEqual(mask_sensitive("short"), "***")

        with patch.dict(os.environ, {"TEST_BOOL": "true", "INT_VAR": "42", "BAD_INT": "abc"}, clear=True):
            self.assertTrue(get_env_bool("TEST_BOOL"))
            self.assertFalse(get_env_bool("NON_EXISTENT"))
            self.assertEqual(get_env_int("INT_VAR", 0), 42)
            self.assertEqual(get_env_int("BAD_INT", 10), 10)
            self.assertEqual(get_env_int("NON_EXISTENT", 100), 100)
            self.assertIsNone(get_env_optional_int("NON_EXISTENT"))
            self.assertEqual(get_env_optional_int("BAD_INT", default=80), 80)

    def test_safe_summary_and_logging(self):
        cfg = Config(
            TELEGRAM_BOT_TOKEN="123456789:ABCdefGHIjklMNOpqrSTUvwxYZ_123456",
            TELEGRAM_CHAT_ID="-1001234567890",
            CRAWL_TARGET_URL="https://test.com/sitemap.xml",
            HTTP_PORT=8080
        )
        summary = cfg.get_safe_summary()
        self.assertIn("123456...", summary["TELEGRAM_BOT_TOKEN"])
        self.assertNotIn("ABCdefGHIjklMNOpqrSTUvwxYZ", summary["TELEGRAM_BOT_TOKEN"])
        self.assertEqual(summary["HTTP_PORT"], 8080)

        mock_logger = MagicMock()
        cfg.log_summary(mock_logger)
        self.assertTrue(mock_logger.info.called)

    def test_ensure_directories(self):
        test_dir = "test_temp_dirs_creation"
        test_db = os.path.join(test_dir, "db", "test.db")
        test_downloads = os.path.join(test_dir, "downloads")
        try:
            cfg = Config(DB_PATH=test_db, TEMP_DOWNLOAD_DIR=test_downloads)
            cfg.ensure_directories()
            self.assertTrue(os.path.exists(os.path.dirname(test_db)))
            self.assertTrue(os.path.exists(test_downloads))
        finally:
            if os.path.exists(test_dir):
                shutil.rmtree(test_dir, ignore_errors=True)

    def test_from_env_file(self):
        test_env_path = ".test_env_file"
        try:
            with open(test_env_path, "w") as f:
                f.write("TELEGRAM_BOT_TOKEN=987654321:XYZ1234567890abcdefghijklmnopqrstuv\n")
                f.write("MAX_PAGES=42\n")
            cfg = Config.from_env(env_file=test_env_path)
            self.assertEqual(cfg.MAX_PAGES, 42)
            self.assertEqual(cfg.TELEGRAM_BOT_TOKEN, "987654321:XYZ1234567890abcdefghijklmnopqrstuv")
        finally:
            if os.path.exists(test_env_path):
                os.remove(test_env_path)


if __name__ == "__main__":
    unittest.main()
