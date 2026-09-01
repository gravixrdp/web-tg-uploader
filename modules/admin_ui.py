"""
Admin UI Template & Helper Module for Bulk Video Scraper & Telegram Uploader.
Loads static/index.html with embedded fallback.
"""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Base static path
STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")
INDEX_HTML_PATH = os.path.join(STATIC_DIR, "index.html")

_CACHED_HTML: Optional[str] = None


def get_admin_html(reload: bool = False) -> str:
    """
    Returns the complete HTML content for the Web Admin Dashboard.
    Reads from static/index.html or returns embedded version if missing.
    """
    global _CACHED_HTML
    if _CACHED_HTML and not reload:
        return _CACHED_HTML

    if os.path.exists(INDEX_HTML_PATH):
        try:
            with open(INDEX_HTML_PATH, "r", encoding="utf-8") as f:
                _CACHED_HTML = f.read()
                return _CACHED_HTML
        except Exception as e:
            logger.error(f"Error reading {INDEX_HTML_PATH}: {e}")

    # Fallback minimal placeholder if file not found
    return """<!DOCTYPE html>
<html>
<head><title>TeleScraper Admin</title></head>
<body style="background:#080b11;color:#fff;font-family:sans-serif;padding:40px;text-align:center;">
  <h2>TeleScraper Admin Dashboard</h2>
  <p>Static index file not found at static/index.html. Please ensure static/index.html is present.</p>
</body>
</html>"""
