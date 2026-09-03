"""
Crawler & Discovery Module for Bulk Video Scraping.
Supports:
1. RSSFeedCrawler (RSS 2.0, Media RSS, Atom feeds, Podcast enclosures)
2. SitemapCrawler (standard XML, video sitemaps, and sitemap index files)
3. PaginationCrawler (crawling page=1..N for direct video links with rate limiting)
4. HTML5Extractor & M3U8 Inspector (extracting HTML5 video, inline JS stream configs,
   direct m3u8 playlists with resolution hints)
5. UniversalCrawler (unified intelligent crawler routing auto/rss/sitemap/pagination/html5 modes)
"""

import os
import re
import random
import logging
import asyncio
from typing import List, Dict, Set, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse, unquote
import html
import aiohttp
from bs4 import BeautifulSoup

from modules.config import config

logger = logging.getLogger(__name__)

# Media extensions recognized across crawlers
VIDEO_EXTENSIONS = (
    '.mp4', '.mkv', '.webm', '.mov', '.m3u8', '.ts', '.avi',
    '.flv', '.wmv', '.m4v', '.3gp', '.mpd', '.f4v', '.vob', '.ogv'
)

IMAGE_EXTENSIONS = (
    '.jpg', '.jpeg', '.png', '.webp', '.gif', '.svg', '.ico',
    '.bmp', '.tiff', '.avif', '.heic'
)

# Common tracking and analytics query parameters to strip for strict deduplication
TRACKING_PARAMS = {
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'fbclid', 'gclid', 'msclkid', 'mc_cid', 'mc_eid', 'ref', 'source',
    '_ga', '_gl', 'yclid', 'zanpid'
}


def normalize_media_url(url: str) -> str:
    """
    Canonicalizes a media URL for 100% airtight deduplication:
    - Strips whitespace, quotes, and HTML entities
    - Lowercases scheme and netloc
    - Strips URL fragments (#...)
    - Strips tracking / analytics query parameters (utm_*, fbclid, etc.)
    - Removes trailing slashes on path
    """
    if not url:
        return ""
    url = url.strip().strip("'\"").replace("&amp;", "&")
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return url

        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()

        # Clean query parameters
        from urllib.parse import parse_qsl, urlencode
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

# Pool of modern, diverse User-Agent strings for anti-ban rotation
USER_AGENTS = [
    # Chrome on Windows 10/11
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    # Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    # Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    # Firefox on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:127.0) Gecko/20100101 Firefox/127.0",
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
    # Chrome on Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    # Safari on iPhone (iOS 17)
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    # Chrome on Android
    "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.122 Mobile Safari/537.36",
]


def extract_resolution_hint(text: str, url: str = "") -> Optional[str]:
    """
    Extracts video resolution / quality hint from text, attributes, or URLs.
    Recognizes 4320p/8k, 2160p/4k, 1440p/2k, 1080p, 720p, 480p, 360p, 240p,
    and dimension patterns like 1920x1080, 1280x720 across filenames, URLs, and text.
    """
    combined = f"{text} {url}".lower()

    # Dimension resolution check: e.g. 1920x1080, 3840x2160, 1280x720
    dim_match = re.search(r'(?<!\d)(\d{3,4})x(\d{3,4})(?!\d)', combined)
    if dim_match:
        w, h = int(dim_match.group(1)), int(dim_match.group(2))
        height = min(w, h)
        if height >= 2160:
            return "4K"
        elif height >= 1440:
            return "1440p"
        elif height >= 1080:
            return "1080p"
        elif height >= 720:
            return "720p"
        elif height >= 480:
            return "480p"
        elif height >= 360:
            return "360p"
        elif height >= 240:
            return "240p"
        return f"{height}p"

    # Named quality flags (using delimiter lookaround to cleanly handle _, -, /, .)
    if re.search(r'(?<![a-z0-9])(8k|4320p)(?![a-z0-9])', combined):
        return "8K"
    if re.search(r'(?<![a-z0-9])(4k|2160p|uhd)(?![a-z0-9])', combined):
        return "4K"
    if re.search(r'(?<![a-z0-9])(2k|1440p|qhd)(?![a-z0-9])', combined):
        return "1440p"
    if re.search(r'(?<![a-z0-9])(1080p|fhd|1080)(?![a-z0-9])', combined):
        return "1080p"
    if re.search(r'(?<![a-z0-9])(720p|hd|720)(?![a-z0-9])', combined):
        return "720p"
    if re.search(r'(?<![a-z0-9])(480p|sd|480)(?![a-z0-9])', combined):
        return "480p"
    if re.search(r'(?<![a-z0-9])(360p|360)(?![a-z0-9])', combined):
        return "360p"
    if re.search(r'(?<![a-z0-9])(240p|240)(?![a-z0-9])', combined):
        return "240p"

    return None


def parse_m3u8_manifest(content: str, base_url: str) -> List[Dict[str, Any]]:
    """
    Parses HLS Master Playlist (#EXT-X-STREAM-INF) to extract variant stream links
    along with resolution, bandwidth, codecs, and quality hints.
    """
    variants: List[Dict[str, Any]] = []
    if not content or "#EXTM3U" not in content:
        return variants

    lines = [line.strip() for line in content.splitlines() if line.strip()]
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("#EXT-X-STREAM-INF:"):
            # Parse attributes
            attrs = line[len("#EXT-X-STREAM-INF:"):].strip()

            bandwidth = None
            resolution = None
            name = None
            codecs = None

            bw_match = re.search(r'BANDWIDTH=(\d+)', attrs, re.IGNORECASE)
            if bw_match:
                bandwidth = int(bw_match.group(1))

            res_match = re.search(r'RESOLUTION=(\d+x\d+)', attrs, re.IGNORECASE)
            if res_match:
                resolution = res_match.group(1)

            name_match = re.search(r'NAME="([^"]+)"', attrs, re.IGNORECASE)
            if name_match:
                name = name_match.group(1)

            codecs_match = re.search(r'CODECS="([^"]+)"', attrs, re.IGNORECASE)
            if codecs_match:
                codecs = codecs_match.group(1)

            # Next non-comment line is the stream URL
            j = i + 1
            while j < len(lines) and lines[j].startswith("#"):
                j += 1

            if j < len(lines):
                stream_uri = lines[j]
                full_stream_url = urljoin(base_url, stream_uri)
                quality_hint = extract_resolution_hint(f"{resolution or ''} {name or ''}", full_stream_url)

                variants.append({
                    "url": full_stream_url,
                    "resolution": resolution,
                    "quality": quality_hint or (name if name else "stream"),
                    "bandwidth": bandwidth,
                    "codecs": codecs,
                })
                i = j
        i += 1

    return variants


class BaseCrawler:
    """
    Base crawler providing:
    - User-Agent rotation and realistic standard browser request headers
    - Configurable request delay and jitter to prevent IP bans
    - Domain-aware rate limiting
    - Resilient async HTTP fetching with retry logic
    """

    def __init__(
        self,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 25,
        delay: Optional[float] = None,
        jitter: Optional[float] = None,
        user_agents: Optional[List[str]] = None,
    ):
        self.custom_headers = headers or {}
        self.timeout = aiohttp.ClientTimeout(total=timeout, connect=15, sock_read=timeout)
        self.user_agents = user_agents or USER_AGENTS

        # Configurable delay and jitter between requests (env override or default)
        self.delay = config.CRAWL_DELAY if delay is None else delay
        self.jitter = config.CRAWL_JITTER if jitter is None else jitter

        self._last_request_times: Dict[str, float] = {}

    def get_random_user_agent(self) -> str:
        """Select a random User-Agent from the pool."""
        return random.choice(self.user_agents)

    def get_request_headers(self, target_url: Optional[str] = None) -> Dict[str, str]:
        """Generate realistic browser headers with rotated User-Agent."""
        ua = self.get_random_user_agent()
        parsed = urlparse(target_url) if target_url else None
        origin = f"{parsed.scheme}://{parsed.netloc}" if parsed and parsed.netloc else ""

        headers = {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin" if origin else "none",
            "Sec-Fetch-User": "?1",
        }

        if origin:
            headers["Referer"] = origin

        # Override with any custom user-provided headers
        headers.update(self.custom_headers)
        return headers

    async def _apply_delay(self, url: str) -> None:
        """Enforces rate limiting with randomized jitter per target domain."""
        if self.delay <= 0 and self.jitter <= 0:
            return

        domain = urlparse(url).netloc or "global"
        now = asyncio.get_event_loop().time()
        last_time = self._last_request_times.get(domain, 0.0)

        elapsed = now - last_time
        sleep_needed = self.delay + random.uniform(0, self.jitter) - elapsed
        if sleep_needed > 0:
            logger.debug(f"Applying rate-limit delay for {domain}: sleeping {sleep_needed:.2f}s")
            await asyncio.sleep(sleep_needed)

        self._last_request_times[domain] = asyncio.get_event_loop().time()

    def is_video_url(self, url: str) -> bool:
        """Check if URL path ends with a video/stream extension."""
        parsed = urlparse(url.lower())
        clean_path = parsed.path.rstrip('/')
        return clean_path.endswith(VIDEO_EXTENSIONS)

    def is_image_url(self, url: str) -> bool:
        """Check if URL path ends with a static image extension."""
        parsed = urlparse(url.lower())
        clean_path = parsed.path.rstrip('/')
        return clean_path.endswith(IMAGE_EXTENSIONS)

    def is_media_match(self, url: str, media_type: str = "video") -> bool:
        """
        Strictly filters media URLs based on media type:
        - 'video' (DEFAULT): accepts only video files (.mp4, .mkv, .m3u8, etc.), strictly rejects images.
        - 'image': accepts only image files (.jpg, .png, .webp, etc.).
        - 'all': accepts both video and image files.
        """
        media_type = (media_type or "video").lower()
        if media_type == "image":
            return self.is_image_url(url)
        elif media_type == "all":
            return self.is_video_url(url) or self.is_image_url(url)
        # Default: Video ONLY (Strictly exclude static images)
        return self.is_video_url(url) and not self.is_image_url(url)

    async def fetch_text(
        self,
        session: aiohttp.ClientSession,
        url: str,
        max_retries: int = 3,
    ) -> Optional[str]:
        """Fetch URL content as text with rate limiting, header rotation, and retry handling."""
        for attempt in range(1, max_retries + 1):
            await self._apply_delay(url)
            headers = self.get_request_headers(url)
            try:
                async with session.get(url, headers=headers, timeout=self.timeout) as resp:
                    if resp.status == 200:
                        # Attempt UTF-8 decode, fallback to resp.text() default
                        try:
                            return await resp.text(encoding="utf-8")
                        except UnicodeDecodeError:
                            return await resp.text()

                    if resp.status in (429, 503):
                        retry_after = resp.headers.get("Retry-After")
                        wait_sec = float(retry_after) if retry_after and retry_after.isdigit() else (attempt * 2.0)
                        logger.warning(f"HTTP {resp.status} for {url}. Backing off {wait_sec:.1f}s (Attempt {attempt}/{max_retries})")
                        await asyncio.sleep(wait_sec)
                        continue

                    logger.warning(f"Fetch {url} returned HTTP {resp.status}")
                    return None

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"Network error fetching {url} (Attempt {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    await asyncio.sleep(attempt * 1.5)
            except Exception as e:
                logger.error(f"Unexpected error fetching {url}: {e}")
                return None

        return None


class RSSFeedCrawler(BaseCrawler):
    """
    Parses RSS 2.0, Media RSS (mrss), Atom feeds, and podcast video enclosures.
    Extracts direct video stream URLs, enclosures, media tags, titles, and resolution hints.
    """

    async def crawl(
        self,
        feed_url: str,
        session: Optional[aiohttp.ClientSession] = None
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        seen_urls: Set[str] = set()
        close_session = False

        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        try:
            logger.info(f"Crawling RSS/Atom feed: {feed_url}")
            content = await self.fetch_text(session, feed_url)
            if not content:
                logger.warning(f"Empty or unreachable RSS feed: {feed_url}")
                return results

            results = self.parse_feed_content(content, feed_url)

        finally:
            if close_session:
                await session.close()

        logger.info(f"RSSFeedCrawler finished: Discovered {len(results)} items from {feed_url}")
        return results

    def parse_feed_content(self, content: str, base_url: str) -> List[Dict[str, Any]]:
        """Parses XML/RSS/Atom content string and returns video items."""
        results: List[Dict[str, Any]] = []
        seen_urls: Set[str] = set()

        # Parse with XML parser if available, fallback to html.parser
        soup = BeautifulSoup(content, "xml" if "xml" in content.lower() or "<?xml" in content else "html.parser")

        # 1. RSS 2.0 / Media RSS items (<item>)
        items = soup.find_all("item")
        # 2. Atom entries (<entry>)
        entries = soup.find_all("entry")

        feed_elements = items if items else entries

        for elem in feed_elements:
            # --- Extract Title ---
            title = ""
            title_tag = elem.find("title")
            if title_tag and title_tag.text:
                title = html.unescape(title_tag.text.strip())

            # Fallback to media:title
            if not title:
                media_title = elem.find(["media:title", "title"])
                if media_title and media_title.text:
                    title = html.unescape(media_title.text.strip())

            # --- Extract Video Candidate URLs & Metadata ---
            candidates: List[Dict[str, Any]] = []

            # A. Check <enclosure> tags (RSS/Podcast)
            for enc in elem.find_all("enclosure"):
                enc_url = enc.get("url")
                enc_type = enc.get("type", "").lower()
                if enc_url:
                    full_url = urljoin(base_url, enc_url.strip())
                    if enc_type.startswith("video/") or self.is_video_url(full_url) or "m3u8" in full_url:
                        res_hint = extract_resolution_hint(enc_type, full_url)
                        candidates.append({
                            "url": full_url,
                            "type": enc_type,
                            "resolution": res_hint,
                            "length": enc.get("length")
                        })

            # B. Check <media:content> tags (Media RSS)
            for mc in elem.find_all(["media:content", "content"]):
                mc_url = mc.get("url")
                mc_type = mc.get("type", "").lower()
                mc_medium = mc.get("medium", "").lower()
                mc_width = mc.get("width")
                mc_height = mc.get("height")
                mc_bitrate = mc.get("bitrate")

                if mc_url:
                    full_url = urljoin(base_url, mc_url.strip())
                    is_vid = (
                        mc_medium == "video" or
                        mc_type.startswith("video/") or
                        self.is_video_url(full_url) or
                        "m3u8" in full_url
                    )
                    if is_vid:
                        # Determine resolution hint from width/height or url
                        res_text = f"{mc_width}x{mc_height}" if mc_width and mc_height else ""
                        res_hint = extract_resolution_hint(f"{res_text} {mc_type}", full_url)
                        candidates.append({
                            "url": full_url,
                            "type": mc_type,
                            "resolution": res_hint,
                            "bitrate": mc_bitrate,
                            "width": mc_width,
                            "height": mc_height
                        })

            # C. Check <media:player> or <media:group>
            for mp in elem.find_all(["media:player", "player"]):
                mp_url = mp.get("url")
                if mp_url:
                    full_url = urljoin(base_url, mp_url.strip())
                    if self.is_video_url(full_url) or "m3u8" in full_url:
                        candidates.append({
                            "url": full_url,
                            "type": "video/stream",
                            "resolution": extract_resolution_hint("", full_url)
                        })

            # D. Check Atom <link> tags
            for link in elem.find_all("link"):
                href = link.get("href")
                rel = link.get("rel", "")
                link_type = link.get("type", "").lower()
                if href:
                    full_url = urljoin(base_url, href.strip())
                    if (
                        rel == "enclosure" or
                        link_type.startswith("video/") or
                        self.is_video_url(full_url) or
                        "m3u8" in full_url
                    ):
                        res_hint = extract_resolution_hint(link_type, full_url)
                        candidates.append({
                            "url": full_url,
                            "type": link_type,
                            "resolution": res_hint
                        })

            # E. Check RSS <link> text tag if not already found
            if not candidates:
                link_tag = elem.find("link")
                if link_tag and link_tag.text:
                    full_url = urljoin(base_url, link_tag.text.strip())
                    if self.is_video_url(full_url) or "m3u8" in full_url:
                        candidates.append({
                            "url": full_url,
                            "type": "video/url",
                            "resolution": extract_resolution_hint("", full_url)
                        })

            # F. Fallback: Search embedded URLs in <description> or <content:encoded>
            if not candidates:
                desc_tag = elem.find(["description", "content:encoded", "summary", "content"])
                if desc_tag and desc_tag.text:
                    desc_text = desc_tag.text
                    found_streams = re.findall(
                        r'(https?://[^\s"\'<>]+\.(?:mp4|m3u8|webm|mkv|mov|ts))',
                        desc_text,
                        re.IGNORECASE
                    )
                    for stream_url in found_streams:
                        candidates.append({
                            "url": stream_url,
                            "type": "video/embedded",
                            "resolution": extract_resolution_hint("", stream_url)
                        })

            # --- Check for Thumbnail ---
            thumbnail_url = None
            thumb_tag = elem.find(["media:thumbnail", "thumbnail", "itunes:image"])
            if thumb_tag:
                thumb_src = thumb_tag.get("url") or thumb_tag.get("href")
                if thumb_src:
                    thumbnail_url = urljoin(base_url, thumb_src.strip())

            # --- Process Candidates ---
            for cand in candidates:
                video_url = cand["url"]
                if video_url in seen_urls:
                    continue
                seen_urls.add(video_url)

                res_hint = cand.get("resolution") or extract_resolution_hint(title, video_url)
                item_title = title if title else video_url.rstrip("/").split("/")[-1].replace("-", " ").replace("_", " ")

                # Decorate title with resolution if detected and not already in title
                if res_hint and res_hint.lower() not in item_title.lower():
                    item_title = f"{item_title} [{res_hint}]"

                item_data = {
                    "url": video_url,
                    "title": item_title,
                }
                if res_hint:
                    item_data["resolution"] = res_hint
                if thumbnail_url:
                    item_data["thumbnail"] = thumbnail_url

                results.append(item_data)

        return results


class SitemapCrawler(BaseCrawler):
    """
    Parses standard sitemap.xml, video sitemaps (video:video),
    and nested sitemap index XML files.
    """

    async def crawl(
        self,
        sitemap_url: str,
        session: Optional[aiohttp.ClientSession] = None
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        close_session = False

        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        try:
            logger.info(f"Crawling Sitemap: {sitemap_url}")
            content = await self.fetch_text(session, sitemap_url)
            if not content:
                return results

            soup = BeautifulSoup(content, "xml" if "xml" in content.lower() or "<?xml" in content else "html.parser")

            # Check if this is a sitemap index file containing child sitemaps
            sitemap_tags = soup.find_all("sitemap")
            if sitemap_tags:
                logger.info(f"Discovered sitemap index at {sitemap_url} with {len(sitemap_tags)} sub-sitemaps")
                for sm in sitemap_tags:
                    loc = sm.find("loc")
                    if loc and loc.text:
                        sub_results = await self.crawl(loc.text.strip(), session=session)
                        results.extend(sub_results)
                return results

            # Standard URL set sitemap
            url_tags = soup.find_all("url")
            for ut in url_tags:
                loc = ut.find("loc")
                if not loc or not loc.text:
                    continue
                page_url = loc.text.strip()

                # Check for Google Video Sitemap tags (<video:video>)
                video_tag = ut.find(["video:video", "video"])
                video_url = None
                title = "Video Media"
                thumb_url = None

                if video_tag:
                    # Video content location or player location
                    content_loc = video_tag.find(["video:content_loc", "content_loc"]) or video_tag.find(re.compile(r'content_loc', re.I))
                    player_loc = video_tag.find(["video:player_loc", "player_loc"]) or video_tag.find(re.compile(r'player_loc', re.I))
                    if content_loc and content_loc.text:
                        video_url = content_loc.text.strip()
                    elif player_loc and player_loc.text:
                        video_url = player_loc.text.strip()

                    v_title = video_tag.find(re.compile(r'title', re.I))
                    if v_title and v_title.text:
                        title = html.unescape(v_title.text.strip())

                    v_thumb = video_tag.find(re.compile(r'thumbnail_loc', re.I))
                    if v_thumb and v_thumb.text:
                        thumb_url = v_thumb.text.strip()

                # If no specific video tag, check if loc itself is a video URL
                if not video_url:
                    if self.is_video_url(page_url):
                        video_url = page_url
                        v_title_direct = ut.find(re.compile(r'title', re.I))
                        if v_title_direct and v_title_direct.text:
                            title = html.unescape(v_title_direct.text.strip())
                        else:
                            title = page_url.rstrip("/").split("/")[-1].replace("-", " ").replace("_", " ")

                if video_url:
                    res_hint = extract_resolution_hint(title, video_url)
                    if res_hint and res_hint.lower() not in title.lower():
                        title = f"{title} [{res_hint}]"

                    item: Dict[str, Any] = {"url": video_url, "title": title}
                    if res_hint:
                        item["resolution"] = res_hint
                    if thumb_url:
                        item["thumbnail"] = thumb_url
                    results.append(item)

        finally:
            if close_session:
                await session.close()

        logger.info(f"SitemapCrawler finished: Discovered {len(results)} items from {sitemap_url}")
        return results


class PaginationCrawler(BaseCrawler):
    """
    Crawls paginated index pages (e.g. ?page=1..N) extracting video links,
    with anti-ban rate limiting and header rotation.
    """

    async def crawl(
        self,
        base_url: str,
        max_pages: int = 10,
        page_param: str = "page",
        session: Optional[aiohttp.ClientSession] = None
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        seen_urls: Set[str] = set()
        close_session = False

        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        try:
            for page in range(1, max_pages + 1):
                delimiter = "&" if "?" in base_url else "?"
                target_page_url = f"{base_url}{delimiter}{page_param}={page}"
                logger.info(f"Crawling pagination: {target_page_url}")

                html_text = await self.fetch_text(session, target_page_url)
                if not html_text:
                    break

                soup = BeautifulSoup(html_text, "html.parser")
                links = soup.find_all("a", href=True)
                page_found = 0

                for link in links:
                    href = link["href"].strip()
                    abs_url = urljoin(base_url, href)

                    if (self.is_video_url(abs_url) or "m3u8" in abs_url) and abs_url not in seen_urls:
                        seen_urls.add(abs_url)
                        raw_title = link.get_text(strip=True) or abs_url.split("/")[-1]
                        res_hint = extract_resolution_hint(raw_title, abs_url)
                        if res_hint and res_hint.lower() not in raw_title.lower():
                            raw_title = f"{raw_title} [{res_hint}]"

                        item: Dict[str, Any] = {"url": abs_url, "title": raw_title}
                        if res_hint:
                            item["resolution"] = res_hint
                        results.append(item)
                        page_found += 1

                logger.info(f"Page {page}: Found {page_found} video URLs.")
                if page_found == 0 and page > 1:
                    logger.info("No further video links discovered on page. Ending pagination scan.")
                    break
        finally:
            if close_session:
                await session.close()

        return results


class HTML5Extractor(BaseCrawler):
    """
    Extracts video and stream sources from HTML pages.
    Detects:
    1. <video src="..."> and <source src="..."> tags
    2. HTML5 data-* attributes (data-src, data-video-url, data-hls, data-m3u8, etc.)
    3. Inline JavaScript player configurations (JWPlayer, VideoJS, HLS.js, Video objects)
    4. Direct M3U8 Master playlists with variant resolution hints and stream links
    5. Discovered RSS/Atom feed links in <head>
    """

    async def inspect_m3u8(
        self,
        session: aiohttp.ClientSession,
        m3u8_url: str
    ) -> List[Dict[str, Any]]:
        """
        Inspects an M3U8 playlist. If it's a master playlist with multiple resolution
        variants, parses and returns variant streams with resolution metadata.
        """
        content = await self.fetch_text(session, m3u8_url)
        if not content:
            return []

        return parse_m3u8_manifest(content, m3u8_url)

    def extract_feed_links(self, soup: BeautifulSoup, page_url: str) -> List[str]:
        """Extracts RSS / Atom alternate feed URLs from HTML <head>."""
        feed_urls: List[str] = []
        for link in soup.find_all("link", rel=lambda r: r and "alternate" in r):
            link_type = link.get("type", "").lower()
            href = link.get("href")
            if href and ("rss" in link_type or "atom" in link_type or "xml" in link_type):
                feed_urls.append(urljoin(page_url, href.strip()))
        return feed_urls

    async def extract(
        self,
        page_url: str,
        session: Optional[aiohttp.ClientSession] = None,
        inspect_m3u8_variants: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Extracts all direct video and stream links from the given page URL.
        """
        results: List[Dict[str, Any]] = []
        seen_urls: Set[str] = set()
        close_session = False

        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        try:
            html_text = await self.fetch_text(session, page_url)
            if not html_text:
                return results

            soup = BeautifulSoup(html_text, "html.parser")
            page_title = html.unescape(soup.title.get_text(strip=True)) if soup.title else "Untitled Media"

            # 1. Playerjs, KVS Flashvars, and Embedded Player configs (Highest Priority for Full Videos)
            player_script_patterns = [
                r'Playerjs\s*\(\s*\{[^}]*[\'"]?file[\'"]?\s*:\s*[\'"]([^\'"]+)[\'"]',
                r'[\'"]?file[\'"]?\s*:\s*[\'"](/videofile/[^\'"]+)[\'"]',
                r'[\'"]?video_url[\'"]?\s*:\s*[\'"]([^\'"]+)[\'"]',
                r'[\'"]?video_alt_url[\'"]?\s*:\s*[\'"]([^\'"]+)[\'"]',
                r'jwplayer\s*\([^)]*\)\s*\.setup\s*\(\s*\{[^}]*[\'"]?file[\'"]?\s*:\s*[\'"]([^\'"]+)[\'"]',
            ]
            for pat in player_script_patterns:
                for match in re.findall(pat, html_text, re.IGNORECASE):
                    clean_url = urljoin(page_url, match.strip().replace("\\/", "/"))
                    if clean_url not in seen_urls and not is_preview_or_teaser(clean_url):
                        seen_urls.add(clean_url)
                        res = extract_resolution_hint("", clean_url)
                        title = f"{page_title} [{res}]" if res and res.lower() not in page_title.lower() else page_title
                        results.append({
                            "url": clean_url,
                            "title": title,
                            "resolution": res,
                            "source_page": page_url
                        })

            # 2. Inspect <video> and <audio> tags
            for v in soup.find_all(["video", "audio"]):
                src = v.get("src")
                if src:
                    abs_url = urljoin(page_url, src.strip())
                    if abs_url not in seen_urls and not is_preview_or_teaser(abs_url):
                        seen_urls.add(abs_url)
                        res = extract_resolution_hint(f"{v.get('width', '')}x{v.get('height', '')}", abs_url)
                        title = f"{page_title} [{res}]" if res and res.lower() not in page_title.lower() else page_title
                        item: Dict[str, Any] = {"url": abs_url, "title": title, "source_page": page_url}
                        if res:
                            item["resolution"] = res
                        results.append(item)

            # 3. Inspect <source> tags
            for src_tag in soup.find_all("source"):
                src = src_tag.get("src")
                src_type = src_tag.get("type", "").lower()
                if src:
                    abs_url = urljoin(page_url, src.strip())
                    if (
                        abs_url not in seen_urls
                        and not is_preview_or_teaser(abs_url)
                        and (self.is_video_url(abs_url) or "video" in src_type or "m3u8" in abs_url)
                    ):
                        seen_urls.add(abs_url)
                        res = extract_resolution_hint(f"{src_tag.get('size', '')} {src_tag.get('label', '')}", abs_url)
                        title = f"{page_title} [{res}]" if res and res.lower() not in page_title.lower() else page_title
                        item = {"url": abs_url, "title": title, "source_page": page_url}
                        if res:
                            item["resolution"] = res
                        results.append(item)

            # 4. Inspect data-* attributes (e.g. data-src, data-video-url, data-hls, data-m3u8, data-file)
            data_attrs = [
                "data-src", "data-video", "data-video-url", "data-stream", "data-hls",
                "data-m3u8", "data-mp4", "data-file", "data-url", "data-stream-url"
            ]
            for elem in soup.find_all(attrs={attr: True for attr in data_attrs}):
                for attr in data_attrs:
                    val = elem.get(attr)
                    if val and isinstance(val, str):
                        val = val.strip()
                        abs_url = urljoin(page_url, val)
                        if (
                            (self.is_video_url(abs_url) or "m3u8" in abs_url)
                            and abs_url not in seen_urls
                            and not is_preview_or_teaser(abs_url)
                        ):
                            seen_urls.add(abs_url)
                            res = extract_resolution_hint(elem.get("data-quality") or elem.get("data-res") or "", abs_url)
                            title = f"{page_title} [{res}]" if res and res.lower() not in page_title.lower() else page_title
                            item = {"url": abs_url, "title": title, "source_page": page_url}
                            if res:
                                item["resolution"] = res
                            results.append(item)

            # 5. Regex search for direct M3U8, MP4, WEBM, MKV links in HTML / JS code
            regex_streams = re.findall(
                r'(https?://[^\s"\'<>]+\.(?:m3u8|mp4|webm|mkv|mov|ts)(?:\?[^\s"\'<>]*)?)',
                html_text,
                re.IGNORECASE
            )
            for stream_url in regex_streams:
                clean_url = stream_url.replace("\\/", "/")
                if clean_url not in seen_urls and not is_preview_or_teaser(clean_url):
                    seen_urls.add(clean_url)
                    res = extract_resolution_hint("", clean_url)
                    title = f"{page_title} [{res}]" if res and res.lower() not in page_title.lower() else page_title
                    item = {"url": clean_url, "title": title, "source_page": page_url}
                    if res:
                        item["resolution"] = res
                    results.append(item)

            # 6. Extract JavaScript player configurations (JWPlayer, VideoJS, HLS.js, sources arrays)
            js_sources = re.findall(
                r'(?:file|source|src|hls|streamUrl|videoUrl)\s*:\s*["\'](https?://[^"\']+\.(?:m3u8|mp4|webm)[^"\']*)["\']',
                html_text,
                re.IGNORECASE
            )
            for js_url in js_sources:
                clean_url = js_url.replace("\\/", "/")
                if clean_url not in seen_urls and not is_preview_or_teaser(clean_url):
                    seen_urls.add(clean_url)
                    res = extract_resolution_hint("", clean_url)
                    title = f"{page_title} [{res}]" if res and res.lower() not in page_title.lower() else page_title
                    item = {"url": clean_url, "title": title, "source_page": page_url}
                    if res:
                        item["resolution"] = res
                    results.append(item)

            # 7. Optionally inspect M3U8 master playlists for variant streams
            if inspect_m3u8_variants:
                m3u8_items = [r for r in results if ".m3u8" in r["url"]]
                for m3u8_item in m3u8_items:
                    variants = await self.inspect_m3u8(session, m3u8_item["url"])
                    for var in variants:
                        var_url = var["url"]
                        if var_url not in seen_urls and not is_preview_or_teaser(var_url):
                            seen_urls.add(var_url)
                            var_res = var.get("quality") or var.get("resolution")
                            v_title = f"{page_title} [{var_res}]" if var_res else page_title
                            results.append({
                                "url": var_url,
                                "title": v_title,
                                "resolution": var_res,
                                "source_page": page_url
                            })

        finally:
            if close_session:
                await session.close()

        return results


PREVIEW_KEYWORDS = (
    'preview', 'trailer', 'teaser', 'hover', 'sample', 'thumb_preview',
    'short_clip', 'mouseover', '_preview', '-preview', '_sample', '-sample',
    'preview.mp4', 'hover.mp4', 'trailer.mp4', 'preview.m3u8',
    'fox-images', '/images/videos/', '/thumbs/videos/', '/previews/'
)


def is_preview_or_teaser(url: str, text: str = "") -> bool:
    """
    Detects if a media URL or HTML element text represents a short 3-5s teaser/hover preview.
    Returns True if preview/trailer/sample is detected, False otherwise.
    """
    combined = f"{url} {text}".lower()
    for kw in PREVIEW_KEYWORDS:
        if kw in combined:
            return True
    # Mini-clip / hover teaser video file pattern (e.g. /m-athena-faris.mp4)
    if re.search(r'/m-[a-zA-Z0-9_-]+\.(?:mp4|webm|m3u8)', url, re.I):
        return True
    # Substring check for teaser delimiters
    if re.search(r'(?:[_-]preview|[_-]trailer|[_-]teaser|[_-]sample|[_-]hover)\b', combined, re.I):
        return True
    return False


DETAIL_LINK_REGEX = re.compile(
    r'/(?:video|watch|v|view|post|media|play|item|detail|movie|clip|embed)/[^\s"\'<>]+',
    re.IGNORECASE
)


class DeepDetailCrawler(BaseCrawler):
    """
    Intelligent Deep Video Crawler:
    1. Scans listing/category/search/gallery pages for individual video watch/detail page links.
    2. Follows each watch page to extract the REAL full-length video stream (or provides the watch URL for yt-dlp).
    3. Strictly ignores 3-5s hover previews and teaser trailers.
    4. Exactly caps output at max_videos (e.g., 30 videos).
    """

    async def crawl(
        self,
        target_url: str,
        max_videos: int = 30,
        max_pages: int = 10,
        session: Optional[aiohttp.ClientSession] = None
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        seen_urls: Set[str] = set()
        seen_detail_urls: Set[str] = set()
        close_session = False

        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        try:
            logger.info(f"DeepDetailCrawler started for {target_url} (target: {max_videos} full videos, max_pages: {max_pages})")
            html_extractor = HTML5Extractor(headers=self.custom_headers, delay=self.delay, jitter=self.jitter)

            for page in range(1, max_pages + 1):
                if len(results) >= max_videos:
                    break

                page_url = target_url
                if page > 1:
                    delimiter = "&" if "?" in target_url else "?"
                    page_url = f"{target_url}{delimiter}page={page}"

                logger.info(f"Scanning listing page {page}: {page_url}")
                html_text = await self.fetch_text(session, page_url)
                if not html_text:
                    break

                soup = BeautifulSoup(html_text, "html.parser")
                base_domain = urlparse(target_url).netloc

                # Extract candidate watch/detail links
                detail_links: List[Tuple[str, str, Optional[str]]] = []
                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"].strip()
                    if not href or href.startswith(("#", "javascript:", "mailto:")):
                        continue

                    full_link = urljoin(page_url, href)
                    parsed_link = urlparse(full_link)

                    # Only follow links on the same domain or known video embeds
                    if parsed_link.netloc and parsed_link.netloc != base_domain and "embed" not in full_link:
                        continue

                    # Filter out static files or obvious non-detail pages
                    if full_link.lower().endswith(('.jpg', '.png', '.css', '.js', '.svg', '.webp')):
                        continue

                    # Check if href or text looks like a video detail/watch page
                    is_detail = bool(DETAIL_LINK_REGEX.search(parsed_link.path)) or any(
                        p in full_link.lower() for p in ['/video/', '/watch', '/v/', '/view/', '/post/', '/item/', '/play/']
                    )

                    # Also check parent container classes
                    parent = a_tag.find_parent()
                    parent_classes = " ".join(parent.get("class", [])) if parent and parent.get("class") else ""
                    if any(c in parent_classes.lower() for c in ["video", "item", "card", "thumb", "movie"]):
                        is_detail = True

                    if is_detail:
                        canonical_detail = normalize_media_url(full_link)
                        if canonical_detail not in seen_detail_urls and canonical_detail != normalize_media_url(target_url):
                            seen_detail_urls.add(canonical_detail)

                            # Extract title
                            title = a_tag.get_text(strip=True) or a_tag.get("title")
                            img = a_tag.find("img")
                            if not title and img:
                                title = img.get("alt") or img.get("title")
                            if not title:
                                title = full_link.rstrip("/").split("/")[-1].replace("-", " ").replace("_", " ")

                            # Clean out leading stats (e.g. '15 min57K78%Title...')
                            dur_match = re.match(r'^(\d+\s*(?:min|sec|m|s)?)\s*\d+[KkMmBb]?\s*\d+%\s*(.*)', title, re.IGNORECASE)
                            if dur_match:
                                dur_str = dur_match.group(1).strip()
                                raw_title = dur_match.group(2).strip()
                                title = f"{raw_title} [{dur_str}]"

                            thumb = None
                            if img:
                                thumb = img.get("src") or img.get("data-src")
                                if thumb:
                                    thumb = urljoin(page_url, thumb)

                            detail_links.append((full_link, title, thumb))

                logger.info(f"Page {page}: Discovered {len(detail_links)} candidate video detail pages.")

                if not detail_links and page == 1:
                    # If page 1 has no separate detail links, inspect page directly for full player stream
                    logger.info("No separate detail links found; inspecting single page for full player stream...")
                    single_streams = await html_extractor.extract(target_url, session=session)
                    for stream in single_streams:
                        if not is_preview_or_teaser(stream["url"]):
                            results.append(stream)
                            if len(results) >= max_videos:
                                break
                    break

                # Visit each detail page to extract the real full video stream
                for detail_url, detail_title, thumb in detail_links:
                    if len(results) >= max_videos:
                        logger.info(f"Reached target count of {max_videos} videos. Stopping deep crawl.")
                        break

                    logger.info(f"Inspecting detail page #{len(results)+1}/{max_videos}: {detail_url}")
                    detail_streams = await html_extractor.extract(detail_url, session=session)

                    # Filter out any preview/teaser clips
                    real_streams = [
                        s for s in detail_streams 
                        if not is_preview_or_teaser(s["url"], s.get("title", ""))
                    ]

                    if real_streams:
                        # Pick best quality stream
                        best_stream = real_streams[0]
                        stream_url = best_stream["url"]
                        if stream_url not in seen_urls:
                            seen_urls.add(stream_url)
                            res = best_stream.get("resolution") or extract_resolution_hint(detail_title, stream_url)
                            clean_title = detail_title
                            if res and res.lower() not in clean_title.lower():
                                clean_title = f"{clean_title} [{res}]"

                            item: Dict[str, Any] = {
                                "url": stream_url,
                                "title": clean_title,
                                "source_page": detail_url
                            }
                            if res:
                                item["resolution"] = res
                            if thumb:
                                item["thumbnail"] = thumb
                            results.append(item)
                            logger.info(f"Extracted FULL video #{len(results)}: {clean_title} -> {stream_url[:80]}...")
                    else:
                        # Fallback: pass the detail page URL itself for yt-dlp to extract
                        if detail_url not in seen_urls:
                            seen_urls.add(detail_url)
                            results.append({
                                "url": detail_url,
                                "title": detail_title,
                                "source_page": detail_url,
                                "thumbnail": thumb
                            })
                            logger.info(f"Queued detail watch page #{len(results)} for yt-dlp: {detail_title} ({detail_url})")

        finally:
            if close_session:
                await session.close()

        logger.info(f"DeepDetailCrawler finished: Collected {len(results)} full video items (requested: {max_videos}).")
        return results[:max_videos]


class UniversalCrawler:
    """
    Unified entry point for scraping media links across all supported strategies.
    Supports:
    - 'deep' / 'watch' / 'full': Deep crawl following video links to extract full videos (skipping previews)
    - 'rss' / 'atom' / 'feed': Parse RSS/Atom video feeds
    - 'sitemap': Parse XML sitemaps and video sitemaps
    - 'pagination': Crawl paginated index pages
    - 'html5': Parse embedded HTML5 media & direct M3U8/MP4 streams
    - 'auto': Intelligently detect content type and run matching crawler strategies with deep crawl fallback
    """

    def __init__(
        self,
        headers: Optional[Dict[str, str]] = None,
        delay: Optional[float] = None,
        jitter: Optional[float] = None
    ):
        self.rss_crawler = RSSFeedCrawler(headers=headers, delay=delay, jitter=jitter)
        self.sitemap_crawler = SitemapCrawler(headers=headers, delay=delay, jitter=jitter)
        self.pagination_crawler = PaginationCrawler(headers=headers, delay=delay, jitter=jitter)
        self.html5_extractor = HTML5Extractor(headers=headers, delay=delay, jitter=jitter)
        self.deep_crawler = DeepDetailCrawler(headers=headers, delay=delay, jitter=jitter)

    async def discover(
        self,
        target_url: str,
        mode: str = "auto",
        max_pages: int = 10,
        max_videos: int = 30,
        session: Optional[aiohttp.ClientSession] = None
    ) -> List[Dict[str, Any]]:
        """
        Discovers full video items from target URL, skipping 3-5s previews and stopping at max_videos.
        """
        target_url = target_url.strip()
        mode = mode.strip().lower()
        logger.info(f"Starting discovery for target: {target_url} (mode: {mode}, max_videos: {max_videos}, max_pages: {max_pages})")

        close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        try:
            # 1. Explicit Deep / Full Video mode
            if mode in ("deep", "watch", "full"):
                return await self.deep_crawler.crawl(target_url, max_videos=max_videos, max_pages=max_pages, session=session)

            # 2. Explicit RSS / Atom / Feed mode
            if mode in ("rss", "atom", "feed"):
                items = await self.rss_crawler.crawl(target_url, session=session)
                return items[:max_videos]

            # 3. Explicit Sitemap mode
            if mode == "sitemap":
                items = await self.sitemap_crawler.crawl(target_url, session=session)
                return items[:max_videos]

            # 4. Explicit Pagination mode
            if mode == "pagination":
                items = await self.pagination_crawler.crawl(target_url, max_pages=max_pages, session=session)
                return items[:max_videos]

            # 5. Explicit HTML5 mode
            if mode == "html5":
                items = await self.html5_extractor.extract(target_url, session=session)
                return [s for s in items if not is_preview_or_teaser(s["url"])][:max_videos]

            # 6. AUTO mode: Intelligent detection with deep full video extraction
            return await self._discover_auto(target_url, max_pages=max_pages, max_videos=max_videos, session=session)

        finally:
            if close_session:
                await session.close()

    async def _discover_auto(
        self,
        target_url: str,
        max_pages: int = 10,
        max_videos: int = 30,
        session: Optional[aiohttp.ClientSession] = None
    ) -> List[Dict[str, Any]]:
        """
        Automatic multi-strategy discovery:
        1. Checks URL patterns for RSS / Atom / Sitemap.
        2. Inspects content for XML schemas (RSS, Atom, Sitemap).
        3. If HTML gallery/listing page, runs DeepDetailCrawler to extract REAL full-length videos.
        4. Detects RSS/Atom <link rel="alternate"> in HTML head.
        5. Enforces max_videos limit (e.g. 30).
        """
        lower_url = target_url.lower()

        # Step A: Check if URL clearly points to a sitemap
        if "sitemap" in lower_url and lower_url.endswith((".xml", ".xml.gz")):
            sitemap_items = await self.sitemap_crawler.crawl(target_url, session=session)
            if sitemap_items:
                return sitemap_items[:max_videos]

        # Step B: Check if URL clearly points to an RSS / Atom feed
        if lower_url.endswith((".rss", ".atom", "/feed", "/rss", "/feed.xml", "/rss.xml", "/atom.xml")):
            feed_items = await self.rss_crawler.crawl(target_url, session=session)
            if feed_items:
                return feed_items[:max_videos]

        # Step C: Fetch initial content to inspect headers and body structure
        content = await self.html5_extractor.fetch_text(session, target_url)
        if not content:
            logger.warning(f"Could not retrieve content for auto discovery from: {target_url}")
            return []

        trimmed = content.lstrip()[:1000].lower()

        # XML detection: RSS / Atom vs Sitemap
        if "<?xml" in trimmed or "<rss" in trimmed or "<feed" in trimmed or "<urlset" in trimmed or "<sitemapindex" in trimmed:
            if "<urlset" in trimmed or "<sitemapindex" in trimmed:
                logger.info("Auto-detected XML Sitemap content.")
                items = await self.sitemap_crawler.crawl(target_url, session=session)
                return items[:max_videos]
            elif "<rss" in trimmed or "<feed" in trimmed or "<channel" in trimmed:
                logger.info("Auto-detected RSS/Atom XML feed content.")
                items = self.rss_crawler.parse_feed_content(content, target_url)
                return items[:max_videos]

        # Step D: Deep Video Detail Crawl (Default for all video websites & galleries)
        logger.info(f"Running DeepDetailCrawler to discover and extract {max_videos} full-length videos from {target_url}...")
        deep_items = await self.deep_crawler.crawl(target_url, max_videos=max_videos, max_pages=max_pages, session=session)
        if deep_items:
            logger.info(f"DeepDetailCrawler successfully collected {len(deep_items)} full video items.")
            return deep_items[:max_videos]

        # Step E: Check if the HTML page advertises an alternate RSS/Atom feed in <head>
        soup = BeautifulSoup(content, "html.parser")
        feed_links = self.html5_extractor.extract_feed_links(soup, target_url)
        for feed_link in feed_links:
            logger.info(f"Auto-discovered alternate feed in HTML: {feed_link}")
            feed_items = await self.rss_crawler.crawl(feed_link, session=session)
            if feed_items:
                return feed_items[:max_videos]

        # Step F: Direct single-page HTML5 stream fallback (excluding previews)
        html_items = await self.html5_extractor.extract(target_url, session=session)
        real_html_items = [s for s in html_items if not is_preview_or_teaser(s["url"])]
        if real_html_items:
            logger.info(f"Discovered {len(real_html_items)} items via direct HTML5 extraction.")
            return real_html_items[:max_videos]

        # Step G: Pagination crawler fallback
        pagination_items = await self.pagination_crawler.crawl(target_url, max_pages=max_pages, session=session)
        if pagination_items:
            logger.info(f"Discovered {len(pagination_items)} items via pagination crawler fallback.")
            return pagination_items[:max_videos]

        return []

