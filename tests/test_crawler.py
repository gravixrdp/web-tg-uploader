import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from modules.crawler import (
    BaseCrawler,
    RSSFeedCrawler,
    SitemapCrawler,
    PaginationCrawler,
    HTML5Extractor,
    UniversalCrawler,
    extract_resolution_hint,
    parse_m3u8_manifest,
    USER_AGENTS
)


# --- 1. Resolution Hint Extraction Tests ---

def test_extract_resolution_hint_dimensions():
    assert extract_resolution_hint("Sample Video", "https://cdn.example.com/video_1920x1080.mp4") == "1080p"
    assert extract_resolution_hint("Sample 1280x720 stream", "https://cdn.example.com/stream.m3u8") == "720p"
    assert extract_resolution_hint("4K Ultra HD 3840x2160", "") == "4K"
    assert extract_resolution_hint("Low res 640x360", "") == "360p"
    assert extract_resolution_hint("SD 854x480", "") == "480p"


def test_extract_resolution_hint_keywords():
    assert extract_resolution_hint("Movie 1080p BluRay", "") == "1080p"
    assert extract_resolution_hint("Action Trailer 720p", "") == "720p"
    assert extract_resolution_hint("Nature Doc 4K UHD", "") == "4K"
    assert extract_resolution_hint("Music Video 2K QHD", "") == "1440p"
    assert extract_resolution_hint("Podcast Episode", "https://example.com/audio.mp3") is None


# --- 2. M3U8 Master Playlist Parser Tests ---

def test_parse_m3u8_manifest():
    manifest_content = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360,NAME="360p"
360p/stream.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=1400000,RESOLUTION=1280x720,NAME="720p",CODECS="avc1.4d401f,mp4a.40.2"
720p/stream.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1920x1080,NAME="1080p",CODECS="avc1.640028,mp4a.40.2"
1080p/stream.m3u8
"""
    base_url = "https://cdn.example.com/hls/master.m3u8"
    variants = parse_m3u8_manifest(manifest_content, base_url)

    assert len(variants) == 3
    assert variants[0]["resolution"] == "640x360"
    assert variants[0]["quality"] == "360p"
    assert variants[0]["url"] == "https://cdn.example.com/hls/360p/stream.m3u8"
    assert variants[0]["bandwidth"] == 800000

    assert variants[1]["resolution"] == "1280x720"
    assert variants[1]["quality"] == "720p"
    assert variants[1]["url"] == "https://cdn.example.com/hls/720p/stream.m3u8"

    assert variants[2]["resolution"] == "1920x1080"
    assert variants[2]["quality"] == "1080p"
    assert variants[2]["url"] == "https://cdn.example.com/hls/1080p/stream.m3u8"


# --- 3. BaseCrawler Header Rotation & Request Delays ---

def test_base_crawler_headers_and_ua_rotation():
    crawler = BaseCrawler(headers={"X-Custom-Header": "TestVal"}, delay=0.1, jitter=0.05)
    headers1 = crawler.get_request_headers("https://example.com/videos")
    headers2 = crawler.get_request_headers("https://example.com/videos")

    assert "User-Agent" in headers1
    assert headers1["User-Agent"] in USER_AGENTS
    assert headers1["X-Custom-Header"] == "TestVal"
    assert headers1["Referer"] == "https://example.com"
    assert "Accept" in headers1


@pytest.mark.asyncio
async def test_base_crawler_delay():
    crawler = BaseCrawler(delay=0.05, jitter=0.01)
    start_time = asyncio.get_event_loop().time()
    await crawler._apply_delay("https://target-domain.com/1")
    await crawler._apply_delay("https://target-domain.com/2")
    elapsed = asyncio.get_event_loop().time() - start_time
    assert elapsed >= 0.04


# --- 4. RSSFeedCrawler Tests ---

def test_rss_feed_crawler_rss2_enclosure():
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Sample Video Channel</title>
    <link>https://example.com</link>
    <item>
      <title>Big Buck Bunny 1080p</title>
      <link>https://example.com/item1</link>
      <enclosure url="https://example.com/media/bbb_1080p.mp4" length="50000000" type="video/mp4"/>
    </item>
    <item>
      <title>Elephants Dream</title>
      <link>https://example.com/item2</link>
      <enclosure url="https://example.com/media/elephants_dream.m3u8" type="application/x-mpegURL"/>
    </item>
  </channel>
</rss>
"""
    crawler = RSSFeedCrawler()
    results = crawler.parse_feed_content(xml_content, "https://example.com/feed.xml")

    assert len(results) == 2
    assert results[0]["url"] == "https://example.com/media/bbb_1080p.mp4"
    assert "Big Buck Bunny 1080p" in results[0]["title"]
    assert results[0]["resolution"] == "1080p"

    assert results[1]["url"] == "https://example.com/media/elephants_dream.m3u8"


def test_rss_feed_crawler_media_rss():
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/">
  <channel>
    <title>Media RSS Video Feed</title>
    <item>
      <title>Nature Documentary 4K</title>
      <media:content url="https://cdn.example.com/nature.mp4" type="video/mp4" width="3840" height="2160" duration="600"/>
      <media:thumbnail url="https://cdn.example.com/nature_thumb.jpg"/>
    </item>
    <item>
      <title>Concert Stream</title>
      <media:content url="https://cdn.example.com/concert/master.m3u8" medium="video" width="1280" height="720"/>
    </item>
  </channel>
</rss>
"""
    crawler = RSSFeedCrawler()
    results = crawler.parse_feed_content(xml_content, "https://cdn.example.com/mrss.xml")

    assert len(results) == 2
    assert results[0]["url"] == "https://cdn.example.com/nature.mp4"
    assert results[0]["resolution"] == "4K"
    assert results[0]["thumbnail"] == "https://cdn.example.com/nature_thumb.jpg"

    assert results[1]["url"] == "https://cdn.example.com/concert/master.m3u8"
    assert results[1]["resolution"] == "720p"
    assert "[720p]" in results[1]["title"]


def test_rss_feed_crawler_atom():
    xml_content = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Atom Video Channel</title>
  <entry>
    <title>Episode 1 - The Beginning</title>
    <link rel="enclosure" type="video/mp4" href="https://example.com/atom/ep1_720p.mp4"/>
  </entry>
  <entry>
    <title>Episode 2 - The Return</title>
    <link rel="alternate" type="video/webm" href="https://example.com/atom/ep2_1080p.webm"/>
  </entry>
</feed>
"""
    crawler = RSSFeedCrawler()
    results = crawler.parse_feed_content(xml_content, "https://example.com/atom.xml")

    assert len(results) == 2
    assert results[0]["url"] == "https://example.com/atom/ep1_720p.mp4"
    assert results[0]["resolution"] == "720p"
    assert results[1]["url"] == "https://example.com/atom/ep2_1080p.webm"
    assert results[1]["resolution"] == "1080p"


# --- 5. HTML5Extractor & Stream Detection Tests ---

@pytest.mark.asyncio
async def test_html5_extractor_media_tags_and_data_attrs():
    html_doc = """<!DOCTYPE html>
<html>
<head><title>Streaming Video Hub</title></head>
<body>
  <video src="/videos/direct_movie_1080p.mp4" width="1920" height="1080"></video>
  <video>
    <source src="https://cdn.example.com/hls/live.m3u8" type="application/x-mpegURL" label="720p HD">
  </video>
  <div class="player" data-video-url="https://streams.example.com/clip_480p.mp4" data-quality="480p"></div>
  <script>
    var playerConfig = {
      file: "https://secure.example.com/manifest/master.m3u8",
      title: "Secure Stream"
    };
  </script>
</body>
</html>
"""
    extractor = HTML5Extractor()
    with patch.object(extractor, "fetch_text", new=AsyncMock(return_value=html_doc)):
        results = await extractor.extract("https://example.com/watch")

    urls = [r["url"] for r in results]
    assert "https://example.com/videos/direct_movie_1080p.mp4" in urls
    assert "https://cdn.example.com/hls/live.m3u8" in urls
    assert "https://streams.example.com/clip_480p.mp4" in urls
    assert "https://secure.example.com/manifest/master.m3u8" in urls


# --- 6. UniversalCrawler Discovery & Auto Mode Tests ---

@pytest.mark.asyncio
async def test_universal_crawler_rss_mode():
    rss_xml = """<rss version="2.0"><channel><item><title>Test Vid</title><enclosure url="https://ex.com/vid.mp4" type="video/mp4"/></item></channel></rss>"""
    uc = UniversalCrawler()
    with patch.object(uc.rss_crawler, "fetch_text", new=AsyncMock(return_value=rss_xml)):
        items = await uc.discover("https://example.com/feed.xml", mode="rss")
        assert len(items) == 1
        assert items[0]["url"] == "https://ex.com/vid.mp4"


@pytest.mark.asyncio
async def test_universal_crawler_auto_detects_rss():
    rss_xml = """<?xml version="1.0"?><rss version="2.0"><channel><item><title>Auto Vid</title><enclosure url="https://ex.com/autovid.mp4" type="video/mp4"/></item></channel></rss>"""
    uc = UniversalCrawler()
    with patch.object(uc.html5_extractor, "fetch_text", new=AsyncMock(return_value=rss_xml)):
        items = await uc.discover("https://example.com/feed", mode="auto")
        assert len(items) == 1
        assert items[0]["url"] == "https://ex.com/autovid.mp4"


@pytest.mark.asyncio
async def test_universal_crawler_auto_detects_html5_and_alternate_feed():
    html_doc = """<!DOCTYPE html>
<html>
<head>
  <title>My Video Blog</title>
  <link rel="alternate" type="application/rss+xml" href="/rss.xml" />
</head>
<body>
  <p>No direct video in body</p>
</body>
</html>
"""
    rss_xml = """<rss version="2.0"><channel><item><title>Linked Feed Video</title><enclosure url="https://ex.com/feedvideo.mp4" type="video/mp4"/></item></channel></rss>"""

    uc = UniversalCrawler()
    # First fetch for page HTML
    with patch.object(uc.html5_extractor, "fetch_text", new=AsyncMock(return_value=html_doc)):
        # Second fetch for discovered feed URL
        with patch.object(uc.rss_crawler, "fetch_text", new=AsyncMock(return_value=rss_xml)):
            items = await uc.discover("https://example.com/blog", mode="auto")
            assert len(items) == 1
            assert items[0]["url"] == "https://ex.com/feedvideo.mp4"


# --- 7. Sitemap & Video Sitemap Tests ---

@pytest.mark.asyncio
async def test_sitemap_crawler_video_sitemap():
    sitemap_xml = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:video="http://www.google.com/schemas/sitemap-video/1.1">
  <url>
    <loc>https://example.com/videos/page1.html</loc>
    <video:video>
      <video:thumbnail_loc>https://example.com/thumbs/1.jpg</video:thumbnail_loc>
      <video:title>Sitemap Video 1080p</video:title>
      <video:content_loc>https://cdn.example.com/video1_1080p.mp4</video:content_loc>
    </video:video>
  </url>
  <url>
    <loc>https://cdn.example.com/direct_video_720p.mp4</loc>
  </url>
</urlset>
"""
    crawler = SitemapCrawler()
    with patch.object(crawler, "fetch_text", new=AsyncMock(return_value=sitemap_xml)):
        results = await crawler.crawl("https://example.com/sitemap.xml")

    assert len(results) == 2
    assert results[0]["url"] == "https://cdn.example.com/video1_1080p.mp4"
    assert "Sitemap Video 1080p" in results[0]["title"]
    assert results[0]["thumbnail"] == "https://example.com/thumbs/1.jpg"
    assert results[1]["url"] == "https://cdn.example.com/direct_video_720p.mp4"


# --- 8. HTML5 M3U8 Variant Inspection Test ---

@pytest.mark.asyncio
async def test_html5_extractor_m3u8_variant_inspection():
    html_doc = """<html><body><video src="https://cdn.example.com/master.m3u8"></video></body></html>"""
    manifest_content = """#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1920x1080,NAME="1080p"
1080p/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=1280x720,NAME="720p"
720p/index.m3u8
"""
    extractor = HTML5Extractor()
    async def mock_fetch(session, url, **kwargs):
        if url == "https://example.com/watch":
            return html_doc
        elif url == "https://cdn.example.com/master.m3u8":
            return manifest_content
        return None

    with patch.object(extractor, "fetch_text", side_effect=mock_fetch):
        results = await extractor.extract("https://example.com/watch", inspect_m3u8_variants=True)

    urls = [r["url"] for r in results]
    assert "https://cdn.example.com/master.m3u8" in urls
    assert "https://cdn.example.com/1080p/index.m3u8" in urls
    assert "https://cdn.example.com/720p/index.m3u8" in urls

