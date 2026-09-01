import unittest
import os
import tempfile
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from modules.uploader import (
    TelegramBotUploader,
    TelegramErrorDetails,
    parse_telegram_error,
    format_file_size,
    format_duration,
    get_dynamic_chunk_size,
    async_file_chunk_streamer,
    SafeFormatter,
)

class TestUploader(unittest.IsolatedAsyncioTestCase):

    def test_format_file_size(self):
        self.assertEqual(format_file_size(0), '0 B')
        self.assertEqual(format_file_size(500), '500 B')
        self.assertEqual(format_file_size(1024), '1.00 KB')
        self.assertEqual(format_file_size(1048576), '1.00 MB')
        self.assertEqual(format_file_size(52428800), '50.00 MB')
        self.assertEqual(format_file_size(1073741824), '1.00 GB')

    def test_format_duration(self):
        self.assertEqual(format_duration(0), '00:00')
        self.assertEqual(format_duration(45), '00:45')
        self.assertEqual(format_duration(75), '01:15')
        self.assertEqual(format_duration(3665), '01:01:05')

    def test_dynamic_chunk_size(self):
        # < 10MB -> 64KB
        self.assertEqual(get_dynamic_chunk_size(5 * 1024 * 1024), 64 * 1024)
        # 10MB - 100MB -> 256KB
        self.assertEqual(get_dynamic_chunk_size(50 * 1024 * 1024), 256 * 1024)
        # 100MB - 500MB -> 1MB
        self.assertEqual(get_dynamic_chunk_size(200 * 1024 * 1024), 1024 * 1024)
        # > 500MB -> 4MB
        self.assertEqual(get_dynamic_chunk_size(800 * 1024 * 1024), 4 * 1024 * 1024)
        # Override
        self.assertEqual(get_dynamic_chunk_size(100, 128 * 1024), 128 * 1024)

    def test_caption_templating_and_custom_tags(self):
        uploader = TelegramBotUploader(bot_token='dummy:token', chat_id='12345')
        metadata = {
            'title': 'Test Movie',
            'file_size': 10485760,  # 10 MB
            'duration': 125,        # 02:05
            'width': 1920,
            'height': 1080,
            'video_id': 42,
            'video_url': 'https://example.com/video/42'
        }
        custom_tags = {
            'author': 'Alice',
            'quality': '1080p',
            'genre': 'Documentary'
        }
        template = 'Title: {title} | Size: {size} | Duration: {duration} | Quality: {quality} | Author: {author} | Resolution: {resolution} | ID: {id} | Missing: [{missing_key}]'

        caption = uploader.format_caption(
            file_path='video_42.mp4',
            title='Test Movie',
            metadata=metadata,
            custom_tags=custom_tags,
            template=template
        )
        self.assertIn('Title: Test Movie', caption)
        self.assertIn('Size: 10.00 MB', caption)
        self.assertIn('Duration: 02:05', caption)
        self.assertIn('Quality: 1080p', caption)
        self.assertIn('Author: Alice', caption)
        self.assertIn('Resolution: 1920x1080', caption)
        self.assertIn('ID: 42', caption)
        self.assertIn('Missing: []', caption)

    def test_caption_max_length_truncation(self):
        uploader = TelegramBotUploader(bot_token='dummy:token', chat_id='12345')
        long_title = 'A' * 2000
        caption = uploader.format_caption(
            file_path='test.mp4',
            title=long_title,
            max_length=1024
        )
        self.assertLessEqual(len(caption), 1024)
        self.assertTrue(caption.endswith('...'))

    def test_error_parsing_and_diagnostics(self):
        # 429 FloodWait
        err_429 = parse_telegram_error(429, 'Too Many Requests: retry after 25', {'retry_after': 25})
        self.assertEqual(err_429.status_code, 429)
        self.assertEqual(err_429.retry_after, 25)
        self.assertTrue(err_429.is_retryable)
        self.assertIn('FloodWait', err_429.friendly_name)

        # 400 format error
        err_format = parse_telegram_error(400, 'Bad Request: wrong file identifier/HTTP URL specified')
        self.assertTrue(err_format.is_format_error)
        self.assertIn('Video Format', err_format.friendly_name)

        # 401 unauthorized
        err_401 = parse_telegram_error(401, 'Unauthorized')
        self.assertEqual(err_401.status_code, 401)
        self.assertFalse(err_401.is_retryable)

        # 403 forbidden
        err_403 = parse_telegram_error(403, 'Forbidden: bot was kicked from the channel')
        self.assertIn('Kicked', err_403.friendly_name)

        # 413 file too large
        err_413 = parse_telegram_error(413, 'Request Entity Too Large')
        self.assertIn('50MB', err_413.friendly_name)

        # Check format_log produces readable box
        log_str = err_403.format_log('test_context')
        self.assertIn('TELEGRAM API ERROR', log_str)
        self.assertIn('Action Plan', log_str)

    async def test_async_stream_chunker(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(b'Hello World! Chunked async streaming test content.')
            tf_path = tf.name

        try:
            chunks = []
            async for chunk in async_file_chunk_streamer(tf_path, chunk_size=10):
                chunks.append(chunk)
            reconstructed = b''.join(chunks)
            self.assertEqual(reconstructed, b'Hello World! Chunked async streaming test content.')
            self.assertGreater(len(chunks), 1)
        finally:
            if os.path.exists(tf_path):
                os.remove(tf_path)

    async def test_upload_auto_fallback_to_send_document(self):
        uploader = TelegramBotUploader(bot_token='dummy:token', chat_id='12345', cooldown=0)
        
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as tf:
            tf.write(b'Mock video content')
            tf_path = tf.name

        try:
            call_endpoints = []

            async def mock_send_media_request(endpoint, field_name, file_path, chat_id, caption, metadata, parse_mode=None, max_retries=5):
                call_endpoints.append(endpoint)
                if endpoint == 'sendVideo':
                    # Simulate video format error
                    err_details = parse_telegram_error(400, 'Bad Request: wrong file identifier/HTTP URL specified')
                    return False, err_details
                elif endpoint == 'sendDocument':
                    # Document succeeds
                    return True, None
                return False, None

            with patch.object(uploader, '_send_media_request', side_effect=mock_send_media_request):
                success, error = await uploader.upload_video(file_path=tf_path, title='Sample')
                self.assertTrue(success)
                self.assertIsNone(error)
                self.assertEqual(call_endpoints, ['sendVideo', 'sendDocument'])
        finally:
            if os.path.exists(tf_path):
                os.remove(tf_path)

    async def test_upload_missing_chat_id_or_file(self):
        uploader = TelegramBotUploader(bot_token='dummy:token', chat_id='', cooldown=0)
        success, error = await uploader.upload_video(file_path='nonexistent.mp4')
        self.assertFalse(success)
        self.assertIn('Target chat ID is missing', error)

        uploader.chat_id = '12345'
        success, error = await uploader.upload_video(file_path='nonexistent_file_abc.mp4')
        self.assertFalse(success)
        self.assertIn('File does not exist', error)

    async def test_verify_bot_token_empty(self):
        uploader = TelegramBotUploader(bot_token='')
        ok, msg = await uploader.verify_bot_token()
        self.assertFalse(ok)
        self.assertIn('No TELEGRAM_BOT_TOKEN found', msg)

    async def test_verify_bot_token_mock_success(self):
        uploader = TelegramBotUploader(bot_token='123456:ABC-DEF')
        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value={
            'ok': True,
            'result': {'id': 123456, 'is_bot': True, 'first_name': 'MyBot', 'username': 'my_test_bot'}
        })
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch('aiohttp.ClientSession.get', return_value=mock_response):
            ok, username = await uploader.verify_bot_token()
            self.assertTrue(ok)
            self.assertEqual(username, 'my_test_bot')

    async def test_verify_bot_token_mock_failure(self):
        uploader = TelegramBotUploader(bot_token='123456:INVALID')
        mock_response = AsyncMock()
        mock_response.status = 401
        mock_response.json = AsyncMock(return_value={
            'ok': False,
            'error_code': 401,
            'description': 'Unauthorized'
        })
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch('aiohttp.ClientSession.get', return_value=mock_response):
            ok, msg = await uploader.verify_bot_token()
            self.assertFalse(ok)
            self.assertIn('Unauthorized', msg)

if __name__ == '__main__':
    unittest.main()
