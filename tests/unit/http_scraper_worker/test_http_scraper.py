"""
Unit tests for HTTP Scraper Worker.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
import httpx
from bs4 import BeautifulSoup

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'services', 'http-scraper-worker'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'services', 'worker-shared'))

from app.services.http_scraper import HTTPScraper
from scraper_lib.models import Task, TaskStatus, ScrapingResult


class TestHTTPScraper:
    """Test cases for HTTP Scraper."""
    
    @pytest.fixture
    def mock_db_session(self):
        """Mock database session."""
        return AsyncMock()
    
    @pytest.fixture
    def mock_queue_client(self):
        """Mock queue client."""
        return Mock()
    
    @pytest.fixture
    def mock_s3_client(self):
        """Mock S3 client."""
        return AsyncMock()
    
    @pytest.fixture
    def mock_metrics(self):
        """Mock metrics collector."""
        return Mock()
    
    @pytest.fixture
    def http_scraper(self, mock_db_session, mock_queue_client, mock_s3_client, mock_metrics):
        """Create HTTPScraper instance with mocked dependencies."""
        return HTTPScraper(
            db_session=mock_db_session,
            queue_client=mock_queue_client,
            s3_client=mock_s3_client,
            metrics=mock_metrics
        )
    
    @pytest.mark.asyncio
    async def test_scrape_url_success(self, http_scraper):
        """Test successful URL scraping."""
        # Setup
        url = "https://example.com/test-page"
        html_content = """
        <html>
            <head><title>Test Page</title></head>
            <body>
                <h1>Welcome</h1>
                <p>This is a test page with some content.</p>
                <a href="/link1">Link 1</a>
                <a href="/link2">Link 2</a>
            </body>
        </html>
        """
        
        # Mock HTTP response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.text = html_content
        mock_response.elapsed.total_seconds.return_value = 0.5
        
        with patch.object(http_scraper.http_client, 'get', return_value=mock_response):
            # Execute
            result = await http_scraper._scrape_url(url)
            
            # Verify
            assert result.url == url
            assert result.status_code == 200
            assert result.success is True
            assert "Test Page" in result.title
            assert "Welcome" in result.content
            assert len(result.links) == 2
            assert result.response_time == 0.5
            assert result.quality_score > 0
    
    @pytest.mark.asyncio
    async def test_scrape_url_http_error(self, http_scraper):
        """Test handling of HTTP errors."""
        # Setup
        url = "https://example.com/not-found"
        
        # Mock HTTP error response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_response.elapsed.total_seconds.return_value = 0.2
        
        with patch.object(http_scraper.http_client, 'get', return_value=mock_response):
            # Execute
            result = await http_scraper._scrape_url(url)
            
            # Verify
            assert result.url == url
            assert result.status_code == 404
            assert result.success is False
            assert result.error_message == "HTTP 404"
            assert result.quality_score == 0
    
    @pytest.mark.asyncio
    async def test_scrape_url_network_error(self, http_scraper):
        """Test handling of network errors."""
        # Setup
        url = "https://unreachable.example.com"
        
        # Mock network error
        with patch.object(http_scraper.http_client, 'get', side_effect=httpx.ConnectError("Connection failed")):
            # Execute
            result = await http_scraper._scrape_url(url)
            
            # Verify
            assert result.url == url
            assert result.success is False
            assert "Connection failed" in result.error_message
            assert result.quality_score == 0
    
    @pytest.mark.asyncio
    async def test_extract_content_basic_html(self, http_scraper):
        """Test content extraction from basic HTML."""
        # Setup
        html = """
        <html>
            <head>
                <title>Test Document</title>
                <meta name="description" content="Test description">
            </head>
            <body>
                <h1>Main Heading</h1>
                <p>First paragraph with some text.</p>
                <p>Second paragraph with more content.</p>
                <div>Some div content</div>
                <a href="https://example.com/link1">External Link</a>
                <a href="/internal">Internal Link</a>
            </body>
        </html>
        """
        
        # Execute
        soup = BeautifulSoup(html, 'html.parser')
        title, content, links = http_scraper._extract_content(soup, "https://example.com")
        
        # Verify
        assert title == "Test Document"
        assert "Main Heading" in content
        assert "First paragraph" in content
        assert "Second paragraph" in content
        assert len(links) == 2
        assert "https://example.com/link1" in links
        assert "https://example.com/internal" in links
    
    @pytest.mark.asyncio
    async def test_extract_content_no_title(self, http_scraper):
        """Test content extraction when title is missing."""
        # Setup
        html = """
        <html>
            <body>
                <h1>Page without title tag</h1>
                <p>Some content here.</p>
            </body>
        </html>
        """
        
        # Execute
        soup = BeautifulSoup(html, 'html.parser')
        title, content, links = http_scraper._extract_content(soup, "https://example.com")
        
        # Verify
        assert title == "Page without title tag"  # Should fall back to h1
        assert "Some content here" in content
    
    @pytest.mark.asyncio
    async def test_calculate_quality_score(self, http_scraper):
        """Test quality score calculation."""
        # Test high quality content
        high_quality_result = Mock()
        high_quality_result.title = "Comprehensive Guide to Python Programming"
        high_quality_result.content = "This is a detailed article about Python programming with lots of useful information. " * 50
        high_quality_result.links = ["https://example.com/link" + str(i) for i in range(10)]
        high_quality_result.success = True
        high_quality_result.status_code = 200
        
        score = http_scraper._calculate_quality_score(high_quality_result)
        assert score > 0.7  # Should be high quality
        
        # Test low quality content
        low_quality_result = Mock()
        low_quality_result.title = ""
        low_quality_result.content = "Short"
        low_quality_result.links = []
        low_quality_result.success = True
        low_quality_result.status_code = 200
        
        score = http_scraper._calculate_quality_score(low_quality_result)
        assert score < 0.5  # Should be low quality
        
        # Test failed request
        failed_result = Mock()
        failed_result.success = False
        
        score = http_scraper._calculate_quality_score(failed_result)
        assert score == 0  # Failed requests get 0 score
    
    @pytest.mark.asyncio
    async def test_store_result_success(self, http_scraper, mock_s3_client):
        """Test storing scraping result."""
        # Setup
        result = Mock()
        result.url = "https://example.com/test"
        result.content = "Test content"
        result.to_dict.return_value = {
            "url": "https://example.com/test",
            "content": "Test content",
            "success": True
        }
        
        # Execute
        s3_key = await http_scraper._store_result(result)
        
        # Verify
        assert s3_key.startswith("raw-data/")
        assert s3_key.endswith(".json")
        mock_s3_client.put_object.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_task_status(self, http_scraper, mock_db_session):
        """Test updating task status in database."""
        # Setup
        task_id = "test-task-123"
        result = Mock()
        result.success = True
        result.error_message = None
        
        # Mock task query
        mock_task = Mock(spec=Task)
        mock_db_session.execute.return_value.scalar_one_or_none.return_value = mock_task
        
        # Execute
        await http_scraper._update_task_status(task_id, result, "s3://bucket/key")
        
        # Verify
        assert mock_task.status == TaskStatus.COMPLETED
        assert mock_task.result_location == "s3://bucket/key"
        assert mock_task.error_message is None
        mock_db_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_task_status_failure(self, http_scraper, mock_db_session):
        """Test updating task status for failed scraping."""
        # Setup
        task_id = "test-task-123"
        result = Mock()
        result.success = False
        result.error_message = "Network timeout"
        
        # Mock task query
        mock_task = Mock(spec=Task)
        mock_db_session.execute.return_value.scalar_one_or_none.return_value = mock_task
        
        # Execute
        await http_scraper._update_task_status(task_id, result, None)
        
        # Verify
        assert mock_task.status == TaskStatus.FAILED
        assert mock_task.error_message == "Network timeout"
        assert mock_task.result_location is None
        mock_db_session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_process_task_complete_workflow(self, http_scraper, mock_db_session, mock_queue_client, mock_s3_client):
        """Test complete task processing workflow."""
        # Setup
        task_data = {
            "task_id": "test-task-123",
            "url": "https://example.com/test",
            "job_id": "test-job-456"
        }
        
        # Mock successful scraping
        html_content = "<html><head><title>Test</title></head><body><p>Content</p></body></html>"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/html"}
        mock_response.text = html_content
        mock_response.elapsed.total_seconds.return_value = 0.3
        
        # Mock task query
        mock_task = Mock(spec=Task)
        mock_db_session.execute.return_value.scalar_one_or_none.return_value = mock_task
        
        with patch.object(http_scraper.http_client, 'get', return_value=mock_response):
            # Execute
            await http_scraper.process_task(task_data)
            
            # Verify scraping occurred
            http_scraper.http_client.get.assert_called_once_with("https://example.com/test")
            
            # Verify result stored
            mock_s3_client.put_object.assert_called_once()
            
            # Verify task updated
            assert mock_task.status == TaskStatus.COMPLETED
            mock_db_session.commit.assert_called()
            
            # Verify cleaning task enqueued
            mock_queue_client.enqueue.assert_called_once_with(
                "needs_cleaning",
                {
                    "task_id": "test-task-123",
                    "job_id": "test-job-456",
                    "result_location": mock_task.result_location
                }
            )
    
    @pytest.mark.asyncio
    async def test_retry_logic(self, http_scraper):
        """Test retry logic for failed requests."""
        # Setup
        url = "https://example.com/flaky"
        
        # Mock responses: fail, fail, succeed
        responses = [
            httpx.ConnectError("Connection failed"),
            httpx.ConnectError("Connection failed"),
            Mock(status_code=200, text="<html><body>Success</body></html>", 
                 headers={"content-type": "text/html"},
                 elapsed=Mock(total_seconds=lambda: 0.5))
        ]
        
        call_count = 0
        def mock_get(*args, **kwargs):
            nonlocal call_count
            response = responses[call_count]
            call_count += 1
            if isinstance(response, Exception):
                raise response
            return response
        
        with patch.object(http_scraper.http_client, 'get', side_effect=mock_get):
            # Execute
            result = await http_scraper._scrape_url_with_retry(url, max_retries=3)
            
            # Verify
            assert result.success is True
            assert result.status_code == 200
            assert call_count == 3  # Should have retried twice before succeeding
    
    @pytest.mark.asyncio
    async def test_proxy_rotation(self, http_scraper):
        """Test proxy rotation functionality."""
        # Setup
        http_scraper.proxy_list = [
            "http://proxy1:8080",
            "http://proxy2:8080",
            "http://proxy3:8080"
        ]
        
        # Execute multiple requests
        proxies_used = []
        for i in range(5):
            proxy = http_scraper._get_next_proxy()
            proxies_used.append(proxy)
        
        # Verify round-robin rotation
        assert proxies_used[0] == "http://proxy1:8080"
        assert proxies_used[1] == "http://proxy2:8080" 
        assert proxies_used[2] == "http://proxy3:8080"
        assert proxies_used[3] == "http://proxy1:8080"  # Should wrap around
        assert proxies_used[4] == "http://proxy2:8080"
    
    @pytest.mark.asyncio
    async def test_user_agent_rotation(self, http_scraper):
        """Test user agent rotation."""
        # Execute multiple requests
        user_agents = []
        for i in range(10):
            ua = http_scraper._get_random_user_agent()
            user_agents.append(ua)
        
        # Verify different user agents used
        unique_agents = set(user_agents)
        assert len(unique_agents) > 1  # Should use different user agents
        
        # Verify all are valid user agent strings
        for ua in user_agents:
            assert "Mozilla" in ua  # Common in all browser user agents
    
    @pytest.mark.asyncio
    async def test_robots_txt_compliance(self, http_scraper):
        """Test robots.txt compliance checking."""
        # Setup
        robots_content = """
        User-agent: *
        Disallow: /admin/
        Disallow: /private/
        Allow: /public/
        """
        
        with patch('urllib.robotparser.RobotFileParser') as mock_rp:
            mock_parser = Mock()
            mock_parser.can_fetch.side_effect = lambda ua, url: "/admin/" not in url and "/private/" not in url
            mock_rp.return_value = mock_parser
            
            # Test allowed URLs
            assert await http_scraper._can_fetch("https://example.com/public/page") is True
            assert await http_scraper._can_fetch("https://example.com/index.html") is True
            
            # Test disallowed URLs
            assert await http_scraper._can_fetch("https://example.com/admin/users") is False
            assert await http_scraper._can_fetch("https://example.com/private/data") is False