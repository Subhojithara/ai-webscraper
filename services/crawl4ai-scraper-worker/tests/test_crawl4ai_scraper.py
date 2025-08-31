"""
Comprehensive unit tests for Crawl4AI Scraper Worker.
"""

import pytest
import asyncio
import json
import uuid
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timezone

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'worker-shared'))

from app.services.crawl4ai_scraper import Crawl4AIScraper
from scraper_lib.database import TaskStatus
from scraper_lib.cache import QueueMessage


@pytest.fixture
def mock_settings():
    """Mock settings fixture."""
    settings = Mock()
    settings.rate_limit_enabled = True
    settings.rate_limit_requests_per_second = 10.0
    settings.rate_limit_burst = 20
    settings.worker_retry_attempts = 3
    settings.queue_scrape_crawl4ai = "scrape_crawl4ai"
    settings.openai_api_key = "test-api-key"
    settings.s3_enabled = True
    return settings


@pytest.fixture
def mock_queue_manager():
    """Mock queue manager fixture."""
    return AsyncMock()


@pytest.fixture
def mock_metrics():
    """Mock metrics collector fixture."""
    metrics = Mock()
    metrics.scraping_requests_total = Mock()
    metrics.scraping_requests_total.labels = Mock(return_value=Mock())
    metrics.scraping_duration = Mock()
    metrics.scraping_duration.labels = Mock(return_value=Mock())
    metrics.scraping_data_size = Mock()
    metrics.scraping_data_size.labels = Mock(return_value=Mock())
    return metrics


@pytest.fixture
def crawl4ai_scraper(mock_settings):
    """Crawl4AI scraper fixture."""
    with patch('app.services.crawl4ai_scraper.get_settings', return_value=mock_settings), \
         patch('app.services.crawl4ai_scraper.get_queue_manager'), \
         patch('app.services.crawl4ai_scraper.get_metrics_collector'):
        scraper = Crawl4AIScraper()
        return scraper


@pytest.fixture
def sample_queue_message():
    """Sample queue message fixture."""
    return QueueMessage(
        id="test-message-id",
        data={
            "task_id": str(uuid.uuid4()),
            "job_id": str(uuid.uuid4()),
            "url": "https://example.com",
            "config": {
                "enable_ai_extraction": True,
                "extraction_strategy": "llm",
                "timeout": 30
            }
        },
        retry_count=0,
        max_retries=3
    )


@pytest.fixture
def mock_crawl4ai_result():
    """Mock Crawl4AI result fixture."""
    result = Mock()
    result.success = True
    result.url = "https://example.com"
    result.markdown = "# Sample Page\n\nThis is sample content."
    result.extracted_content = {"title": "Sample Page", "content": "This is sample content."}
    result.links = {"internal": ["https://example.com/page1"], "external": ["https://external.com"]}
    result.images = ["https://example.com/image.jpg"]
    result.metadata = {"title": "Sample Page", "description": "Sample description"}
    return result


class TestCrawl4AIScraper:
    """Test cases for Crawl4AI scraper."""
    
    def test_initialization(self, crawl4ai_scraper):
        """Test scraper initialization."""
        assert crawl4ai_scraper is not None
        assert crawl4ai_scraper.running is False
        assert hasattr(crawl4ai_scraper, 'rate_limiter')
        assert hasattr(crawl4ai_scraper, 'retry_handler')
        assert hasattr(crawl4ai_scraper, 'crawler_config')
    
    def test_default_user_agent(self, crawl4ai_scraper):
        """Test default user agent generation."""
        user_agent = crawl4ai_scraper._get_default_user_agent()
        assert "Mozilla/5.0" in user_agent
        assert "Chrome" in user_agent
    
    @pytest.mark.asyncio
    async def test_start_worker(self, crawl4ai_scraper):
        """Test worker start functionality."""
        with patch.object(crawl4ai_scraper, 'process_scraping_task') as mock_process:
            with patch('app.services.crawl4ai_scraper.dequeue_job') as mock_dequeue:
                # Setup mock to return None to stop the loop
                mock_dequeue.return_value = None
                
                # Start worker in background
                task = asyncio.create_task(crawl4ai_scraper.start_worker("test-worker"))
                
                # Let it run briefly
                await asyncio.sleep(0.1)
                
                # Stop worker
                crawl4ai_scraper.stop_worker()
                
                # Wait for task to complete
                try:
                    await asyncio.wait_for(task, timeout=1.0)
                except asyncio.TimeoutError:
                    task.cancel()
                
                assert crawl4ai_scraper.running is False
    
    @pytest.mark.asyncio
    async def test_scrape_url_with_crawl4ai_success(self, crawl4ai_scraper, mock_crawl4ai_result):
        """Test successful URL scraping with Crawl4AI."""
        url = "https://example.com"
        config = {"extraction_strategy": "markdown"}
        
        # Mock AsyncWebCrawler
        with patch('app.services.crawl4ai_scraper.AsyncWebCrawler') as mock_crawler_class:
            mock_crawler = AsyncMock()
            mock_crawler.__aenter__ = AsyncMock(return_value=mock_crawler)
            mock_crawler.__aexit__ = AsyncMock(return_value=None)
            mock_crawler.arun = AsyncMock(return_value=mock_crawl4ai_result)
            mock_crawler_class.return_value = mock_crawler
            
            result = await crawl4ai_scraper.scrape_url_with_crawl4ai(url, config, "test-worker")
            
            # Verify result
            assert result["success"] is True
            assert result["url"] == url
            assert result["markdown"] == "# Sample Page\n\nThis is sample content."
            assert result["quality_score"] > 0
            assert result["content_size"] > 0
            assert result["extraction_type"] == "markdown"
    
    @pytest.mark.asyncio
    async def test_scrape_url_with_llm_extraction(self, crawl4ai_scraper, mock_crawl4ai_result):
        """Test LLM-based extraction strategy."""
        url = "https://example.com"
        config = {
            "extraction_strategy": "llm",
            "llm_config": {
                "provider": "openai",
                "api_token": "test-token",
                "schema": {"fields": [{"name": "title", "type": "text"}]}
            }
        }
        
        with patch('app.services.crawl4ai_scraper.AsyncWebCrawler') as mock_crawler_class:
            with patch('app.services.crawl4ai_scraper.LLMExtractionStrategy') as mock_llm_strategy:
                mock_strategy_instance = Mock()
                mock_llm_strategy.return_value = mock_strategy_instance
                
                mock_crawler = AsyncMock()
                mock_crawler.__aenter__ = AsyncMock(return_value=mock_crawler)
                mock_crawler.__aexit__ = AsyncMock(return_value=None)
                mock_crawler.arun = AsyncMock(return_value=mock_crawl4ai_result)
                mock_crawler_class.return_value = mock_crawler
                
                result = await crawl4ai_scraper.scrape_url_with_crawl4ai(url, config, "test-worker")
                
                # Verify LLM strategy was created
                mock_llm_strategy.assert_called_once()
                assert result["success"] is True
                assert result["extraction_type"] == "llm"
    
    @pytest.mark.asyncio
    async def test_scrape_url_with_cosine_extraction(self, crawl4ai_scraper, mock_crawl4ai_result):
        """Test cosine similarity-based extraction strategy."""
        url = "https://example.com"
        config = {
            "extraction_strategy": "cosine",
            "semantic_filter": "Find articles about technology",
            "word_count_threshold": 10,
            "top_k": 3
        }
        
        with patch('app.services.crawl4ai_scraper.AsyncWebCrawler') as mock_crawler_class:
            with patch('app.services.crawl4ai_scraper.CosineStrategy') as mock_cosine_strategy:
                mock_strategy_instance = Mock()
                mock_cosine_strategy.return_value = mock_strategy_instance
                
                mock_crawler = AsyncMock()
                mock_crawler.__aenter__ = AsyncMock(return_value=mock_crawler)
                mock_crawler.__aexit__ = AsyncMock(return_value=None)
                mock_crawler.arun = AsyncMock(return_value=mock_crawl4ai_result)
                mock_crawler_class.return_value = mock_crawler
                
                result = await crawl4ai_scraper.scrape_url_with_crawl4ai(url, config, "test-worker")
                
                # Verify cosine strategy was created
                mock_cosine_strategy.assert_called_once()
                assert result["success"] is True
                assert result["extraction_type"] == "cosine"
    
    def test_calculate_quality_score(self, crawl4ai_scraper, mock_crawl4ai_result):
        """Test quality score calculation."""
        # Test successful result with good content
        score = crawl4ai_scraper._calculate_quality_score(mock_crawl4ai_result)
        assert 0.0 <= score <= 1.0
        assert score > 0.5  # Should be reasonably high for good content
        
        # Test failed result
        failed_result = Mock()
        failed_result.success = False
        failed_result.markdown = None
        
        score = crawl4ai_scraper._calculate_quality_score(failed_result)
        assert score == 0.0
        
        # Test minimal content
        minimal_result = Mock()
        minimal_result.success = True
        minimal_result.markdown = "Short content"
        minimal_result.extracted_content = None
        minimal_result.links = None
        minimal_result.images = None
        
        score = crawl4ai_scraper._calculate_quality_score(minimal_result)
        assert 0.0 <= score <= 1.0
        assert score < 0.5  # Should be lower for minimal content
    
    @pytest.mark.asyncio
    async def test_clean_extracted_data(self, crawl4ai_scraper):
        """Test data cleaning functionality."""
        with patch('app.services.crawl4ai_scraper.DataCleaner') as mock_cleaner_class:
            with patch('app.services.crawl4ai_scraper.URLValidator') as mock_validator_class:
                mock_cleaner = Mock()
                mock_cleaner.clean_text = Mock(side_effect=lambda x: x.strip())
                mock_cleaner_class.return_value = mock_cleaner
                
                mock_validator = Mock()
                mock_validator.is_valid_url = Mock(return_value=True)
                mock_validator_class.return_value = mock_validator
                
                data = {
                    "markdown": "  Some markdown content  ",
                    "extracted_content": "  Some extracted content  ",
                    "links": {
                        "internal": ["https://example.com/page1", "invalid-url"],
                        "external": ["https://external.com", "another-invalid-url"]
                    }
                }
                
                cleaned_data = await crawl4ai_scraper._clean_extracted_data(data)
                
                # Verify cleaning was applied
                mock_cleaner.clean_text.assert_called()
                assert cleaned_data["markdown"] == "Some markdown content"
                assert cleaned_data["extracted_content"] == "Some extracted content"
    
    @pytest.mark.asyncio
    async def test_process_scraping_task_success(self, crawl4ai_scraper, sample_queue_message, mock_crawl4ai_result):
        """Test successful task processing."""
        with patch.object(crawl4ai_scraper, 'update_task_status') as mock_update_status:
            with patch.object(crawl4ai_scraper, 'scrape_url_with_crawl4ai') as mock_scrape:
                with patch.object(crawl4ai_scraper, 'store_scraping_result') as mock_store:
                    with patch('app.services.crawl4ai_scraper.ack_job') as mock_ack:
                        
                        mock_scrape.return_value = {
                            "success": True,
                            "url": "https://example.com",
                            "markdown": "content",
                            "status_code": 200,
                            "result_path": "s3://bucket/result.json"
                        }
                        
                        await crawl4ai_scraper.process_scraping_task(sample_queue_message, "test-worker")
                        
                        # Verify task lifecycle
                        assert mock_update_status.call_count == 2  # Processing and Completed
                        mock_scrape.assert_called_once()
                        mock_store.assert_called_once()
                        mock_ack.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_process_scraping_task_failure(self, crawl4ai_scraper, sample_queue_message):
        """Test task processing failure handling."""
        with patch.object(crawl4ai_scraper, 'update_task_status') as mock_update_status:
            with patch.object(crawl4ai_scraper, 'scrape_url_with_crawl4ai') as mock_scrape:
                with patch('app.services.crawl4ai_scraper.ack_job') as mock_ack:
                    
                    # Make scraping fail
                    mock_scrape.side_effect = Exception("Scraping failed")
                    
                    await crawl4ai_scraper.process_scraping_task(sample_queue_message, "test-worker")
                    
                    # Verify failure handling
                    assert mock_update_status.call_count == 2  # Processing and Failed
                    mock_ack.assert_called_once()  # Should still ack to avoid infinite retries
    
    @pytest.mark.asyncio
    async def test_rate_limiting(self, crawl4ai_scraper):
        """Test rate limiting functionality."""
        with patch.object(crawl4ai_scraper.rate_limiter, 'wait_for_token') as mock_wait:
            mock_wait.return_value = asyncio.Future()
            mock_wait.return_value.set_result(None)
            
            # Enable rate limiting
            crawl4ai_scraper.settings.rate_limit_enabled = True
            
            with patch.object(crawl4ai_scraper, 'scrape_url_with_crawl4ai') as mock_scrape:
                with patch.object(crawl4ai_scraper, 'update_task_status'):
                    with patch.object(crawl4ai_scraper, 'store_scraping_result'):
                        with patch('app.services.crawl4ai_scraper.ack_job'):
                            
                            mock_scrape.return_value = {"success": True, "content_size": 100}
                            sample_message = QueueMessage(
                                id="test",
                                data={"task_id": str(uuid.uuid4()), "job_id": str(uuid.uuid4()), "url": "https://example.com"},
                                retry_count=0
                            )
                            
                            await crawl4ai_scraper.process_scraping_task(sample_message, "test-worker")
                            
                            # Verify rate limiting was applied
                            mock_wait.assert_called_once()
    
    def test_get_extraction_strategy_llm(self, crawl4ai_scraper):
        """Test LLM extraction strategy creation."""
        config = {
            "extraction_strategy": "llm",
            "llm_config": {
                "provider": "openai",
                "api_token": "test-token",
                "schema": {"fields": []}
            }
        }
        
        with patch('app.services.crawl4ai_scraper.LLMExtractionStrategy') as mock_strategy:
            strategy = crawl4ai_scraper._get_extraction_strategy(config)
            mock_strategy.assert_called_once()
    
    def test_get_extraction_strategy_cosine(self, crawl4ai_scraper):
        """Test cosine extraction strategy creation."""
        config = {
            "extraction_strategy": "cosine",
            "semantic_filter": "technology articles",
            "word_count_threshold": 10
        }
        
        with patch('app.services.crawl4ai_scraper.CosineStrategy') as mock_strategy:
            strategy = crawl4ai_scraper._get_extraction_strategy(config)
            mock_strategy.assert_called_once()
    
    def test_get_extraction_strategy_default(self, crawl4ai_scraper):
        """Test default extraction strategy (None)."""
        config = {"extraction_strategy": "markdown"}
        
        strategy = crawl4ai_scraper._get_extraction_strategy(config)
        assert strategy is None
    
    @pytest.mark.asyncio
    async def test_store_scraping_result(self, crawl4ai_scraper):
        """Test result storage functionality."""
        task_id = uuid.uuid4()
        job_id = uuid.uuid4()
        result_data = {
            "success": True,
            "url": "https://example.com",
            "markdown": "content",
            "quality_score": 0.8
        }
        
        with patch('app.services.crawl4ai_scraper.get_database_session') as mock_db_session:
            mock_session = AsyncMock()
            mock_db_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_db_session.return_value.__aexit__ = AsyncMock(return_value=None)
            
            # Mock S3 storage
            with patch.object(crawl4ai_scraper, 'settings') as mock_settings:
                mock_settings.s3_enabled = True
                
                with patch('app.services.crawl4ai_scraper.S3Storage') as mock_s3_class:
                    mock_s3 = AsyncMock()
                    mock_s3.store_result = AsyncMock(return_value="s3://bucket/result.json")
                    mock_s3_class.return_value = mock_s3
                    
                    await crawl4ai_scraper.store_scraping_result(task_id, job_id, result_data)
                    
                    # Verify S3 storage was called
                    mock_s3.store_result.assert_called_once_with(job_id, task_id, result_data)
                    
                    # Verify database storage
                    mock_session.add.assert_called_once()
                    mock_session.commit.assert_called_once()


@pytest.mark.asyncio
async def test_integration_workflow():
    """Integration test for complete scraping workflow."""
    # This would test the entire workflow from queue message to result storage
    # In a real implementation, this would use test containers or mock services
    pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])