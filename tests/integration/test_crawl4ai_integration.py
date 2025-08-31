"""
Integration tests for Crawl4AI worker integration with the K-Scrape Nexus system.
"""

import pytest
import asyncio
import json
import uuid
from datetime import datetime, timezone
from unittest.mock import Mock, AsyncMock, patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'services', 'worker-shared'))

from scraper_lib.database import Job, Task, TaskStatus, JobStatus, WorkerType


@pytest.fixture
def sample_job_data():
    """Sample job data for testing."""
    return {
        "name": "Test Crawl4AI Job",
        "description": "Testing AI-enhanced scraping",
        "config": {
            "enable_ai_extraction": True,
            "extraction_strategy": "llm",
            "llm_config": {
                "provider": "openai",
                "schema": {
                    "fields": [
                        {"name": "title", "selector": "h1", "type": "text"},
                        {"name": "content", "selector": "p", "type": "text"},
                        {"name": "author", "selector": ".author", "type": "text"}
                    ]
                }
            }
        },
        "urls": [
            "https://example.com/article1",
            "https://example.com/article2",
            "https://blog.example.com/post1"
        ]
    }


@pytest.fixture
def sample_tasks(sample_job_data):
    """Sample tasks for testing."""
    job_id = uuid.uuid4()
    tasks = []
    
    for i, url in enumerate(sample_job_data["urls"]):
        task = Task(
            id=uuid.uuid4(),
            job_id=job_id,
            url=url,
            status=TaskStatus.PENDING,
            worker_type=WorkerType.CRAWL4AI,
            config=sample_job_data["config"],
            priority=1
        )
        tasks.append(task)
    
    return tasks


class TestCrawl4AIIntegration:
    """Integration tests for Crawl4AI worker."""
    
    @pytest.mark.asyncio
    async def test_job_coordinator_routing_to_crawl4ai(self, sample_tasks):
        """Test that job coordinator correctly routes tasks to Crawl4AI worker."""
        from services.coordinator.app.services.job_coordinator import JobCoordinator
        
        with patch('services.coordinator.app.services.job_coordinator.get_settings') as mock_settings:
            with patch('services.coordinator.app.services.job_coordinator.get_redis_client') as mock_redis:
                coordinator = JobCoordinator()
                
                # Test AI-enhanced task routing
                ai_task = sample_tasks[0]
                ai_task.config = {"enable_ai_extraction": True}
                
                worker_type = await coordinator._determine_optimal_worker(ai_task)
                assert worker_type == WorkerType.CRAWL4AI
                
                # Test semantic extraction routing
                semantic_task = sample_tasks[1]
                semantic_task.config = {"semantic_filter": "technology articles"}
                
                worker_type = await coordinator._determine_optimal_worker(semantic_task)
                assert worker_type == WorkerType.CRAWL4AI
                
                # Test smart routing for content sites
                content_task = sample_tasks[2]
                content_task.url = "https://medium.com/article"
                content_task.config = {"enable_smart_routing": True}
                
                worker_type = await coordinator._determine_optimal_worker(content_task)
                assert worker_type == WorkerType.CRAWL4AI
    
    @pytest.mark.asyncio
    async def test_crawl4ai_worker_queue_consumption(self):
        """Test that Crawl4AI worker correctly consumes from its queue."""
        from services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper import Crawl4AIScraper
        
        with patch('services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper.get_settings') as mock_settings:
            with patch('services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper.dequeue_job') as mock_dequeue:
                mock_settings.return_value.queue_scrape_crawl4ai = "scrape_crawl4ai"
                
                scraper = Crawl4AIScraper()
                
                # Mock queue message
                queue_message = Mock()
                queue_message.data = {
                    "task_id": str(uuid.uuid4()),
                    "job_id": str(uuid.uuid4()),
                    "url": "https://example.com",
                    "config": {"enable_ai_extraction": True}
                }
                
                mock_dequeue.return_value = queue_message
                
                with patch.object(scraper, 'process_scraping_task') as mock_process:
                    # Start worker briefly
                    task = asyncio.create_task(scraper.start_worker("test-worker"))
                    await asyncio.sleep(0.1)
                    scraper.stop_worker()
                    
                    try:
                        await asyncio.wait_for(task, timeout=1.0)
                    except asyncio.TimeoutError:
                        task.cancel()
                    
                    # Verify message was processed
                    mock_process.assert_called()
    
    @pytest.mark.asyncio
    async def test_end_to_end_ai_scraping_workflow(self, sample_job_data):
        """Test complete end-to-end AI scraping workflow."""
        # This test simulates the complete workflow:
        # 1. Job creation with AI config
        # 2. Task routing to Crawl4AI worker
        # 3. AI-enhanced scraping
        # 4. Result storage and processing
        
        job_id = uuid.uuid4()
        task_id = uuid.uuid4()
        
        # Mock the complete workflow
        with patch('services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper.AsyncWebCrawler') as mock_crawler_class:
            # Mock successful Crawl4AI result
            mock_result = Mock()
            mock_result.success = True
            mock_result.url = "https://example.com"
            mock_result.markdown = "# AI-Enhanced Content\n\nThis is extracted content."
            mock_result.extracted_content = {
                "title": "AI-Enhanced Content",
                "content": "This is extracted content.",
                "author": "John Doe"
            }
            mock_result.metadata = {"extraction_type": "llm"}
            
            mock_crawler = AsyncMock()
            mock_crawler.__aenter__ = AsyncMock(return_value=mock_crawler)
            mock_crawler.__aexit__ = AsyncMock(return_value=None)
            mock_crawler.arun = AsyncMock(return_value=mock_result)
            mock_crawler_class.return_value = mock_crawler
            
            # Test the scraping process
            from services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper import Crawl4AIScraper
            
            with patch('services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper.get_settings'):
                scraper = Crawl4AIScraper()
                
                result = await scraper.scrape_url_with_crawl4ai(
                    "https://example.com",
                    sample_job_data["config"],
                    "test-worker"
                )
                
                # Verify AI-enhanced result
                assert result["success"] is True
                assert result["extraction_type"] == "llm"
                assert "AI-Enhanced Content" in result["markdown"]
                assert result["extracted_content"]["title"] == "AI-Enhanced Content"
                assert result["extracted_content"]["author"] == "John Doe"
                assert result["quality_score"] > 0
    
    @pytest.mark.asyncio
    async def test_ai_extraction_with_different_strategies(self):
        """Test different AI extraction strategies."""
        from services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper import Crawl4AIScraper
        
        with patch('services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper.get_settings'):
            scraper = Crawl4AIScraper()
            
            # Test LLM extraction strategy
            llm_config = {
                "extraction_strategy": "llm",
                "llm_config": {
                    "provider": "openai",
                    "schema": {"fields": [{"name": "title", "type": "text"}]}
                }
            }
            
            with patch('services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper.LLMExtractionStrategy') as mock_llm:
                strategy = scraper._get_extraction_strategy(llm_config)
                mock_llm.assert_called_once()
            
            # Test cosine similarity strategy
            cosine_config = {
                "extraction_strategy": "cosine",
                "semantic_filter": "Find technology articles",
                "word_count_threshold": 10
            }
            
            with patch('services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper.CosineStrategy') as mock_cosine:
                strategy = scraper._get_extraction_strategy(cosine_config)
                mock_cosine.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_quality_score_calculation(self):
        """Test quality score calculation for different content types."""
        from services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper import Crawl4AIScraper
        
        with patch('services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper.get_settings'):
            scraper = Crawl4AIScraper()
            
            # High-quality content
            high_quality_result = Mock()
            high_quality_result.success = True
            high_quality_result.markdown = "# High Quality Article\n\n" + "Content " * 200
            high_quality_result.extracted_content = {"title": "High Quality", "content": "Rich content"}
            high_quality_result.links = {"internal": ["url1", "url2"], "external": ["ext1"]}
            high_quality_result.images = ["img1.jpg", "img2.jpg"]
            
            score = scraper._calculate_quality_score(high_quality_result)
            assert score > 0.8
            
            # Low-quality content
            low_quality_result = Mock()
            low_quality_result.success = True
            low_quality_result.markdown = "Short"
            low_quality_result.extracted_content = None
            low_quality_result.links = None
            low_quality_result.images = None
            
            score = scraper._calculate_quality_score(low_quality_result)
            assert score < 0.5
    
    @pytest.mark.asyncio
    async def test_error_handling_and_retry_logic(self):
        """Test error handling and retry logic in the integration."""
        from services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper import Crawl4AIScraper
        from scraper_lib.cache import QueueMessage
        
        with patch('services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper.get_settings'):
            scraper = Crawl4AIScraper()
            
            # Test handling of Crawl4AI failures
            with patch('services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper.AsyncWebCrawler') as mock_crawler_class:
                mock_crawler = AsyncMock()
                mock_crawler.__aenter__ = AsyncMock(return_value=mock_crawler)
                mock_crawler.__aexit__ = AsyncMock(return_value=None)
                mock_crawler.arun = AsyncMock(side_effect=Exception("Crawl4AI failed"))
                mock_crawler_class.return_value = mock_crawler
                
                # Test exception handling
                with pytest.raises(Exception):
                    await scraper.scrape_url_with_crawl4ai(
                        "https://example.com",
                        {"extraction_strategy": "llm"},
                        "test-worker"
                    )
    
    @pytest.mark.asyncio
    async def test_metrics_collection(self):
        """Test that metrics are properly collected during AI scraping."""
        from services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper import Crawl4AIScraper
        
        with patch('services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper.get_settings'):
            with patch('services.crawl4ai_scraper_worker.app.services.crawl4ai_scraper.get_metrics_collector') as mock_metrics:
                mock_metrics_instance = Mock()
                mock_metrics_instance.scraping_requests_total = Mock()
                mock_metrics_instance.scraping_requests_total.labels = Mock(return_value=Mock())
                mock_metrics_instance.scraping_duration = Mock()
                mock_metrics_instance.scraping_duration.labels = Mock(return_value=Mock())
                mock_metrics.return_value = mock_metrics_instance
                
                scraper = Crawl4AIScraper()
                
                # Verify metrics collector was initialized
                assert scraper.metrics is not None
    
    @pytest.mark.asyncio
    async def test_s3_storage_integration(self):
        """Test S3 storage integration for AI scraping results."""
        from services.crawl4ai_scraper_worker.app.services.s3_storage import S3Storage
        
        job_id = uuid.uuid4()
        task_id = uuid.uuid4()
        
        result_data = {
            "success": True,
            "url": "https://example.com",
            "markdown": "# AI Content",
            "extracted_content": {"title": "AI Content"},
            "extraction_type": "llm",
            "quality_score": 0.9
        }
        
        with patch('services.crawl4ai_scraper_worker.app.services.s3_storage.boto3.client') as mock_boto3:
            mock_s3_client = Mock()
            mock_boto3.return_value = mock_s3_client
            
            with patch('services.crawl4ai_scraper_worker.app.services.s3_storage.get_settings') as mock_settings:
                mock_settings.return_value.s3_bucket_name = "test-bucket"
                
                storage = S3Storage()
                s3_key = await storage.store_result(job_id, task_id, result_data)
                
                # Verify S3 storage was called
                mock_s3_client.put_object.assert_called_once()
                assert "crawl4ai-results" in s3_key
                assert str(job_id) in s3_key
                assert str(task_id) in s3_key


@pytest.mark.asyncio
async def test_performance_benchmarking():
    """Benchmark test for AI scraping performance."""
    # This would test performance characteristics like:
    # - Time to process different content types
    # - Memory usage during AI processing
    # - Throughput under load
    pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])