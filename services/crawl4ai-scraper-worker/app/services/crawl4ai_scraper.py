"""
Advanced AI-powered web scraper using Crawl4AI for intelligent content extraction.
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin, urlparse

import structlog
from crawl4ai import AsyncWebCrawler
from crawl4ai.extraction_strategy import LLMExtractionStrategy, CosineStrategy

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'worker-shared'))

from scraper_lib import get_settings, get_database_session, get_queue_manager
from scraper_lib.database import Task, TaskStatus, Result
from scraper_lib.observability import monitor_function, get_metrics_collector
from scraper_lib.cache import QueueMessage, dequeue_job, ack_job
from scraper_lib.utils import URLValidator, DataCleaner, RateLimiter, RetryHandler

logger = structlog.get_logger("crawl4ai-scraper-worker")


class Crawl4AIScraper:
    """Advanced AI-powered scraper using Crawl4AI for intelligent content extraction."""
    
    def __init__(self):
        self.settings = get_settings()
        self.queue_manager = get_queue_manager()
        self.metrics = get_metrics_collector("crawl4ai-scraper-worker")
        
        # Rate limiting and retry handling
        self.rate_limiter = RateLimiter(
            requests_per_second=self.settings.rate_limit_requests_per_second,
            burst=self.settings.rate_limit_burst
        )
        self.retry_handler = RetryHandler(
            max_attempts=self.settings.worker_retry_attempts,
            base_delay=1.0,
            max_delay=30.0
        )
        
        # Crawl4AI configuration
        self.crawler_config = {
            "headless": True,
            "verbose": False,
            "timeout": 30,
            "user_agent": self._get_default_user_agent(),
            "enable_stealth": True,
            "bypass_cloudflare": True
        }
        
        self.running = False
    
    def _get_default_user_agent(self) -> str:
        """Get default user agent for scraping."""
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    
    async def start_worker(self, worker_id: str):
        """Start worker to consume Crawl4AI scraping tasks."""
        logger.info("Starting Crawl4AI scraper worker", worker_id=worker_id)
        
        self.running = True
        
        while self.running:
            try:
                # Consume message from queue
                message = await dequeue_job(
                    self.settings.queue_scrape_crawl4ai,
                    worker_id,
                    timeout=5000
                )
                
                if message:
                    await self.process_scraping_task(message, worker_id)
                    
            except Exception as e:
                logger.error("Error in worker loop", worker_id=worker_id, error=str(e))
                await asyncio.sleep(5)  # Wait before retrying
    
    def stop_worker(self):
        """Stop the worker."""
        logger.info("Stopping Crawl4AI scraper worker")
        self.running = False
    
    @monitor_function("process_crawl4ai_task")
    async def process_scraping_task(self, message: QueueMessage, worker_id: str):
        """Process a single Crawl4AI scraping task."""
        task_id = message.data.get("task_id")
        url = message.data.get("url")
        
        logger.info(
            "Processing Crawl4AI scraping task",
            task_id=task_id,
            url=url,
            worker_id=worker_id
        )
        
        start_time = time.time()
        
        try:
            # Update task status to processing
            await self.update_task_status(
                uuid.UUID(task_id),
                TaskStatus.PROCESSING,
                started_at=datetime.now(timezone.utc)
            )
            
            # Apply rate limiting
            if self.settings.rate_limit_enabled:
                await self.rate_limiter.wait_for_token()
            
            # Perform AI-powered scraping
            scraping_result = await self.scrape_url_with_crawl4ai(
                url,
                task_config=message.data.get("config", {}),
                worker_id=worker_id
            )
            
            # Store results
            await self.store_scraping_result(
                uuid.UUID(task_id),
                uuid.UUID(message.data["job_id"]),
                scraping_result
            )
            
            # Update task as completed
            duration = time.time() - start_time
            await self.update_task_status(
                uuid.UUID(task_id),
                TaskStatus.COMPLETED,
                completed_at=datetime.now(timezone.utc),
                duration_seconds=duration,
                status_code=scraping_result.get("status_code"),
                result_path=scraping_result.get("result_path")
            )
            
            # Acknowledge successful processing
            await ack_job(
                self.settings.queue_scrape_crawl4ai,
                worker_id,
                message.id
            )
            
            # Record metrics
            self.metrics.scraping_requests_total.labels(
                worker_type="crawl4ai",
                status="success"
            ).inc()
            
            self.metrics.scraping_duration.labels(
                worker_type="crawl4ai"
            ).observe(duration)
            
            if scraping_result.get("content_size"):
                self.metrics.scraping_data_size.labels(
                    worker_type="crawl4ai"
                ).observe(scraping_result["content_size"])
            
            logger.info(
                "Crawl4AI scraping task completed successfully",
                task_id=task_id,
                url=url,
                duration=round(duration, 3),
                quality_score=scraping_result.get("quality_score", 0)
            )
            
        except Exception as e:
            duration = time.time() - start_time
            
            logger.error(
                "Error processing Crawl4AI scraping task",
                task_id=task_id,
                url=url,
                error=str(e),
                duration=round(duration, 3)
            )
            
            # Update task as failed
            await self.update_task_status(
                uuid.UUID(task_id),
                TaskStatus.FAILED,
                completed_at=datetime.now(timezone.utc),
                duration_seconds=duration,
                error=str(e)
            )
            
            # Record error metrics
            self.metrics.scraping_requests_total.labels(
                worker_type="crawl4ai",
                status="error"
            ).inc()
            
            # Acknowledge failed processing (to avoid infinite retries)
            await ack_job(
                self.settings.queue_scrape_crawl4ai,
                worker_id,
                message.id
            )
    
    @monitor_function("crawl4ai_scrape_url")
    async def scrape_url_with_crawl4ai(
        self, 
        url: str, 
        task_config: Dict[str, Any] = None,
        worker_id: str = None
    ) -> Dict[str, Any]:
        """
        Scrape URL using Crawl4AI with AI-enhanced extraction.
        
        Args:
            url: The URL to scrape
            task_config: Configuration for scraping behavior
            worker_id: ID of the worker processing this task
            
        Returns:
            Dictionary containing scraped data and metadata
        """
        task_config = task_config or {}
        
        # Merge configuration
        crawler_config = {**self.crawler_config, **task_config.get("crawler_config", {})}
        
        try:
            async with AsyncWebCrawler(**crawler_config) as crawler:
                # Determine extraction strategy
                extraction_strategy = self._get_extraction_strategy(task_config)
                
                # Perform crawling
                result = await crawler.arun(
                    url=url,
                    extraction_strategy=extraction_strategy,
                    bypass_cache=task_config.get("bypass_cache", True),
                    js_code=task_config.get("js_code"),
                    wait_for=task_config.get("wait_for"),
                    timeout=task_config.get("timeout", 30),
                    css_selector=task_config.get("css_selector"),
                    screenshot=task_config.get("screenshot", False),
                    magic=task_config.get("magic", False)
                )
                
                # Process and structure the result
                processed_result = await self._process_crawl_result(result, task_config)
                
                return processed_result
                
        except Exception as e:
            logger.error(f"Crawl4AI scraping failed for {url}: {str(e)}")
            raise
    
    def _get_extraction_strategy(self, config: Dict[str, Any]):
        """Determine the appropriate extraction strategy based on configuration."""
        strategy_type = config.get("extraction_strategy", "markdown")
        
        if strategy_type == "llm" and config.get("llm_config"):
            # LLM-based extraction for complex scenarios
            llm_config = config["llm_config"]
            return LLMExtractionStrategy(
                provider=llm_config.get("provider", "openai"),
                api_token=llm_config.get("api_token") or self.settings.openai_api_key,
                schema=llm_config.get("schema", {}),
                extraction_type=llm_config.get("extraction_type", "schema"),
                apply_chunking=llm_config.get("apply_chunking", True),
                chunking_strategy=llm_config.get("chunking_strategy", "semantic")
            )
        
        elif strategy_type == "cosine" and config.get("semantic_filter"):
            # Semantic similarity-based extraction
            return CosineStrategy(
                semantic_filter=config["semantic_filter"],
                word_count_threshold=config.get("word_count_threshold", 10),
                max_dist=config.get("max_dist", 0.2),
                linkage_method=config.get("linkage_method", "ward"),
                top_k=config.get("top_k", 3)
            )
        
        else:
            # Default to no strategy (markdown extraction)
            return None
    
    async def _process_crawl_result(
        self, 
        result, 
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process and structure the Crawl4AI result."""
        
        # Calculate quality score
        quality_score = self._calculate_quality_score(result)
        
        # Extract and clean content
        processed_data = {
            "success": result.success,
            "url": result.url,
            "status_code": getattr(result, 'status_code', None),
            "markdown": result.markdown,
            "cleaned_html": getattr(result, 'cleaned_html', None),
            "extracted_content": result.extracted_content,
            "links": {
                "internal": [link for link in (result.links.get('internal', []) if result.links else [])],
                "external": [link for link in (result.links.get('external', []) if result.links else [])]
            },
            "images": result.images if hasattr(result, 'images') else [],
            "metadata": result.metadata if hasattr(result, 'metadata') else {},
            "media": getattr(result, 'media', {}),
            "quality_score": quality_score,
            "content_size": len(result.markdown) if result.markdown else 0,
            "extraction_type": config.get("extraction_strategy", "markdown"),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Add screenshot if requested and available
        if config.get("screenshot") and hasattr(result, 'screenshot'):
            processed_data["screenshot"] = result.screenshot
        
        # Clean and validate data
        if config.get("clean_data", True):
            processed_data = await self._clean_extracted_data(processed_data)
        
        return processed_data
    
    def _calculate_quality_score(self, result) -> float:
        """Calculate content quality score based on various factors."""
        if not result.success or not result.markdown:
            return 0.0
        
        score = 0.0
        
        # Base score for successful extraction
        score += 0.3
        
        # Content length factor (enhanced scoring)
        content_length = len(result.markdown)
        if content_length > 1000:
            score += 0.25  # Increased from 0.2
        elif content_length > 500:
            score += 0.15  # Increased from 0.1
        elif content_length > 100:
            score += 0.05  # Added tier for shorter content
        
        # Structured content factor
        if result.extracted_content:
            score += 0.2
        
        # Links factor (safe handling of None values)
        if hasattr(result, 'links') and result.links:
            internal_links = result.links.get('internal', []) if result.links else []
            external_links = result.links.get('external', []) if result.links else []
            total_links = len(internal_links) + len(external_links)
            if total_links > 0:
                score += min(0.15, total_links * 0.02)  # Slightly reduced max
        
        # Images factor (safe handling of None values)
        if hasattr(result, 'images') and result.images:
            score += min(0.1, len(result.images) * 0.01)
        
        return min(1.0, score)
    
    async def _clean_extracted_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize extracted data."""
        cleaner = DataCleaner()
        
        # Clean markdown content
        if data.get("markdown"):
            data["markdown"] = cleaner.clean_text(data["markdown"])
        
        # Clean extracted content (handle None values)
        if data.get("extracted_content"):
            if isinstance(data["extracted_content"], str):
                data["extracted_content"] = cleaner.clean_text(data["extracted_content"])
            elif isinstance(data["extracted_content"], list):
                data["extracted_content"] = [
                    cleaner.clean_text(item) if isinstance(item, str) else item
                    for item in data["extracted_content"]
                ]
            elif isinstance(data["extracted_content"], dict):
                # Handle dictionary extracted content
                cleaned_content = {}
                for key, value in data["extracted_content"].items():
                    if isinstance(value, str):
                        cleaned_content[key] = cleaner.clean_text(value)
                    else:
                        cleaned_content[key] = value
                data["extracted_content"] = cleaned_content
        
        # Validate and clean URLs (safe handling of None values)
        url_validator = URLValidator()
        
        # Ensure links structure exists
        if not data.get("links"):
            data["links"] = {"internal": [], "external": []}
        
        # Clean internal links
        if data["links"].get("internal"):
            data["links"]["internal"] = [
                link for link in data["links"]["internal"] 
                if link and url_validator.is_valid_url(link)
            ]
        else:
            data["links"]["internal"] = []
        
        # Clean external links
        if data["links"].get("external"):
            data["links"]["external"] = [
                link for link in data["links"]["external"] 
                if link and url_validator.is_valid_url(link)
            ]
        else:
            data["links"]["external"] = []
        
        return data
    
    async def update_task_status(
        self,
        task_id: uuid.UUID,
        status: TaskStatus,
        **kwargs
    ):
        """Update task status in database."""
        async with get_database_session() as session:
            task = await session.get(Task, task_id)
            if task:
                task.status = status
                for key, value in kwargs.items():
                    if hasattr(task, key):
                        setattr(task, key, value)
                await session.commit()
    
    async def store_scraping_result(
        self,
        task_id: uuid.UUID,
        job_id: uuid.UUID,
        result_data: Dict[str, Any]
    ):
        """Store scraping result in database and S3."""
        # Store in S3 if configured
        result_path = None
        if self.settings.s3_enabled:
            try:
                from .s3_storage import S3Storage
                s3_storage = S3Storage()
                result_path = await s3_storage.store_result(
                    job_id, 
                    task_id, 
                    result_data
                )
            except Exception as e:
                logger.warning(f"Failed to store result in S3: {e}")
        
        # Store in database
        async with get_database_session() as session:
            result = Result(
                id=uuid.uuid4(),
                task_id=task_id,
                job_id=job_id,
                content=json.dumps(result_data) if not result_path else None,
                metadata={
                    "extraction_type": result_data.get("extraction_type"),
                    "quality_score": result_data.get("quality_score"),
                    "content_size": result_data.get("content_size"),
                    "s3_path": result_path
                },
                created_at=datetime.now(timezone.utc)
            )
            session.add(result)
            await session.commit()