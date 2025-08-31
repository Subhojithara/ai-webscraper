"""
High-performance HTTP scraper with connection pooling and intelligent retry logic.
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin, urlparse
import httpx
from bs4 import BeautifulSoup
import structlog

from scraper_lib import get_settings, get_database_session, get_queue_manager
from scraper_lib.database import Task, TaskStatus, Result
from scraper_lib.observability import monitor_function, get_metrics_collector
from scraper_lib.cache import QueueMessage, dequeue_job, ack_job
from scraper_lib.utils import URLValidator, DataCleaner, RateLimiter, RetryHandler
from .proxy_manager import ProxyManager
from .s3_storage import S3Storage

logger = structlog.get_logger("http-scraper-worker")


class HTTPScraper:
    """High-performance HTTP scraper with connection pooling and advanced features."""
    
    def __init__(self, proxy_manager: ProxyManager):
        self.settings = get_settings()
        self.queue_manager = get_queue_manager()
        self.proxy_manager = proxy_manager
        self.s3_storage = S3Storage()
        self.metrics = get_metrics_collector("http-scraper-worker")
        
        # HTTP client with connection pooling
        self.client = None
        self.rate_limiter = RateLimiter(
            requests_per_second=self.settings.rate_limit_requests_per_second,
            burst=self.settings.rate_limit_burst
        )
        self.retry_handler = RetryHandler(
            max_attempts=self.settings.worker_retry_attempts,
            base_delay=1.0,
            max_delay=30.0
        )
        
        self.running = False
    
    async def initialize_client(self):
        """Initialize HTTP client with optimal settings."""
        limits = httpx.Limits(
            max_keepalive_connections=100,
            max_connections=200,
            keepalive_expiry=30.0
        )
        
        timeout = httpx.Timeout(
            connect=10.0,
            read=self.settings.scraping_timeout,
            write=10.0,
            pool=5.0
        )
        
        self.client = httpx.AsyncClient(
            limits=limits,
            timeout=timeout,
            follow_redirects=True,
            max_redirects=self.settings.scraping_max_redirects,
            verify=False,  # Skip SSL verification for problematic sites
            headers={
                'User-Agent': self.get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
        )
        
        logger.info("HTTP client initialized with connection pooling")
    
    async def start_worker(self, worker_id: str):
        """Start worker to consume scraping tasks."""
        logger.info("Starting HTTP scraper worker", worker_id=worker_id)
        
        if not self.client:
            await self.initialize_client()
        
        self.running = True
        
        while self.running:
            try:
                # Consume message from queue
                message = await dequeue_job(
                    self.settings.queue_scrape_http,
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
        logger.info("Stopping HTTP scraper worker")
        self.running = False
    
    @monitor_function("process_scraping_task")
    async def process_scraping_task(self, message: QueueMessage, worker_id: str):
        """Process a single scraping task."""
        task_id = message.data.get("task_id")
        url = message.data.get("url")
        
        logger.info(
            "Processing scraping task",
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
            
            # Perform scraping
            scraping_result = await self.scrape_url(
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
                self.settings.queue_scrape_http,
                worker_id,
                message.id
            )
            
            # Record metrics
            self.metrics.scraping_requests_total.labels(
                worker_type="http",
                status="success"
            ).inc()
            
            self.metrics.scraping_duration.labels(
                worker_type="http"
            ).observe(duration)
            
            if scraping_result.get("content_size"):
                self.metrics.scraping_data_size.labels(
                    worker_type="http"
                ).observe(scraping_result["content_size"])
            
            logger.info(
                "Scraping task completed successfully",
                task_id=task_id,
                url=url,
                duration=round(duration, 3),
                status_code=scraping_result.get("status_code")
            )
            
        except Exception as e:
            duration = time.time() - start_time
            
            logger.error(
                "Scraping task failed",
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
                error_message=str(e),
                error_type=type(e).__name__
            )
            
            # Record error metrics
            self.metrics.scraping_requests_total.labels(
                worker_type="http",
                status="error"
            ).inc()
            
            # Handle retry logic
            if message.retry_count < message.max_retries:
                await self.retry_task(message, worker_id)
            else:
                await self.handle_failed_task(message, str(e))
                
                # Acknowledge to remove from queue
                await ack_job(
                    self.settings.queue_scrape_http,
                    worker_id,
                    message.id
                )
    
    @monitor_function("scrape_url")
    async def scrape_url(
        self, 
        url: str, 
        task_config: Dict[str, Any],
        worker_id: str
    ) -> Dict[str, Any]:
        """Scrape a single URL with retry logic."""
        logger.debug("Starting URL scraping", url=url, worker_id=worker_id)
        
        # Get proxy if enabled
        proxy = None
        if self.settings.proxy_enabled:
            proxy = await self.proxy_manager.get_proxy()
        
        # Prepare request headers
        headers = self.get_request_headers(task_config)
        
        async def scrape_attempt():
            return await self._perform_request(url, headers, proxy, task_config)
        
        # Execute with retry logic
        result = await self.retry_handler.execute(scrape_attempt)
        
        return result
    
    async def _perform_request(
        self,
        url: str,
        headers: Dict[str, str],
        proxy: Optional[str],
        task_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Perform the actual HTTP request."""
        request_start = time.time()
        
        try:
            # Configure request options
            request_kwargs = {
                "headers": headers,
                "timeout": task_config.get("timeout", self.settings.scraping_timeout)
            }
            
            if proxy:
                request_kwargs["proxies"] = {"http://": proxy, "https://": proxy}
            
            # Make request
            response = await self.client.get(url, **request_kwargs)
            request_duration = time.time() - request_start
            
            logger.debug(
                "HTTP request completed",
                url=url,
                status_code=response.status_code,
                duration=round(request_duration, 3),
                content_length=len(response.content)
            )
            
            # Check for successful response
            response.raise_for_status()
            
            # Extract content
            content_result = await self.extract_content(response, url)
            
            # Store raw content in S3
            result_path = await self.s3_storage.store_raw_content(
                content_result["html"],
                url,
                response.headers.get("content-type", "text/html")
            )
            
            return {
                "status_code": response.status_code,
                "url": url,
                "final_url": str(response.url),
                "title": content_result["title"],
                "content": content_result["text"],
                "html": content_result["html"],
                "links": content_result["links"],
                "images": content_result["images"],
                "content_size": len(response.content),
                "response_headers": dict(response.headers),
                "request_duration": request_duration,
                "result_path": result_path,
                "extracted_data": content_result.get("extracted_data", {}),
                "metadata": {
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                    "proxy_used": proxy is not None,
                    "redirects": len(response.history)
                }
            }
            
        except httpx.HTTPStatusError as e:
            logger.warning(
                "HTTP error response",
                url=url,
                status_code=e.response.status_code,
                error=str(e)
            )
            
            # Still extract what we can from error responses
            if e.response.content:
                content_result = await self.extract_content(e.response, url)
                result_path = await self.s3_storage.store_raw_content(
                    content_result["html"],
                    url,
                    "text/html"
                )
                
                return {
                    "status_code": e.response.status_code,
                    "url": url,
                    "final_url": str(e.response.url),
                    "title": content_result.get("title", ""),
                    "content": content_result.get("text", ""),
                    "html": content_result.get("html", ""),
                    "links": content_result.get("links", []),
                    "images": content_result.get("images", []),
                    "content_size": len(e.response.content),
                    "response_headers": dict(e.response.headers),
                    "request_duration": time.time() - request_start,
                    "result_path": result_path,
                    "error": str(e),
                    "metadata": {
                        "scraped_at": datetime.now(timezone.utc).isoformat(),
                        "proxy_used": proxy is not None,
                        "http_error": True
                    }
                }
            else:
                raise
                
        except Exception as e:
            logger.error("Request failed", url=url, error=str(e))
            raise
    
    async def extract_content(self, response: httpx.Response, url: str) -> Dict[str, Any]:
        """Extract content from HTTP response."""
        content_type = response.headers.get("content-type", "").lower()
        
        if "text/html" in content_type:
            return await self.extract_html_content(response, url)
        elif "application/json" in content_type:
            return await self.extract_json_content(response, url)
        else:
            return await self.extract_text_content(response, url)
    
    async def extract_html_content(self, response: httpx.Response, url: str) -> Dict[str, Any]:
        """Extract content from HTML response."""
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract basic information
        title = soup.title.string.strip() if soup.title else ""
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Extract text content
        text_content = soup.get_text()
        cleaned_text = DataCleaner.clean_text(text_content)
        
        # Extract links
        links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            absolute_url = urljoin(url, href)
            if URLValidator.is_valid_url(absolute_url):
                links.append(absolute_url)
        
        # Extract images
        images = []
        for img in soup.find_all('img', src=True):
            src = img['src']
            absolute_url = urljoin(url, src)
            if URLValidator.is_valid_url(absolute_url):
                images.append(absolute_url)
        
        # Extract additional data
        extracted_data = {
            "emails": DataCleaner.extract_emails(cleaned_text),
            "phone_numbers": DataCleaner.extract_phone_numbers(cleaned_text),
            "meta_description": self.extract_meta_description(soup),
            "meta_keywords": self.extract_meta_keywords(soup),
            "headings": self.extract_headings(soup),
            "language": soup.get('lang') or self.detect_language(cleaned_text)
        }
        
        return {
            "title": DataCleaner.clean_text(title),
            "text": cleaned_text,
            "html": html_content,
            "links": list(set(links)),  # Remove duplicates
            "images": list(set(images)),  # Remove duplicates
            "extracted_data": extracted_data
        }
    
    async def extract_json_content(self, response: httpx.Response, url: str) -> Dict[str, Any]:
        """Extract content from JSON response."""
        try:
            json_data = response.json()
            text_content = json.dumps(json_data, indent=2)
            
            return {
                "title": f"JSON Data from {urlparse(url).netloc}",
                "text": text_content,
                "html": f"<pre>{text_content}</pre>",
                "links": [],
                "images": [],
                "extracted_data": {
                    "json_data": json_data,
                    "data_type": "json"
                }
            }
        except Exception as e:
            logger.warning("Failed to parse JSON", url=url, error=str(e))
            return await self.extract_text_content(response, url)
    
    async def extract_text_content(self, response: httpx.Response, url: str) -> Dict[str, Any]:
        """Extract content from plain text response."""
        text_content = response.text
        cleaned_text = DataCleaner.clean_text(text_content)
        
        return {
            "title": f"Text Content from {urlparse(url).netloc}",
            "text": cleaned_text,
            "html": f"<pre>{text_content}</pre>",
            "links": [],
            "images": [],
            "extracted_data": {
                "content_type": response.headers.get("content-type", ""),
                "data_type": "text"
            }
        }
    
    def extract_meta_description(self, soup: BeautifulSoup) -> str:
        """Extract meta description from HTML."""
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return DataCleaner.clean_text(meta_desc['content'])
        return ""
    
    def extract_meta_keywords(self, soup: BeautifulSoup) -> List[str]:
        """Extract meta keywords from HTML."""
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        if meta_keywords and meta_keywords.get('content'):
            keywords = meta_keywords['content'].split(',')
            return [kw.strip() for kw in keywords if kw.strip()]
        return []
    
    def extract_headings(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        """Extract headings from HTML."""
        headings = {}
        for level in range(1, 7):  # h1 to h6
            tag_name = f'h{level}'
            tags = soup.find_all(tag_name)
            if tags:
                headings[tag_name] = [
                    DataCleaner.clean_text(tag.get_text())
                    for tag in tags
                    if tag.get_text().strip()
                ]
        return headings
    
    def detect_language(self, text: str) -> str:
        """Simple language detection (placeholder)."""
        # This is a simplified implementation
        # In production, you might use langdetect or similar library
        if len(text) < 100:
            return "unknown"
        
        # Simple heuristics
        if any(char in text for char in "αβγδεζηθικλμνξοπρστυφχψω"):
            return "el"  # Greek
        elif any(char in text for char in "àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ"):
            return "fr"  # French (simplified)
        elif any(char in text for char in "äöüß"):
            return "de"  # German
        elif any(char in text for char in "ñáéíóúü"):
            return "es"  # Spanish
        else:
            return "en"  # Default to English
    
    def get_request_headers(self, task_config: Dict[str, Any]) -> Dict[str, str]:
        """Get request headers for scraping."""
        headers = {
            'User-Agent': self.get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # Add custom headers from task config
        custom_headers = task_config.get("headers", {})
        headers.update(custom_headers)
        
        # Override user agent if specified
        if task_config.get("user_agent"):
            headers['User-Agent'] = task_config["user_agent"]
        
        return headers
    
    def get_random_user_agent(self) -> str:
        """Get a random user agent string."""
        import random
        return random.choice(self.settings.scraping_user_agents)
    
    async def update_task_status(
        self,
        task_id: uuid.UUID,
        status: TaskStatus,
        **kwargs
    ):
        """Update task status in database."""
        async for db in get_database_session():
            try:
                from sqlalchemy import select
                stmt = select(Task).where(Task.id == task_id)
                result = await db.execute(stmt)
                task = result.scalar_one_or_none()
                
                if task:
                    task.status = status
                    for key, value in kwargs.items():
                        if hasattr(task, key):
                            setattr(task, key, value)
                    
                    await db.commit()
                    
                    logger.debug(
                        "Task status updated",
                        task_id=str(task_id),
                        status=status.value
                    )
                    
            except Exception as e:
                await db.rollback()
                logger.error(
                    "Failed to update task status",
                    task_id=str(task_id),
                    error=str(e)
                )
    
    async def store_scraping_result(
        self,
        task_id: uuid.UUID,
        job_id: uuid.UUID,
        result: Dict[str, Any]
    ):
        """Store scraping result in database."""
        async for db in get_database_session():
            try:
                # Calculate quality metrics
                quality_metrics = DataCleaner.validate_data_quality(result)
                
                db_result = Result(
                    task_id=task_id,
                    url=result["url"],
                    title=result.get("title"),
                    content=result.get("content"),
                    html=result.get("html"),
                    links=result.get("links"),
                    images=result.get("images"),
                    emails=result.get("extracted_data", {}).get("emails"),
                    phone_numbers=result.get("extracted_data", {}).get("phone_numbers"),
                    language=result.get("extracted_data", {}).get("language"),
                    keywords=result.get("extracted_data", {}).get("meta_keywords"),
                    description=result.get("extracted_data", {}).get("meta_description"),
                    response_headers=result.get("response_headers"),
                    content_quality_score=quality_metrics["quality_score"],
                    data_completeness=quality_metrics["completeness_score"],
                    metadata=result.get("metadata", {})
                )
                
                db.add(db_result)
                await db.commit()
                
                logger.debug(
                    "Scraping result stored",
                    task_id=str(task_id),
                    quality_score=quality_metrics["quality_score"]
                )
                
            except Exception as e:
                await db.rollback()
                logger.error(
                    "Failed to store scraping result",
                    task_id=str(task_id),
                    error=str(e)
                )
    
    async def retry_task(self, message: QueueMessage, worker_id: str):
        """Retry a failed task."""
        logger.info(
            "Retrying scraping task",
            task_id=message.data.get("task_id"),
            retry_count=message.retry_count
        )
        
        # Increment retry count
        message.retry_count += 1
        
        # Add delay before retry
        delay = min(2 ** message.retry_count, 60)  # Exponential backoff with max
        await asyncio.sleep(delay)
        
        # Re-enqueue task
        await self.queue_manager.publish_to_queue(
            self.settings.queue_scrape_http,
            message.data,
            priority=message.data.get("priority", 1)
        )
    
    async def handle_failed_task(self, message: QueueMessage, error: str):
        """Handle permanently failed task."""
        logger.error(
            "Task failed permanently",
            task_id=message.data.get("task_id"),
            url=message.data.get("url"),
            error=error
        )
        
        # Move to dead letter queue
        await self.queue_manager.publish_to_queue(
            self.settings.queue_failed_tasks,
            {
                "original_message": message.to_dict(),
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "service": "http-scraper-worker",
                "error": error
            }
        )
    
    async def close(self):
        """Close HTTP client and cleanup resources."""
        if self.client:
            await self.client.aclose()
            logger.info("HTTP client closed")