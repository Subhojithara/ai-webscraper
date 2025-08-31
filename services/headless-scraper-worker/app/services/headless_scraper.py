"""
Headless Scraper Worker with Playwright for JavaScript-heavy sites.
Handles dynamic content, SPAs, and complex JavaScript interactions.
"""

import asyncio
import json
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from playwright.async_api import async_playwright, Browser, BrowserContext, Page
import structlog

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'worker-shared'))

from scraper_lib import get_settings, get_database_session, get_redis_client
from scraper_lib.models import Task, TaskStatus, ScrapingResult
from scraper_lib.observability import get_metrics_collector

logger = structlog.get_logger(__name__)


class HeadlessScraper:
    """
    Advanced headless scraper using Playwright for JavaScript-heavy sites.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.metrics = get_metrics_collector("headless-scraper")
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        
        # Scraping configuration
        self.default_timeout = 30000  # 30 seconds
        self.wait_timeout = 10000     # 10 seconds
        self.navigation_timeout = 60000  # 60 seconds
        
        # Browser settings
        self.viewport = {"width": 1920, "height": 1080}
        self.user_agents = self.settings.scraping_user_agents
        
    async def initialize(self):
        """Initialize Playwright browser and context."""
        try:
            self.playwright = await async_playwright().start()
            
            # Launch browser with optimized settings
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--no-first-run',
                    '--no-zygote',
                    '--disable-gpu',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding'
                ]
            )
            
            # Create browser context with stealth settings
            self.context = await self.browser.new_context(
                viewport=self.viewport,
                user_agent=self._get_random_user_agent(),
                locale='en-US',
                timezone_id='America/New_York',
                ignore_https_errors=True,
                java_script_enabled=True
            )
            
            # Set additional context options
            await self.context.set_extra_http_headers({
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            })
            
            logger.info("Playwright browser initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Playwright browser: {e}")
            await self.cleanup()
            raise
    
    async def cleanup(self):
        """Clean up browser resources."""
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if hasattr(self, 'playwright'):
                await self.playwright.stop()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def _get_random_user_agent(self) -> str:
        """Get a random user agent string."""
        import random
        return random.choice(self.user_agents)
    
    async def scrape_url(self, url: str, config: Optional[Dict] = None) -> ScrapingResult:
        """
        Scrape a URL using headless browser with JavaScript support.
        
        Args:
            url: The URL to scrape
            config: Optional configuration for scraping behavior
            
        Returns:
            ScrapingResult object with scraped data
        """
        start_time = time.time()
        config = config or {}
        
        # Create new page for each request to avoid conflicts
        page = await self.context.new_page()
        
        try:
            # Configure page settings
            await self._configure_page(page, config)
            
            # Navigate to URL with retries
            response = await self._navigate_with_retry(page, url)
            
            # Wait for page to be ready
            await self._wait_for_page_ready(page, config)
            
            # Handle dynamic content loading
            await self._handle_dynamic_content(page, config)
            
            # Extract content
            result = await self._extract_content(page, url, response)
            
            # Calculate metrics
            result.response_time = time.time() - start_time
            result.quality_score = self._calculate_quality_score(result)
            
            # Record success metrics
            self.metrics.scraping_requests_total.labels(
                worker_type="headless",
                status="success"
            ).inc()
            
            self.metrics.scraping_duration.labels(
                worker_type="headless"
            ).observe(result.response_time)
            
            return result
            
        except Exception as e:
            error_message = f"Error scraping {url}: {str(e)}"
            logger.error(error_message)
            
            # Record error metrics
            self.metrics.scraping_requests_total.labels(
                worker_type="headless",
                status="error"
            ).inc()
            
            return ScrapingResult(
                url=url,
                success=False,
                error_message=error_message,
                response_time=time.time() - start_time,
                quality_score=0.0
            )
            
        finally:
            await page.close()
    
    async def _configure_page(self, page: Page, config: Dict):
        """Configure page settings based on configuration."""
        # Set timeouts
        page.set_default_timeout(config.get('timeout', self.default_timeout))
        page.set_default_navigation_timeout(config.get('navigation_timeout', self.navigation_timeout))
        
        # Block unnecessary resources to improve performance
        if config.get('block_resources', True):
            await page.route("**/*", self._handle_route)
        
        # Set up request/response logging
        if config.get('log_requests', False):
            page.on("request", lambda request: logger.debug(f"Request: {request.method} {request.url}"))
            page.on("response", lambda response: logger.debug(f"Response: {response.status} {response.url}"))
        
        # Handle JavaScript errors
        page.on("pageerror", lambda error: logger.warning(f"JavaScript error: {error}"))
    
    async def _handle_route(self, route):
        """Handle resource requests - block unnecessary resources."""
        resource_type = route.request.resource_type
        
        # Block images, fonts, and other non-essential resources
        if resource_type in ['image', 'font', 'media']:
            await route.abort()
        else:
            await route.continue_()
    
    async def _navigate_with_retry(self, page: Page, url: str, max_retries: int = 3):
        """Navigate to URL with retry logic."""
        for attempt in range(max_retries):
            try:
                response = await page.goto(
                    url,
                    wait_until='domcontentloaded',
                    timeout=self.navigation_timeout
                )
                
                if response and response.status < 400:
                    return response
                    
                if attempt == max_retries - 1:
                    raise Exception(f"HTTP {response.status if response else 'No response'}")
                    
                # Wait before retry
                await asyncio.sleep(2 ** attempt)
                
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                    
                logger.warning(f"Navigation attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(2 ** attempt)
    
    async def _wait_for_page_ready(self, page: Page, config: Dict):
        """Wait for page to be fully loaded and ready."""
        # Wait for network to be idle
        try:
            await page.wait_for_load_state('networkidle', timeout=self.wait_timeout)
        except:
            # If network doesn't become idle, just wait for basic load
            await page.wait_for_load_state('load', timeout=self.wait_timeout)
        
        # Wait for specific selectors if provided
        wait_selectors = config.get('wait_selectors', [])
        for selector in wait_selectors:
            try:
                await page.wait_for_selector(selector, timeout=self.wait_timeout)
            except:
                logger.warning(f"Selector '{selector}' not found within timeout")
        
        # Additional wait time for JavaScript execution
        wait_time = config.get('additional_wait', 2000)
        if wait_time > 0:
            await page.wait_for_timeout(wait_time)
    
    async def _handle_dynamic_content(self, page: Page, config: Dict):
        """Handle dynamic content loading (infinite scroll, lazy loading, etc.)."""
        # Handle infinite scroll
        if config.get('infinite_scroll', False):
            await self._handle_infinite_scroll(page, config)
        
        # Handle lazy loading
        if config.get('trigger_lazy_load', False):
            await self._trigger_lazy_loading(page)
        
        # Execute custom JavaScript
        custom_js = config.get('custom_javascript')
        if custom_js:
            try:
                await page.evaluate(custom_js)
            except Exception as e:
                logger.warning(f"Custom JavaScript execution failed: {e}")
    
    async def _handle_infinite_scroll(self, page: Page, config: Dict):
        """Handle infinite scroll to load more content."""
        max_scrolls = config.get('max_scrolls', 3)
        scroll_pause = config.get('scroll_pause', 2000)
        
        for i in range(max_scrolls):
            # Scroll to bottom
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            
            # Wait for new content to load
            await page.wait_for_timeout(scroll_pause)
            
            # Check if more content was loaded
            new_height = await page.evaluate("document.body.scrollHeight")
            if i > 0 and new_height == getattr(self, '_last_height', 0):
                break  # No new content loaded
            
            self._last_height = new_height
    
    async def _trigger_lazy_loading(self, page: Page):
        """Trigger lazy loading by scrolling and hovering."""
        # Get page dimensions
        viewport = await page.evaluate("""() => {
            return {
                width: window.innerWidth,
                height: window.innerHeight
            }
        }""")
        
        # Scroll through the page to trigger lazy loading
        scroll_steps = 5
        for i in range(scroll_steps):
            scroll_y = (i + 1) * (viewport['height'] / scroll_steps)
            await page.evaluate(f"window.scrollTo(0, {scroll_y})")
            await page.wait_for_timeout(1000)
    
    async def _extract_content(self, page: Page, url: str, response) -> ScrapingResult:
        """Extract content from the page."""
        try:
            # Get basic page information
            title = await page.title()
            html_content = await page.content()
            
            # Extract text content
            text_content = await page.evaluate("""() => {
                // Remove script and style elements
                const scripts = document.querySelectorAll('script, style, noscript');
                scripts.forEach(el => el.remove());
                return document.body.innerText || document.body.textContent || '';
            }""")
            
            # Extract links
            links = await page.evaluate("""() => {
                const links = Array.from(document.querySelectorAll('a[href]'));
                return links.map(link => {
                    const href = link.getAttribute('href');
                    if (href.startsWith('http')) return href;
                    if (href.startsWith('/')) return window.location.origin + href;
                    return new URL(href, window.location.href).href;
                });
            }""")
            
            # Extract images
            images = await page.evaluate("""() => {
                const images = Array.from(document.querySelectorAll('img[src]'));
                return images.map(img => {
                    const src = img.getAttribute('src');
                    if (src.startsWith('http')) return src;
                    if (src.startsWith('/')) return window.location.origin + src;
                    return new URL(src, window.location.href).href;
                });
            }""")
            
            # Extract meta information
            meta_info = await page.evaluate("""() => {
                const metas = {};
                const metaTags = document.querySelectorAll('meta');
                metaTags.forEach(meta => {
                    const name = meta.getAttribute('name') || meta.getAttribute('property');
                    const content = meta.getAttribute('content');
                    if (name && content) {
                        metas[name] = content;
                    }
                });
                return metas;
            }""")
            
            # Get performance metrics
            performance_metrics = await page.evaluate("""() => {
                const perf = performance.getEntriesByType('navigation')[0];
                return {
                    domContentLoaded: perf.domContentLoadedEventEnd - perf.domContentLoadedEventStart,
                    loadComplete: perf.loadEventEnd - perf.loadEventStart,
                    transferSize: perf.transferSize || 0
                };
            }""")
            
            # Create result
            result = ScrapingResult(
                url=url,
                success=True,
                status_code=response.status if response else 200,
                title=title,
                content=text_content,
                html=html_content,
                links=list(set(links)),  # Remove duplicates
                images=list(set(images)),  # Remove duplicates
                metadata={
                    'meta_tags': meta_info,
                    'performance': performance_metrics,
                    'page_stats': {
                        'title_length': len(title),
                        'content_length': len(text_content),
                        'html_length': len(html_content),
                        'links_count': len(links),
                        'images_count': len(images)
                    }
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Content extraction failed: {e}")
            raise
    
    def _calculate_quality_score(self, result: ScrapingResult) -> float:
        """Calculate content quality score based on various factors."""
        if not result.success:
            return 0.0
        
        score = 0.0
        
        # Title quality (0-20 points)
        if result.title:
            title_len = len(result.title)
            if 10 <= title_len <= 100:
                score += 20
            elif title_len > 100:
                score += 15
            else:
                score += 10
        
        # Content quality (0-40 points)
        if result.content:
            content_len = len(result.content)
            if content_len > 1000:
                score += 40
            elif content_len > 500:
                score += 30
            elif content_len > 100:
                score += 20
            else:
                score += 10
        
        # Links quality (0-20 points)
        links_count = len(result.links) if result.links else 0
        if links_count > 10:
            score += 20
        elif links_count > 5:
            score += 15
        elif links_count > 0:
            score += 10
        
        # HTTP status (0-20 points)
        if result.status_code == 200:
            score += 20
        elif 200 <= result.status_code < 300:
            score += 15
        elif 300 <= result.status_code < 400:
            score += 10
        
        return min(score / 100.0, 1.0)  # Normalize to 0-1 range
    
    async def process_task(self, task_data: Dict[str, Any]):
        """
        Process a single scraping task.
        
        Args:
            task_data: Dictionary containing task information
        """
        task_id = task_data.get('task_id')
        url = task_data.get('url')
        job_id = task_data.get('job_id')
        config = task_data.get('config', {})
        
        logger.info(f"Processing headless scraping task {task_id} for URL: {url}")
        
        try:
            # Update task status to in progress
            await self._update_task_status(task_id, TaskStatus.IN_PROGRESS)
            
            # Scrape the URL
            result = await self.scrape_url(url, config)
            
            # Store result if successful
            if result.success:
                result_location = await self._store_result(result)
                await self._update_task_status(
                    task_id, 
                    TaskStatus.COMPLETED, 
                    result_location=result_location
                )
                
                # Enqueue for data processing
                await self._enqueue_for_processing(task_id, job_id, result_location)
                
            else:
                await self._update_task_status(
                    task_id, 
                    TaskStatus.FAILED, 
                    error_message=result.error_message
                )
            
            logger.info(f"Completed headless scraping task {task_id}")
            
        except Exception as e:
            error_message = f"Task processing failed: {str(e)}"
            logger.error(error_message)
            
            await self._update_task_status(
                task_id, 
                TaskStatus.FAILED, 
                error_message=error_message
            )
            
            # Record error metrics
            self.metrics.task_errors_total.labels(
                worker_type="headless",
                error_type=type(e).__name__
            ).inc()
    
    async def _update_task_status(self, task_id: str, status: TaskStatus, 
                                result_location: Optional[str] = None,
                                error_message: Optional[str] = None):
        """Update task status in database."""
        async with get_database_session() as session:
            # Implementation would update the task in database
            # This is a placeholder for the actual implementation
            logger.info(f"Updated task {task_id} status to {status}")
    
    async def _store_result(self, result: ScrapingResult) -> str:
        """Store scraping result to S3."""
        # Implementation would store result to S3
        # Return the S3 location
        return f"s3://bucket/headless-results/{int(time.time())}.json"
    
    async def _enqueue_for_processing(self, task_id: str, job_id: str, result_location: str):
        """Enqueue task for data processing."""
        redis_client = get_redis_client()
        
        message = {
            "task_id": task_id,
            "job_id": job_id,
            "result_location": result_location,
            "worker_type": "headless",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        await redis_client.xadd("needs_cleaning", message)
        logger.info(f"Enqueued task {task_id} for data processing")


class HeadlessScraperWorker:
    """Main worker class for headless scraping."""
    
    def __init__(self):
        self.settings = get_settings()
        self.scraper = HeadlessScraper()
        self.redis_client = get_redis_client()
        self.running = False
        
    async def start(self):
        """Start the headless scraper worker."""
        try:
            await self.scraper.initialize()
            self.running = True
            
            logger.info("Headless Scraper Worker started")
            
            # Start consuming tasks
            await self._consume_tasks()
            
        except Exception as e:
            logger.error(f"Failed to start worker: {e}")
            await self.stop()
            raise
    
    async def stop(self):
        """Stop the worker and cleanup resources."""
        self.running = False
        await self.scraper.cleanup()
        logger.info("Headless Scraper Worker stopped")
    
    async def _consume_tasks(self):
        """Consume tasks from Redis queue."""
        queue_name = "scrape_headless"
        consumer_group = "headless_workers"
        consumer_name = f"worker_{os.getpid()}"
        
        # Create consumer group if it doesn't exist
        try:
            await self.redis_client.xgroup_create(queue_name, consumer_group, id="0", mkstream=True)
        except:
            pass  # Group already exists
        
        while self.running:
            try:
                # Read from queue
                messages = await self.redis_client.xreadgroup(
                    consumer_group,
                    consumer_name,
                    {queue_name: ">"},
                    count=1,
                    block=1000
                )
                
                for stream, msgs in messages:
                    for msg_id, fields in msgs:
                        try:
                            # Process the task
                            await self.scraper.process_task(fields)
                            
                            # Acknowledge the message
                            await self.redis_client.xack(queue_name, consumer_group, msg_id)
                            
                        except Exception as e:
                            logger.error(f"Error processing message {msg_id}: {e}")
                            # Message will be retried later
                            
            except Exception as e:
                logger.error(f"Error consuming tasks: {e}")
                await asyncio.sleep(5)  # Wait before retrying


# Main execution
async def main():
    """Main entry point for the headless scraper worker."""
    worker = HeadlessScraperWorker()
    
    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())