"""
K-Scrape Nexus HTTP Scraper Worker

High-performance HTTP scraping worker with connection pooling,
proxy rotation, and intelligent retry logic.
"""

import asyncio
import sys
import os
from contextlib import asynccontextmanager

# Add parent directories to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "worker-shared"))

import structlog
from scraper_lib import get_settings, get_database_session, get_redis_client, get_queue_manager
from scraper_lib.observability import setup_logging, setup_metrics, setup_tracing
from scraper_lib.database import get_database_manager

from .services.http_scraper import HTTPScraper
from .services.proxy_manager import ProxyManager


@asynccontextmanager
async def lifespan(app):
    """Application lifespan manager."""
    settings = get_settings()
    logger = structlog.get_logger("http-scraper-worker")
    
    # Startup
    logger.info("Starting K-Scrape Nexus HTTP Scraper Worker", version="1.0.0")
    
    try:
        # Initialize database
        db_manager = get_database_manager()
        logger.info("Database connection established")
        
        # Initialize Redis
        redis_client = get_redis_client()
        await redis_client.connect()
        logger.info("Redis connection established")
        
        # Initialize proxy manager
        proxy_manager = ProxyManager()
        await proxy_manager.initialize()
        
        # Start HTTP scraper workers
        scraper = HTTPScraper(proxy_manager)
        worker_tasks = []
        
        # Start multiple worker instances for concurrency
        for i in range(settings.worker_concurrency):
            task = asyncio.create_task(
                scraper.start_worker(worker_id=f"http-worker-{i}")
            )
            worker_tasks.append(task)
        
        logger.info(
            "HTTP Scraper Worker startup completed",
            worker_count=settings.worker_concurrency
        )
        yield
        
    except Exception as e:
        logger.error("Failed to start HTTP Scraper Worker", error=str(e))
        raise
    
    # Shutdown
    logger.info("Shutting down HTTP Scraper Worker")
    try:
        # Cancel worker tasks
        for task in worker_tasks:
            task.cancel()
        
        # Wait for graceful shutdown
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        
        # Close connections
        await scraper.close()
        await redis_client.disconnect()
        await db_manager.close()
        
        logger.info("HTTP Scraper Worker shutdown completed")
    except Exception as e:
        logger.error("Error during shutdown", error=str(e))


async def main():
    """Main entry point for the HTTP scraper worker."""
    settings = get_settings()
    
    # Setup observability
    logger = setup_logging("http-scraper-worker", settings.log_level)
    setup_metrics("http-scraper-worker", settings.metrics_port)
    
    if settings.tracing_enabled:
        setup_tracing("http-scraper-worker", settings.tracing_endpoint)
    
    logger.info("Starting HTTP Scraper Worker")
    
    # Create and start the service
    async with lifespan(None):
        # Keep the service running
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")


if __name__ == "__main__":
    asyncio.run(main())