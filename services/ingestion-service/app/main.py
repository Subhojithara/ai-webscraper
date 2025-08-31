"""
K-Scrape Nexus Ingestion Service

Service responsible for processing uploaded files, extracting URLs, 
and creating individual scraping tasks.
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
from scraper_lib.observability import setup_logging, setup_metrics, setup_tracing, monitor_function
from scraper_lib.database import get_database_manager

from .services.ingestion_service import IngestionService
from .services.file_processor import FileProcessor


@asynccontextmanager
async def lifespan(app):
    """Application lifespan manager."""
    settings = get_settings()
    logger = structlog.get_logger("ingestion-service")
    
    # Startup
    logger.info("Starting K-Scrape Nexus Ingestion Service", version="1.0.0")
    
    try:
        # Initialize database
        db_manager = get_database_manager()
        logger.info("Database connection established")
        
        # Initialize Redis
        redis_client = get_redis_client()
        await redis_client.connect()
        logger.info("Redis connection established")
        
        # Start queue consumer
        ingestion_service = IngestionService()
        consumer_task = asyncio.create_task(ingestion_service.start_consumer())
        
        logger.info("Ingestion Service startup completed")
        yield
        
    except Exception as e:
        logger.error("Failed to start Ingestion Service", error=str(e))
        raise
    
    # Shutdown
    logger.info("Shutting down Ingestion Service")
    try:
        consumer_task.cancel()
        await redis_client.disconnect()
        await db_manager.close()
        logger.info("Ingestion Service shutdown completed")
    except Exception as e:
        logger.error("Error during shutdown", error=str(e))


async def main():
    """Main entry point for the ingestion service."""
    settings = get_settings()
    
    # Setup observability
    logger = setup_logging("ingestion-service", settings.log_level)
    setup_metrics("ingestion-service", settings.metrics_port)
    
    if settings.tracing_enabled:
        setup_tracing("ingestion-service", settings.tracing_endpoint)
    
    logger.info("Starting Ingestion Service")
    
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