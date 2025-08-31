"""
Main application entry point for Data Processing Worker.
"""

import asyncio
import signal
import structlog
from contextlib import asynccontextmanager

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'worker-shared'))

from scraper_lib import get_settings
from scraper_lib.observability import setup_logging, setup_metrics, setup_tracing
from services.data_processor import DataProcessingWorker

# Configure logging
logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan():
    """Application lifespan context manager."""
    settings = get_settings()
    
    # Setup observability
    setup_logging(settings.log_level)
    
    if settings.metrics_enabled:
        setup_metrics("data-processing-worker", settings.metrics_port + 3)
    
    if settings.tracing_enabled:
        setup_tracing("data-processing-worker", settings.tracing_endpoint)
    
    logger.info("Data Processing Worker application starting...")
    
    # Initialize worker
    worker = DataProcessingWorker()
    
    # Setup signal handlers
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(worker.stop())
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        yield worker
    finally:
        logger.info("Data Processing Worker application shutting down...")
        await worker.stop()


async def main():
    """Main application entry point."""
    async with lifespan() as worker:
        await worker.start()


if __name__ == "__main__":
    asyncio.run(main())