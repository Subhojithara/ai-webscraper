"""
Main application entry point for Job Coordinator Service.
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
from services.job_coordinator import JobCoordinatorService

# Configure logging
logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan():
    """Application lifespan context manager."""
    settings = get_settings()
    
    # Setup observability
    setup_logging(settings.log_level)
    
    if settings.metrics_enabled:
        setup_metrics("job-coordinator", settings.metrics_port + 4)
    
    if settings.tracing_enabled:
        setup_tracing("job-coordinator", settings.tracing_endpoint)
    
    logger.info("Job Coordinator Service starting...")
    
    # Initialize service
    service = JobCoordinatorService()
    
    # Setup signal handlers
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(service.stop())
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        yield service
    finally:
        logger.info("Job Coordinator Service shutting down...")
        await service.stop()


async def main():
    """Main application entry point."""
    async with lifespan() as service:
        await service.start()


if __name__ == "__main__":
    asyncio.run(main())