"""
Crawl4AI Scraper Worker - Main application entry point.
"""

import asyncio
import signal
import sys
import uuid
from contextlib import asynccontextmanager

import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel

import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'worker-shared'))

from scraper_lib import get_settings, setup_logging, setup_metrics
from scraper_lib.observability import setup_tracing
from .services.crawl4ai_scraper import Crawl4AIScraper

# Setup logging
setup_logging("crawl4ai-scraper-worker")
logger = structlog.get_logger("crawl4ai-worker-main")

# Global worker instance
crawl4ai_scraper = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management."""
    global crawl4ai_scraper
    
    # Startup
    logger.info("Starting Crawl4AI scraper worker")
    
    # Initialize scraper
    crawl4ai_scraper = Crawl4AIScraper()
    
    # Start worker in background
    worker_id = f"crawl4ai-worker-{uuid.uuid4().hex[:8]}"
    worker_task = asyncio.create_task(
        crawl4ai_scraper.start_worker(worker_id)
    )
    
    yield
    
    # Shutdown
    logger.info("Shutting down Crawl4AI scraper worker")
    if crawl4ai_scraper:
        crawl4ai_scraper.stop_worker()
    
    # Cancel worker task
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


# FastAPI app
app = FastAPI(
    title="Crawl4AI Scraper Worker",
    description="AI-powered web scraping worker using Crawl4AI",
    version="1.0.0",
    lifespan=lifespan
)

# Setup observability
settings = get_settings()
# Skip metrics setup for now
# setup_metrics(app)


# Pydantic models
class ScrapeRequest(BaseModel):
    url: str
    config: dict = {}


class ScrapeResponse(BaseModel):
    success: bool
    url: str
    task_id: str
    result: dict = None
    error: str = None


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "crawl4ai-scraper-worker",
        "timestamp": structlog.get_logger().info("Health check")
    }


@app.get("/metrics")
async def get_metrics():
    """Metrics endpoint for Prometheus."""
    from fastapi import Response
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape_url(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Scrape a URL using Crawl4AI (for testing purposes).
    In production, tasks are processed via queue.
    """
    if not crawl4ai_scraper:
        raise HTTPException(status_code=503, detail="Scraper not initialized")
    
    try:
        task_id = str(uuid.uuid4())
        logger.info(f"Direct scrape request for {request.url}", task_id=task_id)
        
        # Perform scraping
        result = await crawl4ai_scraper.scrape_url_with_crawl4ai(
            request.url,
            request.config,
            worker_id="direct-api"
        )
        
        return ScrapeResponse(
            success=True,
            url=request.url,
            task_id=task_id,
            result=result
        )
        
    except Exception as e:
        logger.error(f"Scraping failed for {request.url}: {str(e)}")
        return ScrapeResponse(
            success=False,
            url=request.url,
            task_id=task_id,
            error=str(e)
        )


@app.get("/status")
async def get_worker_status():
    """Get worker status information."""
    if not crawl4ai_scraper:
        return {"status": "not_initialized"}
    
    return {
        "status": "running" if crawl4ai_scraper.running else "stopped",
        "worker_type": "crawl4ai-scraper",
        "queue": settings.queue_scrape_crawl4ai
    }


def handle_shutdown(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, shutting down...")
    if crawl4ai_scraper:
        crawl4ai_scraper.stop_worker()
    sys.exit(0)


if __name__ == "__main__":
    # Setup signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    # Run the application
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        workers=1,
        log_config=None  # Use structlog instead
    )