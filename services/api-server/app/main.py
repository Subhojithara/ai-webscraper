"""
K-Scrape Nexus API Server

FastAPI-based REST API for job submission, status monitoring, and file management.
Provides endpoints for creating scraping jobs, uploading files, and retrieving results.
"""

import asyncio
import sys
import os
from contextlib import asynccontextmanager

# Add parent directories to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "worker-shared"))

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import structlog

from scraper_lib import get_settings, get_database_session, get_redis_client
from scraper_lib.observability import setup_logging, setup_metrics, setup_tracing, get_health_checker
from scraper_lib.database import get_database_manager

from .routers import jobs, upload, health
from .middleware.auth import AuthMiddleware
from .middleware.logging import LoggingMiddleware
from .middleware.metrics import MetricsMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup and shutdown tasks."""
    settings = get_settings()
    logger = structlog.get_logger("api-server")
    
    # Startup
    logger.info("Starting K-Scrape Nexus API Server", version="1.0.0")
    
    try:
        # Initialize database
        db_manager = get_database_manager()
        await db_manager.create_tables()
        logger.info("Database tables initialized")
        
        # Initialize Redis
        redis_client = get_redis_client()
        await redis_client.connect()
        logger.info("Redis connection established")
        
        # Setup health checks
        health_checker = get_health_checker()
        health_checker.add_check("database", lambda: True)  # Add actual DB health check
        health_checker.add_check("redis", redis_client.ping)
        
        logger.info("API Server startup completed")
        yield
        
    except Exception as e:
        logger.error("Failed to start API Server", error=str(e))
        raise
    
    # Shutdown
    logger.info("Shutting down API Server")
    try:
        await redis_client.disconnect()
        await db_manager.close()
        logger.info("API Server shutdown completed")
    except Exception as e:
        logger.error("Error during shutdown", error=str(e))


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    settings = get_settings()
    
    # Setup observability
    logger = setup_logging("api-server", settings.log_level)
    setup_metrics("api-server", settings.metrics_port)
    
    if settings.tracing_enabled:
        setup_tracing("api-server", settings.tracing_endpoint)
    
    # Create FastAPI app
    app = FastAPI(
        title="K-Scrape Nexus API",
        description="Enterprise-grade web scraping platform API",
        version="1.0.0",
        docs_url="/docs" if settings.debug else None,
        redoc_url="/redoc" if settings.debug else None,
        lifespan=lifespan
    )
    
    # Add middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if settings.debug else ["https://app.k-scrape-nexus.com"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["*"] if settings.debug else ["api.k-scrape-nexus.com", "localhost"]
    )
    
    # Custom middleware
    app.add_middleware(LoggingMiddleware)
    app.add_middleware(MetricsMiddleware)
    app.add_middleware(AuthMiddleware)
    
    # Include routers
    app.include_router(health.router, prefix="/health", tags=["health"])
    app.include_router(jobs.router, prefix="/api/v1/jobs", tags=["jobs"])
    app.include_router(upload.router, prefix="/api/v1/upload", tags=["upload"])
    
    # Global exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger = structlog.get_logger("api-server")
        logger.error(
            "Unhandled exception",
            path=request.url.path,
            method=request.method,
            error_type=type(exc).__name__,
            error_message=str(exc)
        )
        
        if settings.debug:
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal server error",
                    "detail": str(exc),
                    "type": type(exc).__name__
                }
            )
        else:
            return JSONResponse(
                status_code=500,
                content={"error": "Internal server error"}
            )
    
    return app


# Create app instance
app = create_app()


if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("API_SERVER_PORT", 8001)),
        reload=settings.debug,
        log_level=settings.log_level.lower(),
        access_log=True
    )