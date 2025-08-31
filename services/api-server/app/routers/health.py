"""
Health check router for API server monitoring.
"""

from fastapi import APIRouter, Depends
from datetime import datetime
import structlog

from scraper_lib.observability import get_health_checker
from scraper_lib import get_settings
from ..models import HealthResponse

router = APIRouter()
logger = structlog.get_logger("api-server.health")


@router.get("/", response_model=HealthResponse)
async def health_check():
    """
    Get application health status.
    
    Returns overall health status and individual component checks.
    """
    health_checker = get_health_checker()
    settings = get_settings()
    
    health_data = await health_checker.run_checks()
    
    return HealthResponse(
        status=health_data["status"],
        timestamp=datetime.fromisoformat(health_data["timestamp"]),
        checks=health_data["checks"],
        version="1.0.0"
    )


@router.get("/ready")
async def readiness_check():
    """
    Kubernetes readiness probe endpoint.
    
    Returns 200 if the application is ready to serve requests.
    """
    health_checker = get_health_checker()
    health_data = await health_checker.run_checks()
    
    if health_data["status"] == "healthy":
        return {"status": "ready"}
    else:
        from fastapi import HTTPException
        raise HTTPException(status_code=503, detail="Service not ready")


@router.get("/live")
async def liveness_check():
    """
    Kubernetes liveness probe endpoint.
    
    Returns 200 if the application is alive.
    """
    return {"status": "alive", "timestamp": datetime.utcnow()}