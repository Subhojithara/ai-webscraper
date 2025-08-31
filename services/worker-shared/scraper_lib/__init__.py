"""
K-Scrape Nexus Shared Library

This package contains shared components used across all microservices:
- Database models and utilities
- Redis client and queue management
- Observability instrumentation
- Common configurations and utilities
"""

__version__ = "1.0.0"
__author__ = "K-Scrape Nexus Team"

from .config import Settings, get_settings
from .database import get_database_session
from .cache import get_redis_client, get_queue_manager
from .observability import setup_logging, setup_metrics, setup_tracing

__all__ = [
    "Settings",
    "get_settings", 
    "get_database_session",
    "get_redis_client",
    "get_queue_manager",
    "setup_logging",
    "setup_metrics", 
    "setup_tracing",
]