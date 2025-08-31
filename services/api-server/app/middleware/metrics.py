"""
Metrics middleware for Prometheus metrics collection.
"""

import time
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from scraper_lib.observability import get_metrics_collector


class MetricsMiddleware(BaseHTTPMiddleware):
    """Middleware for collecting HTTP metrics."""
    
    def __init__(self, app):
        super().__init__(app)
        self.metrics = get_metrics_collector("api-server")
    
    async def dispatch(self, request: Request, call_next):
        """Collect metrics for HTTP requests."""
        start_time = time.time()
        
        # Extract path pattern (remove IDs for grouping)
        path_pattern = self._get_path_pattern(request.url.path)
        
        try:
            response = await call_next(request)
            
            # Record success metrics
            duration = time.time() - start_time
            
            self.metrics.http_requests_total.labels(
                method=request.method,
                endpoint=path_pattern,
                status=str(response.status_code)
            ).inc()
            
            self.metrics.http_request_duration.labels(
                method=request.method,
                endpoint=path_pattern
            ).observe(duration)
            
            return response
            
        except Exception as e:
            # Record error metrics
            duration = time.time() - start_time
            
            self.metrics.http_requests_total.labels(
                method=request.method,
                endpoint=path_pattern,
                status="500"
            ).inc()
            
            self.metrics.http_request_duration.labels(
                method=request.method,
                endpoint=path_pattern
            ).observe(duration)
            
            self.metrics.errors_total.labels(
                service="api-server",
                error_type=type(e).__name__
            ).inc()
            
            raise
    
    def _get_path_pattern(self, path: str) -> str:
        """Convert path with IDs to pattern for grouping."""
        # Simple pattern matching - replace UUIDs with placeholder
        import re
        
        # Replace UUID patterns
        uuid_pattern = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
        path_pattern = re.sub(uuid_pattern, '{id}', path, flags=re.IGNORECASE)
        
        # Replace other numeric IDs
        path_pattern = re.sub(r'/\d+', '/{id}', path_pattern)
        
        return path_pattern