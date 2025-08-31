"""
Observability instrumentation for K-Scrape Nexus.
Provides structured logging, metrics, and distributed tracing.
"""

import asyncio
import time
import sys
from typing import Any, Dict, Optional, Callable
from functools import wraps
from datetime import datetime
import logging
import json

import structlog
from prometheus_client import (
    Counter, Histogram, Gauge, Info, CollectorRegistry,
    start_http_server, CONTENT_TYPE_LATEST, generate_latest
)
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

from .config import get_settings


# Structured Logging Setup
def setup_logging(service_name: str, log_level: str = "INFO") -> structlog.BoundLogger:
    """Setup structured logging with JSON format."""
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper())
    )
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    logger = structlog.get_logger(service_name)
    logger.info("Logging configured", service=service_name, level=log_level)
    
    return logger


# Prometheus Metrics
class MetricsCollector:
    """Centralized metrics collection for the application."""
    
    def __init__(self, service_name: str, registry: Optional[CollectorRegistry] = None):
        self.service_name = service_name
        self.registry = registry or CollectorRegistry()
        
        # Service info
        self.service_info = Info(
            'service_info', 
            'Service information',
            registry=self.registry
        )
        self.service_info.info({
            'name': service_name,
            'version': '1.0.0'
        })
        
        # HTTP metrics
        self.http_requests_total = Counter(
            'http_requests_total',
            'Total HTTP requests',
            ['method', 'endpoint', 'status'],
            registry=self.registry
        )
        
        self.http_request_duration = Histogram(
            'http_request_duration_seconds',
            'HTTP request duration',
            ['method', 'endpoint'],
            registry=self.registry
        )
        
        # Queue metrics
        self.queue_messages_published = Counter(
            'queue_messages_published_total',
            'Total messages published to queues',
            ['queue_name'],
            registry=self.registry
        )
        
        self.queue_messages_consumed = Counter(
            'queue_messages_consumed_total',
            'Total messages consumed from queues',
            ['queue_name', 'consumer'],
            registry=self.registry
        )
        
        self.queue_length = Gauge(
            'queue_length',
            'Current queue length',
            ['queue_name'],
            registry=self.registry
        )
        
        self.queue_processing_duration = Histogram(
            'queue_processing_duration_seconds',
            'Queue message processing duration',
            ['queue_name', 'consumer'],
            registry=self.registry
        )
        
        # Scraping metrics
        self.scraping_requests_total = Counter(
            'scraping_requests_total',
            'Total scraping requests',
            ['worker_type', 'status'],
            registry=self.registry
        )
        
        self.scraping_duration = Histogram(
            'scraping_duration_seconds',
            'Time spent scraping URLs',
            ['worker_type'],
            registry=self.registry
        )
        
        self.scraping_data_size = Histogram(
            'scraping_data_size_bytes',
            'Size of scraped data',
            ['worker_type'],
            registry=self.registry
        )
        
        # Job metrics
        self.jobs_total = Counter(
            'jobs_total',
            'Total jobs created',
            ['status'],
            registry=self.registry
        )
        
        self.job_duration = Histogram(
            'job_duration_seconds',
            'Job completion time',
            ['status'],
            registry=self.registry
        )
        
        self.active_jobs = Gauge(
            'active_jobs',
            'Currently active jobs',
            registry=self.registry
        )
        
        # Database metrics
        self.database_connections = Gauge(
            'database_connections',
            'Active database connections',
            registry=self.registry
        )
        
        self.database_queries_total = Counter(
            'database_queries_total',
            'Total database queries',
            ['operation', 'table'],
            registry=self.registry
        )
        
        self.database_query_duration = Histogram(
            'database_query_duration_seconds',
            'Database query duration',
            ['operation', 'table'],
            registry=self.registry
        )
        
        # Error metrics
        self.errors_total = Counter(
            'errors_total',
            'Total errors',
            ['service', 'error_type'],
            registry=self.registry
        )
    
    def get_metrics(self) -> str:
        """Get Prometheus metrics output."""
        return generate_latest(self.registry).decode()


# Global metrics collector
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector(service_name: str) -> MetricsCollector:
    """Get global metrics collector instance."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector(service_name)
    return _metrics_collector


def setup_metrics(service_name: str, port: int = 8080) -> MetricsCollector:
    """Setup Prometheus metrics collection and HTTP server."""
    collector = get_metrics_collector(service_name)
    
    # Start metrics HTTP server
    try:
        start_http_server(port, registry=collector.registry)
        logger = structlog.get_logger(service_name)
        logger.info("Metrics server started", port=port)
    except Exception as e:
        logger = structlog.get_logger(service_name)
        logger.error("Failed to start metrics server", error=str(e), port=port)
    
    return collector


# Distributed Tracing Setup
def setup_tracing(service_name: str, endpoint: Optional[str] = None) -> trace.Tracer:
    """Setup OpenTelemetry distributed tracing."""
    
    # Create resource
    resource = Resource(attributes={
        SERVICE_NAME: service_name
    })
    
    # Setup tracer provider
    tracer_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(tracer_provider)
    
    # Setup span processor and exporter
    if endpoint:
        from opentelemetry.exporter.jaeger.thrift import JaegerExporter
        jaeger_exporter = JaegerExporter(
            agent_host_name="localhost",
            agent_port=14268,
            collector_endpoint=endpoint,
        )
        span_processor = BatchSpanProcessor(jaeger_exporter)
        tracer_provider.add_span_processor(span_processor)
    
    # Auto-instrument common libraries
    FastAPIInstrumentor().instrument()
    SQLAlchemyInstrumentor().instrument()
    RedisInstrumentor().instrument()
    HTTPXClientInstrumentor().instrument()
    
    tracer = trace.get_tracer(service_name)
    
    logger = structlog.get_logger(service_name)
    logger.info("Tracing configured", service=service_name, endpoint=endpoint)
    
    return tracer


# Decorators for automatic instrumentation
def monitor_function(
    operation_name: Optional[str] = None,
    record_metrics: bool = True,
    record_traces: bool = True
):
    """Decorator to automatically monitor function performance."""
    
    def decorator(func: Callable) -> Callable:
        func_name = operation_name or f"{func.__module__}.{func.__name__}"
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            tracer = trace.get_tracer(__name__)
            
            # Start span if tracing enabled
            span = None
            if record_traces:
                span = tracer.start_span(func_name)
                span.set_attribute("function.name", func_name)
            
            try:
                # Execute function
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                
                # Record success metrics
                if record_metrics:
                    metrics = get_metrics_collector("default")
                    duration = time.time() - start_time
                    # You can add specific metrics here based on function type
                
                if span:
                    span.set_attribute("success", True)
                
                return result
                
            except Exception as e:
                # Record error metrics
                if record_metrics:
                    metrics = get_metrics_collector("default")
                    metrics.errors_total.labels(
                        service=func.__module__,
                        error_type=type(e).__name__
                    ).inc()
                
                if span:
                    span.set_attribute("success", False)
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                
                raise
            
            finally:
                if span:
                    duration = time.time() - start_time
                    span.set_attribute("duration_seconds", duration)
                    span.end()
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            tracer = trace.get_tracer(__name__)
            
            # Start span if tracing enabled
            span = None
            if record_traces:
                span = tracer.start_span(func_name)
                span.set_attribute("function.name", func_name)
            
            try:
                result = func(*args, **kwargs)
                
                if record_metrics:
                    metrics = get_metrics_collector("default")
                    duration = time.time() - start_time
                
                if span:
                    span.set_attribute("success", True)
                
                return result
                
            except Exception as e:
                if record_metrics:
                    metrics = get_metrics_collector("default")
                    metrics.errors_total.labels(
                        service=func.__module__,
                        error_type=type(e).__name__
                    ).inc()
                
                if span:
                    span.set_attribute("success", False)
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                
                raise
            
            finally:
                if span:
                    duration = time.time() - start_time
                    span.set_attribute("duration_seconds", duration)
                    span.end()
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    
    return decorator


def monitor_queue_operation(queue_name: str, operation: str):
    """Decorator to monitor queue operations."""
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            metrics = get_metrics_collector("default")
            
            try:
                result = await func(*args, **kwargs)
                
                # Record operation metrics
                if operation == "publish":
                    metrics.queue_messages_published.labels(queue_name=queue_name).inc()
                elif operation == "consume":
                    metrics.queue_messages_consumed.labels(
                        queue_name=queue_name,
                        consumer=kwargs.get("consumer_name", "default")
                    ).inc()
                
                duration = time.time() - start_time
                metrics.queue_processing_duration.labels(
                    queue_name=queue_name,
                    consumer=kwargs.get("consumer_name", "default")
                ).observe(duration)
                
                return result
                
            except Exception as e:
                metrics.errors_total.labels(
                    service="queue",
                    error_type=type(e).__name__
                ).inc()
                raise
        
        return wrapper
    
    return decorator


# Health Check Utilities
class HealthChecker:
    """Provides health check functionality for services."""
    
    def __init__(self):
        self.checks: Dict[str, Callable] = {}
    
    def add_check(self, name: str, check_func: Callable):
        """Add a health check function."""
        self.checks[name] = check_func
    
    async def run_checks(self) -> Dict[str, Any]:
        """Run all health checks."""
        results = {}
        overall_status = "healthy"
        
        for name, check_func in self.checks.items():
            try:
                if asyncio.iscoroutinefunction(check_func):
                    result = await check_func()
                else:
                    result = check_func()
                
                results[name] = {
                    "status": "healthy" if result else "unhealthy",
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                if not result:
                    overall_status = "unhealthy"
                    
            except Exception as e:
                results[name] = {
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
                overall_status = "unhealthy"
        
        return {
            "status": overall_status,
            "checks": results,
            "timestamp": datetime.utcnow().isoformat()
        }


# Global health checker
_health_checker: Optional[HealthChecker] = None


def get_health_checker() -> HealthChecker:
    """Get global health checker instance."""
    global _health_checker
    if _health_checker is None:
        _health_checker = HealthChecker()
    return _health_checker


# Utility functions for common observability tasks
def log_scraping_result(
    logger: structlog.BoundLogger,
    url: str,
    status: str,
    duration: float,
    size: Optional[int] = None,
    error: Optional[str] = None
):
    """Log scraping operation result."""
    log_data = {
        "operation": "scraping",
        "url": url,
        "status": status,
        "duration_seconds": duration
    }
    
    if size is not None:
        log_data["size_bytes"] = size
    
    if error:
        log_data["error"] = error
        logger.error("Scraping failed", **log_data)
    else:
        logger.info("Scraping completed", **log_data)


def log_job_progress(
    logger: structlog.BoundLogger,
    job_id: str,
    completed: int,
    total: int,
    failed: int = 0
):
    """Log job progress."""
    progress_percentage = (completed / total * 100) if total > 0 else 0
    
    logger.info(
        "Job progress update",
        job_id=job_id,
        completed_urls=completed,
        total_urls=total,
        failed_urls=failed,
        progress_percentage=round(progress_percentage, 2)
    )


def log_database_operation(
    logger: structlog.BoundLogger,
    operation: str,
    table: str,
    duration: float,
    records_affected: Optional[int] = None
):
    """Log database operation."""
    log_data = {
        "operation": "database",
        "db_operation": operation,
        "table": table,
        "duration_seconds": duration
    }
    
    if records_affected is not None:
        log_data["records_affected"] = records_affected
    
    logger.info("Database operation", **log_data)