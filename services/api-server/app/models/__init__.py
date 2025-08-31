"""
Pydantic models for API requests and responses.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID
from enum import Enum

from pydantic import BaseModel, Field, validator, HttpUrl
from scraper_lib.database import JobStatus, TaskStatus, WorkerType


class JobCreateRequest(BaseModel):
    """Request model for creating a new scraping job."""
    name: str = Field(..., min_length=1, max_length=255, description="Job name")
    description: Optional[str] = Field(None, max_length=1000, description="Job description")
    file_path: str = Field(..., description="S3 path to the uploaded file containing URLs")
    config: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Job configuration")
    
    @validator('name')
    def validate_name(cls, v):
        if not v.strip():
            raise ValueError('Job name cannot be empty')
        return v.strip()


class JobResponse(BaseModel):
    """Response model for job information."""
    id: UUID
    name: str
    description: Optional[str]
    status: JobStatus
    file_path: str
    total_urls: int
    completed_urls: int
    failed_urls: int
    progress_percentage: float
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    config: Optional[Dict[str, Any]]
    metadata: Optional[Dict[str, Any]]
    
    class Config:
        from_attributes = True


class JobListResponse(BaseModel):
    """Response model for job list."""
    jobs: List[JobResponse]
    total: int
    page: int
    page_size: int
    has_next: bool
    has_previous: bool


class TaskResponse(BaseModel):
    """Response model for task information."""
    id: UUID
    job_id: UUID
    url: str
    status: TaskStatus
    worker_type: WorkerType
    retry_count: int
    result_path: Optional[str]
    status_code: Optional[int]
    error_message: Optional[str]
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    duration_seconds: Optional[float]
    
    class Config:
        from_attributes = True


class TaskListResponse(BaseModel):
    """Response model for task list."""
    tasks: List[TaskResponse]
    total: int
    page: int
    page_size: int
    has_next: bool
    has_previous: bool


class JobStatsResponse(BaseModel):
    """Response model for job statistics."""
    job_id: UUID
    total_urls: int
    completed_urls: int
    failed_urls: int
    pending_urls: int
    processing_urls: int
    progress_percentage: float
    estimated_completion: Optional[datetime]
    average_processing_time: Optional[float]


class UploadUrlRequest(BaseModel):
    """Request model for getting upload URL."""
    filename: str = Field(..., description="Name of the file to upload")
    content_type: Optional[str] = Field(default="text/plain", description="MIME type of the file")
    
    @validator('filename')
    def validate_filename(cls, v):
        if not v.strip():
            raise ValueError('Filename cannot be empty')
        # Check for valid file extensions
        valid_extensions = ['.txt', '.csv', '.json', '.xlsx', '.xls']
        if not any(v.lower().endswith(ext) for ext in valid_extensions):
            raise ValueError(f'File must have one of these extensions: {", ".join(valid_extensions)}')
        return v.strip()


class UploadUrlResponse(BaseModel):
    """Response model for upload URL."""
    upload_url: str = Field(..., description="Pre-signed URL for file upload")
    file_path: str = Field(..., description="S3 path where the file will be stored")
    expires_in: int = Field(..., description="URL expiration time in seconds")


class ResultExportRequest(BaseModel):
    """Request model for exporting job results."""
    job_id: UUID
    format: str = Field(default="json", regex="^(json|csv|xlsx)$", description="Export format")
    include_failed: bool = Field(default=False, description="Include failed URLs in export")


class ResultExportResponse(BaseModel):
    """Response model for result export."""
    export_url: str = Field(..., description="URL to download the exported results")
    expires_in: int = Field(..., description="URL expiration time in seconds")
    format: str = Field(..., description="Export format")
    file_size: Optional[int] = Field(None, description="File size in bytes")


class ErrorResponse(BaseModel):
    """Response model for errors."""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    error_code: Optional[str] = Field(None, description="Error code for programmatic handling")


class HealthResponse(BaseModel):
    """Response model for health check."""
    status: str = Field(..., description="Overall health status")
    timestamp: datetime = Field(..., description="Health check timestamp")
    checks: Dict[str, Any] = Field(..., description="Individual component health checks")
    version: str = Field(..., description="Application version")


class MetricsResponse(BaseModel):
    """Response model for metrics endpoint."""
    active_jobs: int
    total_jobs: int
    queue_lengths: Dict[str, int]
    processing_rates: Dict[str, float]
    error_rates: Dict[str, float]
    system_resources: Dict[str, Any]


# Request/Response models for pagination
class PaginationParams(BaseModel):
    """Parameters for pagination."""
    page: int = Field(default=1, ge=1, description="Page number")
    page_size: int = Field(default=20, ge=1, le=100, description="Items per page")


class FilterParams(BaseModel):
    """Parameters for filtering."""
    status: Optional[JobStatus] = Field(None, description="Filter by job status")
    created_after: Optional[datetime] = Field(None, description="Filter jobs created after this date")
    created_before: Optional[datetime] = Field(None, description="Filter jobs created before this date")
    search: Optional[str] = Field(None, min_length=1, max_length=100, description="Search in job names")


class JobUpdateRequest(BaseModel):
    """Request model for updating job details."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    
    @validator('name')
    def validate_name(cls, v):
        if v is not None and not v.strip():
            raise ValueError('Job name cannot be empty')
        return v.strip() if v else v


class TaskRetryRequest(BaseModel):
    """Request model for retrying failed tasks."""
    task_ids: List[UUID] = Field(..., min_items=1, max_items=100, description="List of task IDs to retry")
    reset_retry_count: bool = Field(default=False, description="Reset retry count to 0")


class BulkActionResponse(BaseModel):
    """Response model for bulk operations."""
    success_count: int = Field(..., description="Number of successful operations")
    failure_count: int = Field(..., description="Number of failed operations")
    errors: List[str] = Field(default_factory=list, description="List of error messages")


# Webhook models
class WebhookConfig(BaseModel):
    """Webhook configuration model."""
    url: HttpUrl = Field(..., description="Webhook URL")
    events: List[str] = Field(..., description="List of events to send webhooks for")
    secret: Optional[str] = Field(None, description="Secret for webhook signature")
    
    @validator('events')
    def validate_events(cls, v):
        valid_events = ['job.created', 'job.started', 'job.completed', 'job.failed']
        for event in v:
            if event not in valid_events:
                raise ValueError(f'Invalid event: {event}. Must be one of: {", ".join(valid_events)}')
        return v


class WebhookResponse(BaseModel):
    """Webhook response model."""
    id: UUID
    url: str
    events: List[str]
    created_at: datetime
    is_active: bool
    
    class Config:
        from_attributes = True