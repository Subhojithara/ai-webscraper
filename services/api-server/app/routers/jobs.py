"""
Jobs router for managing scraping jobs.
"""

from typing import List, Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from scraper_lib import get_database_session, get_settings
from scraper_lib.observability import monitor_function
from ..models import (
    JobCreateRequest, JobResponse, JobListResponse, JobStatsResponse,
    TaskListResponse, PaginationParams, FilterParams, JobUpdateRequest,
    TaskRetryRequest, BulkActionResponse, ErrorResponse
)
from ..services.job_service import JobService
from ..services.task_service import TaskService

router = APIRouter()
logger = structlog.get_logger("api-server.jobs")


@router.post("/", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
@monitor_function("create_job")
async def create_job(
    job_request: JobCreateRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_database_session)
):
    """
    Create a new scraping job.
    
    This endpoint creates a new job and triggers the ingestion process
    to extract URLs from the uploaded file.
    """
    logger.info("Creating new job", name=job_request.name, file_path=job_request.file_path)
    
    try:
        job_service = JobService(db)
        job = await job_service.create_job(job_request)
        
        # Trigger ingestion in background
        background_tasks.add_task(job_service.trigger_ingestion, job.id)
        
        logger.info("Job created successfully", job_id=str(job.id), name=job.name)
        return job
        
    except ValueError as e:
        logger.error("Invalid job request", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to create job", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to create job")


@router.get("/", response_model=JobListResponse)
@monitor_function("list_jobs")
async def list_jobs(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    status_filter: Optional[str] = Query(None, description="Filter by job status"),
    search: Optional[str] = Query(None, description="Search in job names"),
    db: AsyncSession = Depends(get_database_session)
):
    """
    List all scraping jobs with pagination and filtering.
    """
    logger.debug("Listing jobs", page=page, page_size=page_size, status_filter=status_filter)
    
    try:
        job_service = JobService(db)
        
        # Build filter params
        filter_params = FilterParams(
            status=status_filter,
            search=search
        )
        
        pagination_params = PaginationParams(page=page, page_size=page_size)
        
        jobs_data = await job_service.list_jobs(pagination_params, filter_params)
        
        return jobs_data
        
    except Exception as e:
        logger.error("Failed to list jobs", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve jobs")


@router.get("/{job_id}", response_model=JobResponse)
@monitor_function("get_job")
async def get_job(
    job_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """
    Get details of a specific job.
    """
    logger.debug("Getting job details", job_id=str(job_id))
    
    try:
        job_service = JobService(db)
        job = await job_service.get_job(job_id)
        
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return job
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get job", job_id=str(job_id), error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve job")


@router.put("/{job_id}", response_model=JobResponse)
@monitor_function("update_job")
async def update_job(
    job_id: UUID,
    job_update: JobUpdateRequest,
    db: AsyncSession = Depends(get_database_session)
):
    """
    Update job details (name, description).
    """
    logger.info("Updating job", job_id=str(job_id))
    
    try:
        job_service = JobService(db)
        job = await job_service.update_job(job_id, job_update)
        
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        logger.info("Job updated successfully", job_id=str(job_id))
        return job
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error("Invalid update request", job_id=str(job_id), error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to update job", job_id=str(job_id), error=str(e))
        raise HTTPException(status_code=500, detail="Failed to update job")


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
@monitor_function("delete_job")
async def delete_job(
    job_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """
    Delete a job and all its tasks.
    
    This will stop any running tasks and clean up associated data.
    """
    logger.info("Deleting job", job_id=str(job_id))
    
    try:
        job_service = JobService(db)
        success = await job_service.delete_job(job_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Job not found")
        
        logger.info("Job deleted successfully", job_id=str(job_id))
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete job", job_id=str(job_id), error=str(e))
        raise HTTPException(status_code=500, detail="Failed to delete job")


@router.post("/{job_id}/cancel", response_model=JobResponse)
@monitor_function("cancel_job")
async def cancel_job(
    job_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """
    Cancel a running job.
    
    This will stop processing new tasks and mark the job as cancelled.
    """
    logger.info("Cancelling job", job_id=str(job_id))
    
    try:
        job_service = JobService(db)
        job = await job_service.cancel_job(job_id)
        
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        logger.info("Job cancelled successfully", job_id=str(job_id))
        return job
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to cancel job", job_id=str(job_id), error=str(e))
        raise HTTPException(status_code=500, detail="Failed to cancel job")


@router.get("/{job_id}/stats", response_model=JobStatsResponse)
@monitor_function("get_job_stats")
async def get_job_stats(
    job_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """
    Get detailed statistics for a job.
    """
    logger.debug("Getting job statistics", job_id=str(job_id))
    
    try:
        job_service = JobService(db)
        stats = await job_service.get_job_stats(job_id)
        
        if not stats:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return stats
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get job stats", job_id=str(job_id), error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve job statistics")


@router.get("/{job_id}/tasks", response_model=TaskListResponse)
@monitor_function("list_job_tasks")
async def list_job_tasks(
    job_id: UUID,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    status_filter: Optional[str] = Query(None, description="Filter by task status"),
    db: AsyncSession = Depends(get_database_session)
):
    """
    List tasks for a specific job.
    """
    logger.debug("Listing job tasks", job_id=str(job_id), page=page, page_size=page_size)
    
    try:
        task_service = TaskService(db)
        
        pagination_params = PaginationParams(page=page, page_size=page_size)
        
        tasks_data = await task_service.list_job_tasks(
            job_id, pagination_params, status_filter
        )
        
        return tasks_data
        
    except Exception as e:
        logger.error("Failed to list job tasks", job_id=str(job_id), error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve job tasks")


@router.post("/{job_id}/tasks/retry", response_model=BulkActionResponse)
@monitor_function("retry_failed_tasks")
async def retry_failed_tasks(
    job_id: UUID,
    retry_request: TaskRetryRequest,
    db: AsyncSession = Depends(get_database_session)
):
    """
    Retry failed tasks for a job.
    """
    logger.info("Retrying failed tasks", job_id=str(job_id), task_count=len(retry_request.task_ids))
    
    try:
        task_service = TaskService(db)
        result = await task_service.retry_tasks(retry_request.task_ids, retry_request.reset_retry_count)
        
        logger.info(
            "Task retry completed", 
            job_id=str(job_id),
            success_count=result.success_count,
            failure_count=result.failure_count
        )
        
        return result
        
    except Exception as e:
        logger.error("Failed to retry tasks", job_id=str(job_id), error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retry tasks")


@router.get("/{job_id}/export")
@monitor_function("export_job_results")
async def export_job_results(
    job_id: UUID,
    format: str = Query("json", regex="^(json|csv|xlsx)$", description="Export format"),
    include_failed: bool = Query(False, description="Include failed URLs"),
    db: AsyncSession = Depends(get_database_session)
):
    """
    Export job results in the specified format.
    
    Returns a pre-signed URL to download the exported file.
    """
    logger.info("Exporting job results", job_id=str(job_id), format=format)
    
    try:
        job_service = JobService(db)
        export_data = await job_service.export_results(job_id, format, include_failed)
        
        if not export_data:
            raise HTTPException(status_code=404, detail="Job not found")
        
        logger.info("Job results exported", job_id=str(job_id), format=format)
        return export_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to export job results", job_id=str(job_id), error=str(e))
        raise HTTPException(status_code=500, detail="Failed to export results")