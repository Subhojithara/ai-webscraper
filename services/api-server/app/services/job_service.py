"""
Job service for managing scraping jobs business logic.
"""

import uuid
from typing import Optional, List
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_
from sqlalchemy.orm import selectinload
import structlog

from scraper_lib.database import Job, Task, JobStatus, TaskStatus
from scraper_lib.cache import get_queue_manager
from scraper_lib import get_settings
from ..models import (
    JobCreateRequest, JobResponse, JobListResponse, JobStatsResponse,
    PaginationParams, FilterParams, JobUpdateRequest, ResultExportResponse
)

logger = structlog.get_logger("api-server.job_service")


class JobService:
    """Service for managing scraping jobs."""
    
    def __init__(self, db_session: AsyncSession):
        self.db = db_session
        self.settings = get_settings()
        self.queue_manager = get_queue_manager()
    
    async def create_job(self, job_request: JobCreateRequest) -> JobResponse:
        """Create a new scraping job."""
        logger.info("Creating job", name=job_request.name)
        
        # Create job instance
        job = Job(
            name=job_request.name,
            description=job_request.description,
            file_path=job_request.file_path,
            status=JobStatus.PENDING,
            config=job_request.config or {},
            metadata={"created_via": "api"}
        )
        
        self.db.add(job)
        await self.db.flush()
        await self.db.refresh(job)
        
        logger.info("Job created", job_id=str(job.id), name=job.name)
        return JobResponse.model_validate(job)
    
    async def get_job(self, job_id: uuid.UUID) -> Optional[JobResponse]:
        """Get job by ID."""
        stmt = select(Job).where(Job.id == job_id)
        result = await self.db.execute(stmt)
        job = result.scalar_one_or_none()
        
        return JobResponse.model_validate(job) if job else None
    
    async def list_jobs(
        self, 
        pagination: PaginationParams, 
        filters: FilterParams
    ) -> JobListResponse:
        """List jobs with pagination and filtering."""
        # Build query
        stmt = select(Job)
        
        # Apply filters
        if filters.status:
            stmt = stmt.where(Job.status == filters.status)
        
        if filters.search:
            search_pattern = f"%{filters.search}%"
            stmt = stmt.where(Job.name.ilike(search_pattern))
        
        if filters.created_after:
            stmt = stmt.where(Job.created_at >= filters.created_after)
        
        if filters.created_before:
            stmt = stmt.where(Job.created_at <= filters.created_before)
        
        # Get total count
        count_stmt = select(func.count()).select_from(stmt.subquery())
        total_result = await self.db.execute(count_stmt)
        total = total_result.scalar()
        
        # Apply pagination
        offset = (pagination.page - 1) * pagination.page_size
        stmt = stmt.offset(offset).limit(pagination.page_size)
        stmt = stmt.order_by(Job.created_at.desc())
        
        # Execute query
        result = await self.db.execute(stmt)
        jobs = result.scalars().all()
        
        # Convert to response models
        job_responses = [JobResponse.model_validate(job) for job in jobs]
        
        return JobListResponse(
            jobs=job_responses,
            total=total,
            page=pagination.page,
            page_size=pagination.page_size,
            has_next=(pagination.page * pagination.page_size) < total,
            has_previous=pagination.page > 1
        )
    
    async def update_job(
        self, 
        job_id: uuid.UUID, 
        job_update: JobUpdateRequest
    ) -> Optional[JobResponse]:
        """Update job details."""
        stmt = select(Job).where(Job.id == job_id)
        result = await self.db.execute(stmt)
        job = result.scalar_one_or_none()
        
        if not job:
            return None
        
        # Update fields
        if job_update.name is not None:
            job.name = job_update.name
        
        if job_update.description is not None:
            job.description = job_update.description
        
        job.updated_at = datetime.now(timezone.utc)
        
        await self.db.flush()
        await self.db.refresh(job)
        
        return JobResponse.model_validate(job)
    
    async def delete_job(self, job_id: uuid.UUID) -> bool:
        """Delete job and all its tasks."""
        stmt = select(Job).where(Job.id == job_id)
        result = await self.db.execute(stmt)
        job = result.scalar_one_or_none()
        
        if not job:
            return False
        
        # Cancel any running tasks first
        await self._cancel_job_tasks(job_id)
        
        # Delete job (tasks will be deleted by cascade)
        await self.db.delete(job)
        await self.db.flush()
        
        logger.info("Job deleted", job_id=str(job_id))
        return True
    
    async def cancel_job(self, job_id: uuid.UUID) -> Optional[JobResponse]:
        """Cancel a running job."""
        stmt = select(Job).where(Job.id == job_id)
        result = await self.db.execute(stmt)
        job = result.scalar_one_or_none()
        
        if not job:
            return None
        
        if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            raise ValueError("Job is already finished")
        
        # Update job status
        job.status = JobStatus.CANCELLED
        job.completed_at = datetime.now(timezone.utc)
        job.updated_at = datetime.now(timezone.utc)
        
        # Cancel running tasks
        await self._cancel_job_tasks(job_id)
        
        await self.db.flush()
        await self.db.refresh(job)
        
        logger.info("Job cancelled", job_id=str(job_id))
        return JobResponse.model_validate(job)
    
    async def get_job_stats(self, job_id: uuid.UUID) -> Optional[JobStatsResponse]:
        """Get detailed job statistics."""
        stmt = select(Job).where(Job.id == job_id)
        result = await self.db.execute(stmt)
        job = result.scalar_one_or_none()
        
        if not job:
            return None
        
        # Get task status counts
        task_counts_stmt = (
            select(Task.status, func.count(Task.id))
            .where(Task.job_id == job_id)
            .group_by(Task.status)
        )
        
        task_counts_result = await self.db.execute(task_counts_stmt)
        task_counts = dict(task_counts_result.all())
        
        pending_count = task_counts.get(TaskStatus.PENDING, 0)
        processing_count = task_counts.get(TaskStatus.PROCESSING, 0)
        
        # Calculate average processing time
        avg_time_stmt = (
            select(func.avg(Task.duration_seconds))
            .where(
                and_(
                    Task.job_id == job_id,
                    Task.duration_seconds.is_not(None),
                    Task.status == TaskStatus.COMPLETED
                )
            )
        )
        
        avg_time_result = await self.db.execute(avg_time_stmt)
        avg_processing_time = avg_time_result.scalar()
        
        # Estimate completion time
        estimated_completion = None
        if avg_processing_time and pending_count > 0:
            remaining_seconds = avg_processing_time * pending_count
            estimated_completion = datetime.now(timezone.utc).timestamp() + remaining_seconds
            estimated_completion = datetime.fromtimestamp(estimated_completion, tz=timezone.utc)
        
        return JobStatsResponse(
            job_id=job_id,
            total_urls=job.total_urls,
            completed_urls=job.completed_urls,
            failed_urls=job.failed_urls,
            pending_urls=pending_count,
            processing_urls=processing_count,
            progress_percentage=job.progress_percentage,
            estimated_completion=estimated_completion,
            average_processing_time=avg_processing_time
        )
    
    async def trigger_ingestion(self, job_id: uuid.UUID):
        """Trigger ingestion process for a job."""
        logger.info("Triggering ingestion", job_id=str(job_id))
        
        # Enqueue job for ingestion
        await self.queue_manager.publish_to_queue(
            self.settings.queue_new_files,
            {
                "job_id": str(job_id),
                "action": "ingest_file"
            }
        )
        
        logger.info("Ingestion triggered", job_id=str(job_id))
    
    async def export_results(
        self, 
        job_id: uuid.UUID, 
        format: str, 
        include_failed: bool
    ) -> Optional[ResultExportResponse]:
        """Export job results in specified format."""
        # This would implement result export logic
        # For now, return a placeholder
        return ResultExportResponse(
            export_url=f"https://example.com/exports/{job_id}.{format}",
            expires_in=3600,
            format=format,
            file_size=1024
        )
    
    async def _cancel_job_tasks(self, job_id: uuid.UUID):
        """Cancel all running tasks for a job."""
        # Update running tasks to cancelled status
        from sqlalchemy import update
        
        stmt = (
            update(Task)
            .where(
                and_(
                    Task.job_id == job_id,
                    Task.status.in_([TaskStatus.PENDING, TaskStatus.PROCESSING])
                )
            )
            .values(
                status=TaskStatus.FAILED,
                error_message="Cancelled by user",
                completed_at=datetime.now(timezone.utc)
            )
        )
        
        await self.db.execute(stmt)
        await self.db.flush()
        
        logger.info("Job tasks cancelled", job_id=str(job_id))